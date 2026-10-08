//! Coordinate a snapshot through public backend contracts.
use crate::{
    api::{CopyRequest, PackRequest, StateStore, TestRequest},
    archive::{check_cancel, digest, verify},
    backends::Modules,
    domain::{Job, Manifest, Permanent, Snapshot},
};
use anyhow::{Result, ensure};
use chrono::Utc;
use std::{
    fs::{self, File},
    io::Write,
    path::Path,
    sync::atomic::AtomicBool,
};

pub fn create(
    job: &Job,
    state: &dyn StateStore,
    state_dir: &Path,
    cancel: &AtomicBool,
) -> Result<Snapshot> {
    let modules = Modules::from_choices(&job.spec.backends)?;
    create_with(job, state, state_dir, cancel, &modules)
}

pub fn create_with(
    job: &Job,
    state: &dyn StateStore,
    state_dir: &Path,
    cancel: &AtomicBool,
    modules: &Modules,
) -> Result<Snapshot> {
    ensure!(
        uuid::Uuid::parse_str(&job.id).is_ok(),
        Permanent("invalid job identifier".into())
    );
    ensure!(
        !crate::config::overlap(
            &job.spec.target.source_dir,
            &job.spec.target.destination_dir
        )? && !crate::config::overlap(&job.spec.target.source_dir, state_dir)?
            && !crate::config::overlap(&job.spec.target.destination_dir, state_dir)?,
        Permanent("source, destination, or state paths overlap at capture time".into())
    );
    let destination = modules.storage.open(&job.spec.target, true, state_dir)?;
    let _filesystem_lock = destination.serialize_writes(state_dir)?;
    check_cancel(cancel)?;
    let source = modules.source.open(&job.spec.target)?;
    let started = Utc::now();
    let filename = format!(
        "{}_{}.zip",
        started.format("%Y-%m-%dT%H-%M-%S%.9fZ"),
        job.id
    );
    let parent = destination.working_directory();
    let tree = parent.join(format!("{}.tree", job.id));
    let archive_path = parent.join(format!("{}.zip.part", job.id));
    let files_path = parent.join(format!("{}.files", job.id));
    let address_limit =
        job.spec.resources.memory_budget_bytes / job.spec.resources.max_concurrent_snapshots as u64;
    let result = (|| {
        let mut entries = source.inventory(&job.spec.target, &job.spec.resources)?;
        let source_bytes = entries
            .iter()
            .filter(|e| e.kind == "file")
            .map(|e| e.metadata.size)
            .sum::<u64>();
        destination.check_space(source_bytes.saturating_add(1024 * 1024))?;
        ensure!(
            !archive_path.exists(),
            "unfinished archive already exists; recovery is required"
        );
        fs::create_dir(&tree)?;
        fs::create_dir(tree.join("data"))?;
        let mut list = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&files_path)?;
        for entry in &entries {
            if entry.kind == "skipped_symlink" || entry.path.is_empty() {
                continue;
            }
            list.write_all(entry.path.as_bytes())?;
            list.write_all(b"\0")?;
        }
        list.sync_all()?;
        drop(list);
        let source_path = source.rooted_path();
        let monitor = || {
            source.check()?;
            destination.check_space(1024 * 1024)?;
            let mut bytes = 0u64;
            for entry in &entries {
                if entry.kind == "file"
                    && let Ok(metadata) = fs::symlink_metadata(tree.join("data").join(&entry.path))
                {
                    bytes = bytes.saturating_add(metadata.len());
                }
            }
            ensure!(
                bytes <= job.spec.target.storage.max_staging_bytes,
                "max_staging_bytes exceeded during rsync"
            );
            Ok(())
        };
        modules.synchronizer.copy(CopyRequest {
            source: &source_path,
            destination: &tree.join("data"),
            files_from: &files_path,
            memory_limit: address_limit,
            file_limit: job.spec.target.storage.max_staging_bytes,
            cancel,
            monitor: &monitor,
        })?;
        for entry in &mut entries {
            check_cancel(cancel)?;
            if entry.kind == "skipped_symlink" {
                continue;
            }
            ensure!(
                source.fingerprint(Path::new(&entry.path), entry.kind == "directory")?
                    == entry.metadata,
                "source changed during rsync: {}",
                entry.path
            );
            let staged = tree.join("data").join(&entry.path);
            let metadata = fs::symlink_metadata(&staged)?;
            if entry.kind == "directory" {
                ensure!(metadata.is_dir(), "staged directory type mismatch");
            } else {
                ensure!(
                    metadata.is_file() && metadata.len() == entry.metadata.size,
                    "staged file type/size mismatch: {}",
                    entry.path
                );
                entry.sha256 = Some(digest(
                    File::open(staged)?,
                    job.spec.resources.io_buffer_bytes,
                    cancel,
                )?);
            }
        }
        source.check()?;
        // The root is deliberately absent from rsync's file list: listing '.'
        // would implicitly copy entries excluded by the selection policy.
        if let Some(root) = entries.iter().find(|entry| entry.path.is_empty()) {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                tree.join("data"),
                fs::Permissions::from_mode(root.metadata.mode & 0o777),
            )?;
            let time = chrono::DateTime::from_timestamp(
                root.metadata.mtime_seconds,
                root.metadata.mtime_nanoseconds as u32,
            )
            .ok_or_else(|| anyhow::anyhow!("invalid source root modification time"))?;
            File::open(tree.join("data"))?
                .set_times(fs::FileTimes::new().set_modified(time.into()))?;
        }
        let manifest = Manifest {
            format_version: 1,
            job_id: job.id.clone(),
            target: job.spec.target.clone(),
            capture_start: started.to_rfc3339(),
            capture_finish: Utc::now().to_rfc3339(),
            consistency: source.consistency().into(),
            entries,
        };
        fs::create_dir(tree.join("meta"))?;
        let mut manifest_file = File::create(tree.join("meta/manifest.json"))?;
        serde_json::to_writer(&mut manifest_file, &manifest)?;
        manifest_file.sync_all()?;
        drop(manifest_file);
        modules.archiver.pack(PackRequest {
            tree: &tree,
            output: &archive_path,
            target: &job.spec.target,
            memory_limit: address_limit,
            cancel,
            monitor: &|| destination.check_space(1024 * 1024),
        })?;
        let bytes = fs::metadata(&archive_path)?.len();
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&archive_path, fs::Permissions::from_mode(0o600))?;
        }
        ensure!(
            bytes <= job.spec.target.archive.max_archive_bytes,
            "max_archive_bytes exceeded"
        );
        modules.archiver.test(TestRequest {
            archive: &archive_path,
            memory_limit: address_limit,
            file_limit: job.spec.target.archive.max_archive_bytes,
            cancel,
            monitor: &|| destination.check(),
        })?;
        verify(
            File::open(&archive_path)?,
            &job.spec.resources,
            Some(&job.id),
            Some(&job.spec.target),
            cancel,
        )?;
        let hash = digest(
            File::open(&archive_path)?,
            job.spec.resources.io_buffer_bytes,
            cancel,
        )?;
        File::open(&archive_path)?.sync_all()?;
        source.check()?;
        destination.check_space(0)?;
        check_cancel(cancel)?;
        // Staging is disposable; remove it before beginning durable publication.
        destination.remove_staging(&job.id)?;
        let snapshot = Snapshot {
            backends: job.spec.backends.clone(),
            job_id: job.id.clone(),
            target_id: job.spec.target.id.clone(),
            policy_id: job.spec.policy_id()?,
            capture_ms: started.timestamp_millis(),
            filename,
            bytes,
            sha256: hash,
            target: job.spec.target.clone(),
        };
        state.prepare(&snapshot)?;
        destination.publish(&job.id, &snapshot.filename)?;
        state.complete(&snapshot)?;
        Ok(snapshot)
    })();
    if result.is_err() && state.intention(&job.id)?.is_none() {
        let _ = destination.remove_temporary(&job.id);
        let _ = destination.remove_staging(&job.id);
    }
    result
}
