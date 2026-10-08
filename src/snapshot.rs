//! Coordinate a snapshot through public backend contracts.
use crate::{
    api::{CopyRequest, PackRequest, StateStore, TestRequest},
    archive::{check_cancel, digest, verify},
    backends::Modules,
    domain::{Job, Manifest, Permanent, Snapshot},
    hooks, telemetry,
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
    let _context = telemetry::context(job);
    telemetry::audit(
        "backup",
        "started",
        serde_json::json!({"attempt":job.attempts}),
    )?;
    if !job.spec.target.hooks.finally.is_empty() {
        state.register_cleanup(job)?;
    }
    let mut committed: Option<Snapshot> = None;
    let mut unchanged = false;

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
    let mut result = (|| {
        destination.check_space(1024 * 1024)?;
        state.progress(&job.id, "before_backup")?;
        hooks::run_phase(
            job,
            modules,
            "before_backup",
            &job.spec.target.hooks.before_backup,
            cancel,
        )?;
        let source = telemetry::operation("source.open", serde_json::json!({}), || {
            modules.source.open(&job.spec.target)
        })?;
        state.progress(&job.id, "inventory")?;
        let mut entries = source.inventory(&job.spec.target, &job.spec.resources)?;
        if job.spec.target.skip_unchanged {
            state.progress(&job.id, "source_hash_check")?;
            if let Some(snapshot) = unchanged_capture(
                job,
                state,
                source.as_ref(),
                destination.as_ref(),
                &mut entries,
                cancel,
            )? {
                hooks::run_phase(
                    job,
                    modules,
                    "after_capture",
                    &job.spec.target.hooks.after_capture,
                    cancel,
                )?;
                state.record_source_check(&job.spec.target.id, Some(snapshot.capture_ms))?;
                unchanged = true;
                return Ok(snapshot);
            }
        }
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
        let last_space_scan = std::cell::Cell::new(std::time::Instant::now());
        let monitor = || {
            source.check()?;
            destination.check_space(1024 * 1024)?;
            if last_space_scan.get().elapsed().as_secs() < 5 {
                return Ok(());
            }
            last_space_scan.set(std::time::Instant::now());
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
        state.progress(&job.id, "copy")?;
        telemetry::operation(
            "rsync.copy",
            serde_json::json!({"entries":entries.len(),"bytes":source_bytes}),
            || {
                modules.synchronizer.copy(CopyRequest {
                    source: &source_path,
                    destination: &tree.join("data"),
                    files_from: &files_path,
                    memory_limit: address_limit,
                    file_limit: job.spec.target.storage.max_staging_bytes,
                    cancel,
                    monitor: &monitor,
                })
            },
        )?;
        state.progress(&job.id, "staging_verify")?;
        for entry in &mut entries {
            check_cancel(cancel)?;
            if entry.kind == "skipped_symlink" {
                continue;
            }
            if entry.kind == "symlink" {
                let (metadata, target) = source.symlink(Path::new(&entry.path))?;
                ensure!(
                    metadata == entry.metadata && Some(&target) == entry.symlink_target.as_ref(),
                    "source symlink changed during rsync"
                );
                let staged = fs::read_link(tree.join("data").join(&entry.path))?;
                ensure!(
                    staged.to_str() == Some(target.as_str()),
                    "staged symlink mismatch"
                );
                telemetry::audit(
                    "staging.entry",
                    "copied",
                    serde_json::json!({"path":entry.path,"kind":entry.kind}),
                )?;
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
            telemetry::audit(
                "staging.entry",
                "copied",
                serde_json::json!({"path":entry.path,"kind":entry.kind}),
            )?;
        }
        source.check()?;
        state.record_source_check(&job.spec.target.id, None)?;
        state.progress(&job.id, "after_capture")?;
        hooks::run_phase(
            job,
            modules,
            "after_capture",
            &job.spec.target.hooks.after_capture,
            cancel,
        )?;
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
            consistency: job.spec.target.consistency.clone(),
            entries,
        };
        fs::create_dir(tree.join("meta"))?;
        let mut manifest_file = File::create(tree.join("meta/manifest.json"))?;
        serde_json::to_writer(&mut manifest_file, &manifest)?;
        manifest_file.sync_all()?;
        drop(manifest_file);
        state.progress(&job.id, "archive")?;
        telemetry::operation("archive.pack", serde_json::json!({}), || {
            modules.archiver.pack(PackRequest {
                tree: &tree,
                output: &archive_path,
                target: &job.spec.target,
                memory_limit: address_limit,
                cancel,
                monitor: &|| destination.check_space(1024 * 1024),
            })
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
        state.progress(&job.id, "verify")?;
        telemetry::operation("archive.test", serde_json::json!({}), || {
            modules.archiver.test(TestRequest {
                archive: &archive_path,
                memory_limit: address_limit,
                file_limit: job.spec.target.archive.max_archive_bytes,
                cancel,
                monitor: &|| destination.check(),
            })
        })?;
        state.progress(&job.id, "content_verify")?;
        telemetry::operation("archive.verify", serde_json::json!({}), || {
            verify(
                File::open(&archive_path)?,
                &job.spec.resources,
                Some(&job.id),
                Some(&job.spec.target),
                cancel,
            )
        })?;
        state.progress(&job.id, "digest")?;
        let hash = telemetry::operation("archive.digest", serde_json::json!({}), || {
            digest(
                File::open(&archive_path)?,
                job.spec.resources.io_buffer_bytes,
                cancel,
            )
        })?;
        File::open(&archive_path)?.sync_all()?;
        if job.spec.target.archive.reopened_verification {
            state.progress(&job.id, "reopened_verify")?;
            let file = File::open(&archive_path)?;
            let before = crate::source::fingerprint(&file.metadata()?);
            let hints = crate::integrity::evict(&file);
            drop(file);
            let actual = digest(
                File::open(&archive_path)?,
                job.spec.resources.io_buffer_bytes,
                cancel,
            )?;
            if actual != hash {
                return Err(crate::archive::Mismatch {
                    phase: "reopened_archive_digest".into(),
                    path: archive_path.display().to_string(),
                    expected: hash.clone(),
                    actual,
                }
                .into());
            }
            verify(
                File::open(&archive_path)?,
                &job.spec.resources,
                Some(&job.id),
                Some(&job.spec.target),
                cancel,
            )?;
            ensure!(
                crate::source::fingerprint(&File::open(&archive_path)?.metadata()?) == before,
                "archive metadata changed during reopened verification"
            );
            telemetry::audit("archive.reopened_verify", "succeeded", hints)?;
        }
        source.check()?;
        destination.check_space(0)?;
        check_cancel(cancel)?;
        // Staging is disposable; remove it before beginning durable publication.
        telemetry::operation("staging.cleanup", serde_json::json!({}), || {
            destination.remove_staging(&job.id)
        })?;
        let snapshot = Snapshot {
            backends: job.spec.backends.clone(),
            job_id: job.id.clone(),
            target_id: job.spec.target.id.clone(),
            policy_id: job.spec.policy_id()?,
            capture_ms: started.timestamp_millis(),
            filename,
            bytes,
            selected_bytes: Some(source_bytes),
            sha256: hash,
            target: job.spec.target.clone(),
        };
        state.prepare(&snapshot)?;
        telemetry::operation(
            "archive.publish",
            serde_json::json!({"file":snapshot.filename}),
            || destination.publish(&job.id, &snapshot.filename),
        )?;
        state.complete(&snapshot)?;
        committed = Some(snapshot.clone());
        state.progress(&job.id, "after_backup")?;
        hooks::run_phase(
            job,
            modules,
            "after_backup",
            &job.spec.target.hooks.after_backup,
            cancel,
        )?;
        Ok(snapshot)
    })();

    let failed_verification = result
        .as_ref()
        .err()
        .is_some_and(|e| e.is::<crate::archive::Mismatch>())
        || (result.is_err()
            && !cancel.load(std::sync::atomic::Ordering::Relaxed)
            && state.job_status(&job.id)?.is_some_and(|s| {
                ["verify", "content_verify", "digest", "reopened_verify"]
                    .contains(&s.phase.as_str())
            }));
    if failed_verification {
        let error = result.as_ref().expect_err("verification failed");
        let report = serde_json::json!({"id":uuid::Uuid::new_v4().to_string(),"target":job.spec.target.id,"job_id":job.id,"phase":state.job_status(&job.id)?.map(|s|s.phase),"path":archive_path,"staging_path":tree,"error":format!("{error:#}"),"mismatch":error.downcast_ref::<crate::archive::Mismatch>().map(|m|serde_json::json!(m)),"evidence_preserved":true});
        if let Err(e) = state.record_inspection("integrity_incident", &report) {
            telemetry::event(
                "error",
                "integrity evidence persistence failed",
                serde_json::json!({"error":format!("{e:#}")}),
            );
        }
        result = result.map_err(|e| {
            e.context(Permanent(
                "archive verification failed; evidence preserved; automatic recapture disabled"
                    .into(),
            ))
        });
    }
    if result.is_err() && !failed_verification && state.intention(&job.id)?.is_none() {
        if let Err(error) = destination.remove_temporary(&job.id) {
            telemetry::event(
                "error",
                "temporary cleanup failed",
                serde_json::json!({"error":format!("{error:#}")}),
            );
        }
        if let Err(error) = destination.remove_staging(&job.id) {
            telemetry::event(
                "error",
                "staging cleanup failed",
                serde_json::json!({"error":format!("{error:#}")}),
            );
        }
    }
    if let Err(error) = hooks::cleanup(job, state, modules) {
        state.finish_job(
            job,
            "cleanup_failed",
            Some(&format!("{error:#}")),
            committed.as_ref(),
        )?;
        telemetry::event(
            "error",
            "mandatory cleanup pending",
            serde_json::json!({"error":format!("{error:#}")}),
        );
        return Err(error);
    }
    match &result {
        Ok(snapshot) => state.finish_job(
            job,
            if unchanged { "unchanged" } else { "succeeded" },
            None,
            Some(snapshot),
        )?,
        Err(error) if committed.is_some() => state.finish_job(
            job,
            "completed_with_hook_failure",
            Some(&format!("{error:#}")),
            committed.as_ref(),
        )?,
        Err(error) if error.is::<crate::domain::Skipped>() => {
            state.finish_job(job, "skipped", Some(&format!("{error:#}")), None)?
        }
        _ => (),
    }
    telemetry::audit(
        "backup",
        if result.is_ok() {
            "succeeded"
        } else if result
            .as_ref()
            .err()
            .is_some_and(|e| e.is::<crate::domain::Skipped>())
        {
            "skipped"
        } else {
            "failed"
        },
        serde_json::json!({"error":result.as_ref().err().map(|e|format!("{e:#}"))}),
    )?;
    result
}

fn unchanged_capture(
    job: &Job,
    state: &dyn StateStore,
    source: &dyn crate::api::SourceSession,
    destination: &dyn crate::api::DestinationSession,
    entries: &mut [crate::domain::Entry],
    cancel: &AtomicBool,
) -> Result<Option<Snapshot>> {
    let policy = job.spec.policy_id()?;
    let catalog = state.catalog()?;
    let mut snapshots = catalog
        .iter()
        .filter(|(s, h, _)| *h && s.policy_id == policy)
        .map(|(s, _, _)| s)
        .collect::<Vec<_>>();
    snapshots.sort_by_key(|s| std::cmp::Reverse(s.capture_ms));
    if snapshots.len() < job.spec.target.retention.min_snapshots {
        return Ok(None);
    }
    let latest = snapshots[0];
    let age = Utc::now()
        .timestamp_millis()
        .saturating_sub(latest.capture_ms)
        .max(0) as u64
        / 1000;
    if job
        .spec
        .target
        .max_capture_age_seconds
        .is_none_or(|limit| age >= limit)
    {
        return Ok(None);
    }
    for entry in entries.iter_mut().filter(|e| e.kind == "file") {
        let file = source.file(Path::new(&entry.path))?;
        ensure!(
            crate::source::fingerprint(&file.metadata()?) == entry.metadata,
            "source changed before unchanged check: {}",
            entry.path
        );
        entry.sha256 = Some(digest(
            file.try_clone()?,
            job.spec.resources.io_buffer_bytes,
            cancel,
        )?);
        ensure!(
            crate::source::fingerprint(&file.metadata()?) == entry.metadata,
            "source changed during unchanged check: {}",
            entry.path
        );
    }
    let second = source.inventory(&job.spec.target, &job.spec.resources)?;
    let second = second
        .iter()
        .map(|e| (&e.path, (&e.kind, &e.metadata, &e.symlink_target)))
        .collect::<std::collections::BTreeMap<_, _>>();
    ensure!(
        second.len() == entries.len()
            && entries
                .iter()
                .all(|e| second.get(&e.path) == Some(&(&e.kind, &e.metadata, &e.symlink_target))),
        "source selection changed during unchanged check"
    );
    crate::integrity::verify_checked(
        state,
        destination,
        latest,
        &job.spec.resources,
        false,
        cancel,
    )?;
    let previous = verify(
        destination.archive(&latest.filename)?,
        &job.spec.resources,
        Some(&latest.job_id),
        Some(&latest.target),
        cancel,
    )?;
    let previous = previous
        .entries
        .iter()
        .map(|e| (&e.path, e))
        .collect::<std::collections::BTreeMap<_, _>>();
    let same = previous.len() == entries.len()
        && entries.iter().all(|e| {
            previous.get(&e.path).is_some_and(|p| {
                p.kind == e.kind
                    && p.sha256 == e.sha256
                    && p.symlink_target == e.symlink_target
                    && p.metadata.mode == e.metadata.mode
                    && (e.kind != "file"
                        || (p.metadata.size == e.metadata.size
                            && p.metadata.mtime_seconds == e.metadata.mtime_seconds
                            && p.metadata.mtime_nanoseconds == e.metadata.mtime_nanoseconds))
            })
        });
    if !same {
        return Ok(None);
    }
    for (snapshot, healthy, _) in catalog.iter().filter(|(s, _, _)| {
        s.target_id == job.spec.target.id
            && s.target.source_dir == job.spec.target.source_dir
            && s.target.destination_dir == job.spec.target.destination_dir
    }) {
        ensure!(
            *healthy,
            "unchanged result blocked by unhealthy retained archive"
        );
        crate::integrity::verify_checked(
            state,
            destination,
            snapshot,
            &job.spec.resources,
            false,
            cancel,
        )?;
    }
    Ok(Some(latest.clone()))
}
