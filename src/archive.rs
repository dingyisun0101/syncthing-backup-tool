//! ZIP backend and canonical manifest verification. Copying, scheduling, and
//! publication belong to the snapshot coordinator, outside this module.
use crate::{
    api::DestinationSession,
    domain::{Manifest, Snapshot},
    resources::MemoryBudget,
};
use anyhow::{Result, ensure};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};
use zip::ZipArchive;

pub fn check_cancel(cancel: &AtomicBool) -> Result<()> {
    ensure!(!cancel.load(Ordering::Relaxed), "backup cancelled");
    Ok(())
}
pub fn digest(mut file: File, buffer_size: usize, cancel: &AtomicBool) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = vec![0; buffer_size];
    loop {
        check_cancel(cancel)?;
        let n = file.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(hex::encode(hash.finalize()))
}

pub fn verify(
    file: File,
    resources: &crate::config::Resources,
    job: Option<&str>,
    target: Option<&crate::config::Target>,
    cancel: &AtomicBool,
) -> Result<Manifest> {
    let mut zip = ZipArchive::new(file)?;
    let budget = MemoryBudget::new(resources);
    let mut bytes = Vec::new();
    zip.by_name("meta/manifest.json")?
        .take(budget.manifest_limit() + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 <= budget.manifest_limit(),
        "manifest exceeds memory allowance"
    );
    let manifest: Manifest = serde_json::from_slice(&bytes)?;
    drop(bytes);
    ensure!(
        manifest.format_version == 1
            && ["live", "application_quiesced"].contains(&manifest.consistency.as_str()),
        "unsupported manifest format"
    );
    if let Some(id) = job {
        ensure!(manifest.job_id == id, "archive job identity mismatch");
    }
    if let Some(target) = target {
        ensure!(
            serde_json::to_value(&manifest.target)? == serde_json::to_value(target)?,
            "archive target configuration mismatch"
        );
    }
    let mut allowance = MemoryBudget::new(resources);
    let mut paths = HashSet::new();
    let mut count = 1usize;
    let mut buffer = vec![0; resources.io_buffer_bytes];
    for entry in &manifest.entries {
        check_cancel(cancel)?;
        allowance.entry(&entry.path)?;
        ensure!(paths.insert(&entry.path), "duplicate manifest path");
        ensure!(
            !PathBuf::from(&entry.path).is_absolute()
                && !Path::new(&entry.path)
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir)),
            "invalid manifest path"
        );
        if entry.kind == "skipped_symlink" {
            continue;
        }
        let name = if entry.kind == "directory" {
            if entry.path.is_empty() {
                "data/".into()
            } else {
                format!("data/{}/", entry.path)
            }
        } else {
            ensure!(
                ["file", "symlink"].contains(&entry.kind.as_str()),
                "unknown manifest entry type"
            );
            format!("data/{}", entry.path)
        };
        let mut member = zip.by_name(&name)?;
        if entry.kind == "directory" {
            ensure!(
                member.is_dir() && member.size() == 0,
                "invalid archive directory"
            );
        } else {
            ensure!(
                member.size() == entry.metadata.size,
                "archive file size mismatch"
            );
            if entry.kind == "symlink" {
                ensure!(
                    member
                        .unix_mode()
                        .is_some_and(|mode| mode & 0o170000 == 0o120000),
                    "archive symlink type mismatch"
                );
                let mut target = String::new();
                member.by_ref().take(65537).read_to_string(&mut target)?;
                ensure!(
                    Some(&target) == entry.symlink_target.as_ref(),
                    "archive symlink target mismatch"
                );
                crate::telemetry::audit(
                    "archive.entry.verify",
                    "succeeded",
                    serde_json::json!({"path":entry.path,"kind":"symlink"}),
                )?;
                count += 1;
                continue;
            }
            let mut hash = Sha256::new();
            let mut size = 0;
            loop {
                check_cancel(cancel)?;
                let n = member.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                size += n as u64;
                hash.update(&buffer[..n]);
            }
            ensure!(
                size == entry.metadata.size && Some(hex::encode(hash.finalize())) == entry.sha256,
                "archive content checksum mismatch: {}",
                entry.path
            );
        }
        crate::telemetry::audit(
            "archive.entry.verify",
            "succeeded",
            serde_json::json!({"path":entry.path,"kind":entry.kind}),
        )?;
        count += 1;
    }
    if let Ok(metadata) = zip.by_name("meta/") {
        ensure!(
            metadata.is_dir() && metadata.size() == 0,
            "invalid meta directory"
        );
        count += 1;
    }
    ensure!(
        zip.len() == count,
        "archive has extra or missing entries (found {}, expected {}, first names {:?})",
        zip.len(),
        count,
        zip.file_names().take(16).collect::<Vec<_>>()
    );
    Ok(manifest)
}

pub fn verify_snapshot(
    destination: &dyn DestinationSession,
    snapshot: &Snapshot,
    resources: &crate::config::Resources,
    cancel: &AtomicBool,
) -> Result<()> {
    let file = destination.archive(&snapshot.filename)?;
    ensure!(
        file.metadata()?.len() == snapshot.bytes,
        "archive size changed"
    );
    ensure!(
        digest(file, resources.io_buffer_bytes, cancel)? == snapshot.sha256,
        "archive digest changed"
    );
    verify(
        destination.archive(&snapshot.filename)?,
        resources,
        Some(&snapshot.job_id),
        Some(&snapshot.target),
        cancel,
    )?;
    Ok(())
}

pub fn never_cancel() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

pub struct InfoZip;
impl crate::api::Archiver for InfoZip {
    fn pack(&self, request: crate::api::PackRequest<'_>) -> Result<()> {
        let mut command = crate::process::tool(
            "/usr/bin/zip",
            request.memory_limit,
            request.target.archive.max_archive_bytes,
        );
        command
            .current_dir(request.tree)
            .args(["-q", "-r", "-y"])
            // Info-ZIP uses random zi* temporary names even for a new archive.
            // Keep those inside the journaled tree so crash recovery owns them.
            .arg("-b")
            .arg(request.tree)
            .arg(if request.target.archive.compression == "store" {
                "-0".to_owned()
            } else {
                format!("-{}", request.target.archive.compression_level)
            })
            .arg(request.output)
            .args(["data", "meta"]);
        crate::process::run(&mut command, request.cancel, request.monitor)
    }
    fn test(&self, request: crate::api::TestRequest<'_>) -> Result<()> {
        let mut command =
            crate::process::tool("/usr/bin/unzip", request.memory_limit, request.file_limit);
        command.arg("-tqq").arg(request.archive);
        crate::process::run(&mut command, request.cancel, request.monitor)
    }
}
