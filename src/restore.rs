//! Verified extraction into an empty, isolated destination using pinned parents.
use crate::{
    archive,
    config::{self, Config},
    domain::Manifest,
    source,
    storage::{open_beneath, proc_path},
};
use anyhow::{Context, Result, ensure};
use rustix::fs::{Mode, OFlags};
use serde_json::{Value, json};
use std::{
    fs::{self, File},
    io::{Read, Write},
    os::unix::fs::PermissionsExt,
    path::{Component, Path},
    sync::atomic::AtomicBool,
};

pub fn safe_relative(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty()
            && path
                .split('/')
                .all(|part| !part.is_empty() && part != "." && part != "..")
            && !path.contains('\\')
            && path.len() <= 4096
            && !Path::new(path).is_absolute()
            && Path::new(path)
                .components()
                .all(|c| matches!(c, Component::Normal(_))),
        "unsafe extraction path: {path:?}"
    );
    Ok(())
}
pub fn isolated(config: &Config, path: &Path) -> Result<()> {
    ensure!(
        !config::overlap(path, &config.state_dir)?,
        "restore destination overlaps state"
    );
    let mut targets = config.targets.clone();
    if let Some(state) = crate::state::State::read_only(&config.state_dir)? {
        targets.extend(state.historical_targets()?);
    }
    for target in targets {
        ensure!(
            !config::overlap(path, &target.source_dir)?
                && !config::overlap(path, &target.destination_dir)?,
            "restore destination overlaps source or archive data"
        );
    }
    Ok(())
}
fn parent(root: &File, path: &Path) -> Result<File> {
    let mut pinned = root.try_clone()?;
    for component in path.components() {
        let Component::Normal(name) = component else {
            anyhow::bail!("unsafe parent component");
        };
        match rustix::fs::mkdirat(&pinned, name, Mode::RUSR | Mode::WUSR | Mode::XUSR) {
            Ok(()) => (),
            Err(e) if e == rustix::io::Errno::EXIST => (),
            Err(e) => return Err(e.into()),
        }
        pinned = open_beneath(&pinned, Path::new(name), OFlags::RDONLY | OFlags::DIRECTORY)?;
    }
    Ok(pinned)
}
fn safe_link(path: &Path, target: &str, links: &[&crate::domain::Entry]) -> Result<()> {
    use std::collections::VecDeque;
    ensure!(
        !target.is_empty() && !Path::new(target).is_absolute() && !target.contains('\\'),
        "unsafe symlink target"
    );
    let mut stack = path
        .parent()
        .map(|p| {
            p.components()
                .filter_map(|c| {
                    if let Component::Normal(n) = c {
                        Some(n.to_owned())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let mut pending = Path::new(target).components().collect::<VecDeque<_>>();
    let mut followed = 0;
    while let Some(component) = pending.pop_front() {
        match component {
            Component::Normal(name) => {
                stack.push(name.to_owned());
                let current = stack.iter().collect::<std::path::PathBuf>();
                if let Some(link) = links.iter().find(|e| Path::new(&e.path) == current) {
                    followed += 1;
                    ensure!(followed <= 40, "symlink cycle or excessive chain");
                    stack.pop();
                    let next = link
                        .symlink_target
                        .as_deref()
                        .context("missing symlink target")?;
                    ensure!(
                        !Path::new(next).is_absolute() && !next.contains('\\'),
                        "unsafe symlink chain"
                    );
                    for part in Path::new(next).components().rev() {
                        pending.push_front(part);
                    }
                }
            }
            Component::CurDir => (),
            Component::ParentDir => {
                ensure!(stack.pop().is_some(), "symlink escapes restore destination");
            }
            _ => anyhow::bail!("unsafe symlink target"),
        }
    }
    Ok(())
}

pub fn inspect(config: &Config, path: &Path, cancel: &AtomicBool) -> Result<Manifest> {
    let file = File::open(path)?;
    let before = source::fingerprint(&file.metadata()?);
    let known = crate::planning::catalog(config)?
        .into_iter()
        .find(|(s, _, _)| {
            config::resolved(&s.target.destination_dir.join(&s.filename)).ok()
                == config::resolved(path).ok()
        });
    if let Some((snapshot, _, _)) = known {
        ensure!(
            archive::digest(file.try_clone()?, config.resources.io_buffer_bytes, cancel)?
                == snapshot.sha256,
            "catalog archive digest mismatch"
        );
        use std::io::Seek;
        let mut reset = file.try_clone()?;
        reset.rewind()?;
    }
    let manifest = archive::verify(file, &config.resources, None, None, cancel)?;
    ensure!(
        source::fingerprint(&File::open(path)?.metadata()?) == before,
        "archive changed during inspection"
    );
    Ok(manifest)
}
pub fn extract(
    config: &Config,
    path: &Path,
    destination: &Path,
    selected: &[String],
    sample: Option<usize>,
    symlinks: bool,
    cancel: &AtomicBool,
) -> Result<Value> {
    isolated(config, destination)?;
    for wanted in selected {
        safe_relative(wanted)?;
    }
    let manifest = inspect(config, path, cancel)?;
    for wanted in selected {
        ensure!(
            manifest
                .entries
                .iter()
                .any(|e| &e.path == wanted || Path::new(&e.path).starts_with(wanted)),
            "selected path not found: {wanted}"
        );
    }
    let root_meta = match fs::symlink_metadata(destination) {
        Ok(m) => {
            ensure!(
                m.is_dir() && !m.is_symlink(),
                "restore destination must be a real empty directory"
            );
            Some(m)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(e.into()),
    };
    if root_meta.is_none() {
        fs::create_dir(destination)?;
    }
    let root = File::from(rustix::fs::open(
        destination,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?);
    ensure!(
        fs::read_dir(proc_path(&root))?.next().is_none(),
        "restore destination is not empty"
    );
    let before = source::fingerprint(&File::open(path)?.metadata()?);

    let sample_paths = sample.map(|limit| {
        use sha2::{Digest, Sha256};
        let mut candidates = manifest
            .entries
            .iter()
            .filter(|e| {
                e.kind == "file"
                    && (selected.is_empty()
                        || selected.iter().any(|p| Path::new(&e.path).starts_with(p)))
            })
            .map(|e| {
                let mut hash = Sha256::new();
                hash.update(manifest.job_id.as_bytes());
                hash.update(e.path.as_bytes());
                (hash.finalize(), e.path.clone())
            })
            .collect::<Vec<_>>();
        candidates.sort();
        candidates
            .into_iter()
            .take(limit)
            .map(|(_, path)| path)
            .collect::<std::collections::HashSet<_>>()
    });
    let filesystem = crate::io_policy::filesystem_path(destination)?;
    let reserve = config
        .targets
        .iter()
        .filter(|t| crate::io_policy::filesystem(t).is_ok_and(|id| id == filesystem))
        .map(|t| t.storage.min_free_bytes)
        .max()
        .unwrap_or(0);
    let payload = manifest
        .entries
        .iter()
        .filter(|e| {
            e.kind == "file"
                && (selected.is_empty()
                    || selected.iter().any(|p| Path::new(&e.path).starts_with(p)))
                && sample_paths
                    .as_ref()
                    .is_none_or(|paths| paths.contains(&e.path))
        })
        .map(|e| e.metadata.size as u128)
        .sum::<u128>();
    ensure!(
        payload
            .saturating_add(reserve as u128)
            .saturating_add(1024 * 1024)
            <= fs2::available_space(proc_path(&root))? as u128,
        "insufficient restore space including the configured filesystem reserve"
    );
    let mut zip = zip::ZipArchive::new(File::open(path)?)?;
    let mut restored = Vec::new();
    let mut links = Vec::new();
    let mut dirs = Vec::new();
    let mut count = 0usize;
    let mut buffer = vec![0u8; config.resources.io_buffer_bytes];
    for entry in &manifest.entries {
        archive::check_cancel(cancel)?;
        if entry.path.is_empty() {
            continue;
        }
        safe_relative(&entry.path)?;
        if !selected.is_empty()
            && !selected
                .iter()
                .any(|wanted| Path::new(&entry.path).starts_with(wanted))
        {
            continue;
        }
        if entry.kind == "skipped_symlink" {
            continue;
        }
        if entry.kind == "symlink" {
            if symlinks {
                links.push(entry);
            }
            continue;
        }
        if entry.kind == "directory" {
            if sample.is_none() {
                parent(&root, Path::new(&entry.path))?;
                dirs.push(entry);
            }
            continue;
        }
        if sample_paths
            .as_ref()
            .is_some_and(|paths| !paths.contains(&entry.path))
        {
            continue;
        }
        let relative = Path::new(&entry.path);
        let pinned = parent(&root, relative.parent().unwrap_or(Path::new("")))?;
        let mut output = File::from(rustix::fs::openat(
            &pinned,
            relative.file_name().context("missing file name")?,
            OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::RUSR | Mode::WUSR,
        )?);
        let mut member = zip.by_name(&format!("data/{}", entry.path))?;
        use sha2::{Digest, Sha256};
        let mut hash = Sha256::new();
        let mut bytes = 0u64;
        loop {
            archive::check_cancel(cancel)?;
            let n = member.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            output.write_all(&buffer[..n])?;
            hash.update(&buffer[..n]);
            bytes += n as u64;
        }
        ensure!(
            bytes == entry.metadata.size && Some(hex::encode(hash.finalize())) == entry.sha256,
            "restored content checksum mismatch: {}",
            entry.path
        );
        output.set_permissions(fs::Permissions::from_mode(entry.metadata.mode & 0o777))?;
        if let Some(time) = chrono::DateTime::from_timestamp(
            entry.metadata.mtime_seconds,
            entry.metadata.mtime_nanoseconds as u32,
        ) {
            output.set_times(fs::FileTimes::new().set_modified(time.into()))?;
        }
        output.sync_all()?;
        count += 1;
        restored.push(entry.path.clone());
    }
    for entry in &links {
        safe_link(
            Path::new(&entry.path),
            entry
                .symlink_target
                .as_deref()
                .context("missing symlink target")?,
            &links,
        )?;
    }
    for entry in &links {
        let relative = Path::new(&entry.path);
        let target = entry
            .symlink_target
            .as_deref()
            .context("missing symlink target")?;
        safe_link(relative, target, &links)?;
        let pinned = parent(&root, relative.parent().unwrap_or(Path::new("")))?;
        rustix::fs::symlinkat(
            target,
            &pinned,
            relative.file_name().context("missing link name")?,
        )?;
        restored.push(entry.path.clone());
    }
    for entry in dirs.into_iter().rev() {
        let dir = open_beneath(
            &root,
            Path::new(&entry.path),
            OFlags::RDONLY | OFlags::DIRECTORY,
        )?;
        dir.set_permissions(fs::Permissions::from_mode(entry.metadata.mode & 0o777))?;
        if let Some(time) = chrono::DateTime::from_timestamp(
            entry.metadata.mtime_seconds,
            entry.metadata.mtime_nanoseconds as u32,
        ) {
            dir.set_times(fs::FileTimes::new().set_modified(time.into()))?;
        }
        dir.sync_all()?;
    }
    if sample.is_none()
        && selected.is_empty()
        && let Some(entry) = manifest
            .entries
            .iter()
            .find(|e| e.path.is_empty() && e.kind == "directory")
    {
        root.set_permissions(fs::Permissions::from_mode(entry.metadata.mode & 0o777))?;
        if let Some(time) = chrono::DateTime::from_timestamp(
            entry.metadata.mtime_seconds,
            entry.metadata.mtime_nanoseconds as u32,
        ) {
            root.set_times(fs::FileTimes::new().set_modified(time.into()))?;
        }
    }
    root.sync_all()?;
    ensure!(
        source::fingerprint(&File::open(path)?.metadata()?) == before,
        "archive changed during extraction; destination preserved for inspection"
    );
    Ok(
        json!({"archive":path,"destination":destination,"job_id":manifest.job_id,"restored":restored,"file_count":count,"verified":true,"sample":sample,"symlinks_restored":symlinks,"metadata_restored":["file and directory permissions without special bits","modification times"],"metadata_not_restored":["ownership","ACLs","extended attributes","hard-link identity"],"symlinks_skipped":!symlinks}),
    )
}
pub fn rehearse(
    state: &dyn crate::api::StateStore,
    config: &Config,
    cancel: &AtomicBool,
) -> Result<Value> {
    let scratch = config
        .rehearsal
        .scratch_dir
        .as_ref()
        .context("rehearsal requires scratch_dir")?;
    isolated(config, scratch)?;
    fs::create_dir_all(scratch)?;
    ensure!(
        !fs::symlink_metadata(scratch)?.is_symlink(),
        "scratch directory cannot be a symlink"
    );
    let mut latest = std::collections::BTreeMap::new();
    for (s, h, _) in state.catalog()? {
        if h && crate::planning::managed(config, &s)
            && latest
                .get(&s.target_id)
                .is_none_or(|previous: &crate::domain::Snapshot| previous.capture_ms < s.capture_ms)
        {
            latest.insert(s.target_id.clone(), s);
        }
    }
    let mut reports = Vec::new();
    let mut failed = 0;
    for snapshot in latest.into_values() {
        archive::check_cancel(cancel)?;
        let destination = scratch.join(format!("rehearsal-{}", uuid::Uuid::new_v4()));
        let report = extract(
            config,
            &snapshot.target.destination_dir.join(&snapshot.filename),
            &destination,
            &[],
            config.rehearsal.sample_files,
            false,
            cancel,
        );
        match report {
            Ok(report) => {
                // Only this uniquely created rehearsal tree is eligible for cleanup.
                for item in walkdir::WalkDir::new(&destination).follow_links(false) {
                    let item = item?;
                    if item.file_type().is_dir() {
                        fs::set_permissions(item.path(), fs::Permissions::from_mode(0o700))?;
                    }
                }
                fs::remove_dir_all(&destination)?;
                reports.push(report);
            }
            Err(e) => {
                failed += 1;
                reports.push(json!({"target":snapshot.target_id,"destination":destination,"error":format!("{e:#}"),"evidence_preserved":true}));
            }
        }
    }
    let report = json!({"id":uuid::Uuid::new_v4().to_string(),"finished_ms":chrono::Utc::now().timestamp_millis(),"healthy":failed==0,"failed":failed,"results":reports});
    state.record_inspection("rehearsal_summary", &report)?;
    Ok(report)
}
