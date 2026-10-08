use crate::{
    config::Target,
    domain::{Fingerprint, Permanent},
    storage::{mount_present, open_beneath, proc_path},
};
use crate::{domain::Entry, resources::MemoryBudget};
use anyhow::{Result, ensure};
use rustix::fs::OFlags;
use std::{
    fs::{self, File, Metadata},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

pub fn fingerprint(m: &Metadata) -> Fingerprint {
    Fingerprint {
        device: m.dev(),
        inode: m.ino(),
        size: m.len(),
        mtime_seconds: m.mtime(),
        mtime_nanoseconds: m.mtime_nsec(),
        ctime_seconds: m.ctime(),
        ctime_nanoseconds: m.ctime_nsec(),
        mode: m.mode(),
    }
}

pub struct Source {
    pub root: File,
    path: PathBuf,
    required_mount: Option<PathBuf>,
}
impl Source {
    pub fn open(t: &Target) -> Result<Self> {
        if let Some(mount) = &t.required_source_mount {
            mount_present(mount)?;
        }
        let root = File::open(&t.source_dir)?;
        ensure!(root.metadata()?.is_dir(), "source is not a directory");
        Ok(Self {
            root,
            path: t.source_dir.clone(),
            required_mount: t.required_source_mount.clone(),
        })
    }
    pub fn open_entry(&self, path: &Path, directory: bool) -> Result<File> {
        open_beneath(
            &self.root,
            path,
            OFlags::RDONLY
                | OFlags::NONBLOCK
                | if directory {
                    OFlags::DIRECTORY
                } else {
                    OFlags::empty()
                },
        )
    }
    pub fn check(&self) -> Result<()> {
        if let Some(mount) = &self.required_mount {
            mount_present(mount)?;
        }
        let current = fs::metadata(&self.path)?;
        let pinned = self.root.metadata()?;
        ensure!(
            current.dev() == pinned.dev() && current.ino() == pinned.ino(),
            "source directory changed during capture"
        );
        Ok(())
    }
}

/// Inspect selection and metadata. rsync performs the actual data copying.
pub fn inventory(
    source: &Source,
    target: &Target,
    resources: &crate::config::Resources,
) -> Result<Vec<Entry>> {
    let base = proc_path(&source.root);
    let exclusions = target.exclusions()?;
    let mut budget = MemoryBudget::new(resources);
    let mut entries = Vec::new();
    let mut total = 0u64;
    let walker = walkdir::WalkDir::new(&base)
        .follow_links(false)
        .max_depth(target.archive.max_depth + 1)
        .into_iter()
        .filter_entry(|entry| {
            if entry.depth() == 0 {
                return true;
            }
            let relative = entry.path().strip_prefix(&base).unwrap_or(entry.path());
            let path = relative.to_string_lossy();
            !exclusions.is_match(relative) && !exclusions.is_match(format!("{path}/"))
        });
    for item in walker {
        let item = item?;
        ensure!(
            item.depth()
                .saturating_sub(usize::from(!item.file_type().is_dir()))
                <= target.archive.max_depth,
            Permanent("max_depth exceeded".into())
        );
        let relative = item.path().strip_prefix(&base)?;
        let path = relative
            .to_str()
            .ok_or_else(|| Permanent("source filename is not UTF-8".into()))?
            .to_owned();
        ensure!(
            path.len() <= 4096 && !path.contains('\\'),
            Permanent("unsupported source-relative filename".into())
        );
        ensure!(
            entries.len() < target.archive.max_entries,
            Permanent("max_entries exceeded".into())
        );
        budget.entry(&path)?;
        let metadata = if item.depth() == 0 {
            source.root.metadata()?
        } else {
            fs::symlink_metadata(item.path())?
        };
        let kind = if metadata.is_symlink() {
            ensure!(
                target.symlink_policy == "skip",
                Permanent(format!("symlink rejected: {path}"))
            );
            "skipped_symlink"
        } else if metadata.is_dir() {
            "directory"
        } else if metadata.is_file() {
            "file"
        } else {
            return Err(Permanent(format!("unsupported source entry: {path}")).into());
        };
        if kind == "file" {
            total = total
                .checked_add(metadata.len())
                .ok_or_else(|| Permanent("staging byte count overflow".into()))?;
        }
        ensure!(
            total <= target.storage.max_staging_bytes,
            Permanent("max_staging_bytes exceeded".into())
        );
        if kind != "skipped_symlink" && item.depth() != 0 {
            let opened = source.open_entry(relative, kind == "directory")?;
            ensure!(
                fingerprint(&opened.metadata()?) == fingerprint(&metadata),
                "source entry changed during inspection: {path}"
            );
        }
        entries.push(Entry {
            path,
            kind: kind.into(),
            metadata: fingerprint(&metadata),
            sha256: None,
        });
    }
    Ok(entries)
}

pub struct Rsync;
pub struct LiveDirectory;
impl crate::api::SourceProvider for LiveDirectory {
    fn open(&self, target: &Target) -> Result<Box<dyn crate::api::SourceSession>> {
        Ok(Box::new(Source::open(target)?))
    }
}
impl crate::api::SourceSession for Source {
    fn rooted_path(&self) -> PathBuf {
        use std::os::fd::AsRawFd;
        PathBuf::from(format!(
            "/proc/{}/fd/{}/",
            std::process::id(),
            self.root.as_raw_fd()
        ))
    }
    fn check(&self) -> Result<()> {
        Source::check(self)
    }
    fn consistency(&self) -> &str {
        "live"
    }
    fn inventory(
        &self,
        target: &Target,
        resources: &crate::config::Resources,
    ) -> Result<Vec<Entry>> {
        inventory(self, target, resources)
    }
    fn fingerprint(&self, path: &Path, directory: bool) -> Result<Fingerprint> {
        let file = if path.as_os_str().is_empty() {
            self.root.try_clone()?
        } else {
            self.open_entry(path, directory)?
        };
        Ok(fingerprint(&file.metadata()?))
    }
}
impl crate::api::Synchronizer for Rsync {
    fn copy(&self, request: crate::api::CopyRequest<'_>) -> Result<()> {
        let mut command =
            crate::process::tool("/usr/bin/rsync", request.memory_limit, request.file_limit);
        command
            .args([
                "--dirs",
                "--no-recursive",
                "--links",
                "--perms",
                "--times",
                "--relative",
                "--no-implied-dirs",
                "--whole-file",
                "--inplace",
                "--from0",
            ])
            .arg(format!("--files-from={}", request.files_from.display()))
            .arg("--")
            .arg(request.source)
            .arg(request.destination);
        crate::process::run(&mut command, request.cancel, request.monitor)
    }
}
