use crate::telemetry;
use crate::{
    config::Target,
    domain::{Fingerprint, Permanent},
    storage::{mount_present, open_beneath, proc_path},
};
use crate::{domain::Entry, resources::MemoryBudget};
use anyhow::{Result, ensure};
use rustix::fs::OFlags;
use serde_json::json;
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

#[derive(Debug, serde::Serialize)]
pub struct Omission {
    pub path: String,
    pub reason: String,
    pub subtree: bool,
}
#[derive(Debug, serde::Serialize)]
pub struct CacheCandidate {
    pub path: String,
    pub reason: String,
    pub approved: bool,
    pub include_override: bool,
}
#[derive(Debug, serde::Serialize)]
pub struct Selection {
    pub entries: Vec<Entry>,
    pub omitted: Vec<Omission>,
    pub cache_candidates: Vec<CacheCandidate>,
    pub errors: Vec<String>,
    #[serde(skip)]
    pub permanent_error: bool,
    pub unmatched_exclusions: Vec<String>,
    pub selected_bytes: u64,
    pub file_count: usize,
    pub entry_budget_bytes: u64,
}
fn protected(target: &Target, path: &Path) -> bool {
    target
        .include_paths
        .iter()
        .any(|include| path.starts_with(include) || Path::new(include).starts_with(path))
}
fn valid_tag(source: &Source, path: &Path) -> bool {
    use std::io::Read;
    let Ok(mut file) = source.open_entry(&path.join("CACHEDIR.TAG"), false) else {
        return false;
    };
    if !file.metadata().is_ok_and(|m| m.is_file()) {
        return false;
    }
    let mut header = [0u8; 43];
    file.read_exact(&mut header).is_ok()
        && &header == b"Signature: 8a477f597d28d172789f06886806bc55"
}
fn cargo_build(source: &Source, path: &Path) -> bool {
    use std::io::Read;
    if path.file_name().is_none_or(|n| n != "target") || !valid_tag(source, path) {
        return false;
    }
    let parent = path.parent().unwrap_or(Path::new(""));
    if !source
        .open_entry(&parent.join("Cargo.toml"), false)
        .is_ok_and(|f| f.metadata().is_ok_and(|m| m.is_file()))
    {
        return false;
    }
    let Ok(file) = source.open_entry(&path.join(".rustc_info.json"), false) else {
        return false;
    };
    if !file.metadata().is_ok_and(|m| m.is_file()) {
        return false;
    }
    let mut bytes = Vec::new();
    if file.take(65537).read_to_end(&mut bytes).is_err() || bytes.len() > 65536 {
        return false;
    }
    serde_json::from_slice::<serde_json::Value>(&bytes)
        .is_ok_and(|v| v["rustc_fingerprint"].is_number())
}
/// The backup and read-only preview use this same bounded selection walk.
pub fn inspect(
    source: &Source,
    target: &Target,
    resources: &crate::config::Resources,
) -> Result<Selection> {
    let base = proc_path(&source.root);
    let exclusions = target.exclusions()?;
    let mut matches = vec![false; target.exclude_globs.len()];
    let mut budget = MemoryBudget::new(resources);
    let mut report = Selection {
        entries: Vec::new(),
        omitted: Vec::new(),
        cache_candidates: Vec::new(),
        errors: Vec::new(),
        permanent_error: false,
        unmatched_exclusions: Vec::new(),
        selected_bytes: 0,
        file_count: 0,
        entry_budget_bytes: 0,
    };
    let mut inherited = std::collections::HashMap::<PathBuf, String>::new();
    let mut walker = walkdir::WalkDir::new(&base)
        .follow_links(false)
        .max_depth(target.archive.max_depth + 1)
        .into_iter();
    while let Some(item) = walker.next() {
        let item = match item {
            Ok(item) => item,
            Err(e) => {
                report.errors.push(e.to_string());
                continue;
            }
        };
        let relative = item.path().strip_prefix(&base)?;
        let Some(path) = relative.to_str() else {
            report.errors.push("source filename is not UTF-8".into());
            if item.file_type().is_dir() {
                walker.skip_current_dir();
            }
            continue;
        };
        if path.len() > 4096 || path.contains('\\') {
            report.permanent_error = true;
            report
                .errors
                .push(format!("unsupported filename: {path:?}"));
            break;
        }
        if let Err(e) = budget.entry(path) {
            report.permanent_error = true;
            report.errors.push(format!("{e:#}"));
            break;
        }
        report.entry_budget_bytes = report
            .entry_budget_bytes
            .saturating_add(8192 + path.len() as u64 * 16);
        let is_dir = item.depth() == 0 || item.file_type().is_dir();
        let mut indexes = exclusions.matches(relative);
        indexes.extend(exclusions.matches(format!("{path}/")));
        for index in &indexes {
            matches[*index] = true;
        }
        let mut reason = indexes
            .first()
            .map(|i| format!("exclude_globs[{}]: {}", i, target.exclude_globs[*i]));
        if reason.is_none() {
            reason = relative
                .ancestors()
                .skip(1)
                .find_map(|parent| inherited.get(parent).cloned());
        }
        if is_dir && item.depth() > 0 {
            let candidate = if target.cache.cargo_build && cargo_build(source, relative) {
                Some("reviewed Cargo build-directory preset")
            } else if target.cache.cachedir_tags && valid_tag(source, relative) {
                Some("valid CACHEDIR.TAG")
            } else {
                None
            };
            if let Some(rule) = candidate {
                let approved = target
                    .cache
                    .approved_paths
                    .iter()
                    .any(|p| Path::new(p) == relative);
                report.cache_candidates.push(CacheCandidate {
                    path: path.into(),
                    reason: rule.into(),
                    approved,
                    include_override: protected(target, relative),
                });
                if approved {
                    reason = Some(format!("cache: {rule}"));
                }
            }
        }
        if let Some(rule) = &reason {
            if is_dir {
                inherited.insert(relative.to_owned(), rule.clone());
            }
            if !protected(target, relative) && item.depth() > 0 {
                report.omitted.push(Omission {
                    path: path.into(),
                    reason: rule.clone(),
                    subtree: is_dir,
                });
                if is_dir {
                    walker.skip_current_dir();
                }
                continue;
            }
        }
        let result = (|| -> Result<Entry> {
            ensure!(
                item.depth().saturating_sub(usize::from(!is_dir)) <= target.archive.max_depth,
                Permanent("max_depth exceeded".into())
            );
            ensure!(
                report.entries.len() < target.archive.max_entries,
                Permanent("max_entries exceeded".into())
            );
            let metadata = if item.depth() == 0 {
                source.root.metadata()?
            } else {
                fs::symlink_metadata(item.path())?
            };
            let kind = if metadata.is_symlink() {
                ensure!(
                    target.symlink_policy != "reject",
                    Permanent(format!("symlink rejected: {path}"))
                );
                if target.symlink_policy == "preserve" {
                    "symlink"
                } else {
                    "skipped_symlink"
                }
            } else if metadata.is_dir() {
                "directory"
            } else if metadata.is_file() {
                "file"
            } else {
                return Err(Permanent(format!("unsupported source entry: {path}")).into());
            };
            if !matches!(kind, "symlink" | "skipped_symlink") && item.depth() != 0 {
                let opened = source.open_entry(relative, kind == "directory")?;
                ensure!(
                    fingerprint(&opened.metadata()?) == fingerprint(&metadata),
                    "source entry changed during inspection: {path}"
                );
            }
            let symlink_target = if kind == "symlink" {
                Some(
                    fs::read_link(item.path())?
                        .to_str()
                        .ok_or_else(|| Permanent("non-UTF-8 symlink target".into()))?
                        .to_owned(),
                )
            } else {
                None
            };
            Ok(Entry {
                path: path.into(),
                kind: kind.into(),
                metadata: fingerprint(&metadata),
                sha256: None,
                symlink_target,
            })
        })();
        match result {
            Ok(entry) => {
                if entry.kind == "file" {
                    report.file_count += 1;
                    report.selected_bytes =
                        report.selected_bytes.saturating_add(entry.metadata.size);
                }
                if entry.kind == "skipped_symlink" {
                    report.omitted.push(Omission {
                        path: entry.path.clone(),
                        reason: "symlink_policy: skip".into(),
                        subtree: false,
                    });
                }
                report.entries.push(entry);
            }
            Err(e) => {
                report.permanent_error |= e.is::<Permanent>();
                report.errors.push(format!("{path}: {e:#}"));
                if is_dir {
                    walker.skip_current_dir();
                }
            }
        }
        if report.selected_bytes > target.storage.max_staging_bytes {
            report.permanent_error = true;
            report.errors.push("max_staging_bytes exceeded".into());
            break;
        }
    }
    if let Err(e) = source.check() {
        report.errors.push(format!("{e:#}"));
    }
    report.unmatched_exclusions = target
        .exclude_globs
        .iter()
        .zip(matches)
        .filter(|(_, matched)| !*matched)
        .map(|(rule, _)| rule.clone())
        .collect();
    Ok(report)
}
pub fn inventory(
    source: &Source,
    target: &Target,
    resources: &crate::config::Resources,
) -> Result<Vec<Entry>> {
    let report = inspect(source, target, resources)?;
    if report.permanent_error {
        return Err(Permanent(format!(
            "source selection failed: {}",
            report.errors.join("; ")
        ))
        .into());
    }
    ensure!(
        report.errors.is_empty(),
        "source selection failed: {}",
        report.errors.join("; ")
    );
    for omission in &report.omitted {
        telemetry::audit("source.entry", "excluded", json!(omission))?;
    }
    for candidate in &report.cache_candidates {
        telemetry::audit("source.cache", "candidate", json!(candidate))?;
    }
    for entry in &report.entries {
        telemetry::audit(
            "source.entry",
            "selected",
            json!({"path":entry.path,"kind":entry.kind,"bytes":entry.metadata.size}),
        )?;
    }
    Ok(report.entries)
}

pub struct Rsync;
pub struct LiveDirectory;
impl crate::api::SourceProvider for LiveDirectory {
    fn open(&self, target: &Target) -> Result<Box<dyn crate::api::SourceSession>> {
        Ok(Box::new(Source::open(target)?))
    }
}
impl crate::api::SourceSession for Source {
    fn file(&self, path: &Path) -> Result<File> {
        self.open_entry(path, false)
    }
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
    fn symlink(&self, path: &Path) -> Result<(Fingerprint, String)> {
        let parent = path.parent().unwrap_or(Path::new(""));
        let directory = if parent.as_os_str().is_empty() {
            self.root.try_clone()?
        } else {
            self.open_entry(parent, true)?
        };
        let leaf = proc_path(&directory).join(
            path.file_name()
                .ok_or_else(|| anyhow::anyhow!("missing symlink filename"))?,
        );
        let metadata = fs::symlink_metadata(&leaf)?;
        ensure!(metadata.is_symlink(), "source symlink changed type");
        let target = fs::read_link(&leaf)?
            .to_str()
            .ok_or_else(|| Permanent("non-UTF-8 symlink target".into()))?
            .to_owned();
        Ok((fingerprint(&metadata), target))
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
