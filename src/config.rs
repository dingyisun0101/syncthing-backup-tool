use anyhow::{Context, Result, ensure};
use globset::{GlobBuilder, GlobSet, GlobSetBuilder};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

pub const DEFAULT_CONFIG: &str = "/etc/syncthing-backup-tool/config.json";
pub const DEFAULT_SOCKET: &str = "/run/syncthing-backup-tool/control.sock";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub config_version: u32,
    #[serde(default)]
    pub backends: BackendChoices,
    pub state_dir: PathBuf,
    #[serde(default = "grace")]
    pub shutdown_grace_seconds: u64,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub resources: Resources,
    #[serde(default)]
    pub retention: SweepConfig,
    #[serde(default)]
    pub logging: Logging,
    pub targets: Vec<Target>,
}
fn grace() -> u64 {
    60
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct BackendChoices {
    pub source: String,
    pub sync: String,
    pub archive: String,
    pub storage: String,
    pub state: String,
    pub scheduler: String,
    pub queue: String,
    pub retention: String,
}
impl Default for BackendChoices {
    fn default() -> Self {
        Self {
            source: "live_directory".into(),
            sync: "rsync".into(),
            archive: "infozip".into(),
            storage: "local".into(),
            state: "sqlite".into(),
            scheduler: "interval".into(),
            queue: "bounded_fifo".into(),
            retention: "oldest_first".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueConfig {
    pub max_pending_jobs: usize,
    pub max_attempts: u32,
    pub retry_initial_seconds: u64,
    pub retry_max_seconds: u64,
}
impl Default for QueueConfig {
    fn default() -> Self {
        Self {
            max_pending_jobs: 16,
            max_attempts: 3,
            retry_initial_seconds: 30,
            retry_max_seconds: 900,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct Resources {
    pub max_worker_threads: usize,
    pub max_concurrent_snapshots: usize,
    pub memory_budget_bytes: u64,
    pub memory_limit_bytes: u64,
    pub io_buffer_bytes: usize,
}
impl Default for Resources {
    fn default() -> Self {
        Self {
            max_worker_threads: 2,
            max_concurrent_snapshots: 1,
            memory_budget_bytes: 256 * 1024 * 1024,
            memory_limit_bytes: 512 * 1024 * 1024,
            io_buffer_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SweepConfig {
    pub sweep_interval_seconds: u64,
}
impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            sweep_interval_seconds: 3600,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Logging {
    pub level: String,
    pub format: String,
}
impl Default for Logging {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: "json".into(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Target {
    pub id: String,
    #[serde(default = "yes")]
    pub enabled: bool,
    pub source_dir: PathBuf,
    pub destination_dir: PathBuf,
    #[serde(default)]
    pub required_source_mount: Option<PathBuf>,
    #[serde(default)]
    pub required_destination_mount: Option<PathBuf>,
    #[serde(default = "interval")]
    pub backup_interval_seconds: u64,
    #[serde(default = "yes")]
    pub run_on_startup: bool,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
    #[serde(default = "reject")]
    pub symlink_policy: String,
    #[serde(default)]
    pub archive: ArchiveConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
}
fn yes() -> bool {
    true
}
fn interval() -> u64 {
    21600
}
fn reject() -> String {
    "reject".into()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArchiveConfig {
    pub compression: String,
    pub compression_level: i64,
    pub max_archive_bytes: u64,
    pub max_entries: usize,
    pub max_depth: usize,
}
impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            compression: "deflate".into(),
            compression_level: 6,
            max_archive_bytes: 100 * 1024 * 1024 * 1024,
            max_entries: 1_000_000,
            max_depth: 128,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StorageConfig {
    pub min_free_bytes: u64,
    pub max_staging_bytes: u64,
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            min_free_bytes: 10 * 1024 * 1024 * 1024,
            max_staging_bytes: 500 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionConfig {
    pub min_snapshots: usize,
    pub max_snapshots: usize,
    pub max_age_seconds: Option<u64>,
    pub max_total_bytes: Option<u64>,
}
impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            min_snapshots: 2,
            max_snapshots: 30,
            max_age_seconds: None,
            max_total_bytes: None,
        }
    }
}

pub fn load(path: &Path) -> Result<Config> {
    use std::io::Read;
    let file = fs::File::open(path).with_context(|| format!("read config {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= 1024 * 1024, "config exceeds 1 MiB");
    let config: Config = serde_json::from_slice(&bytes).context("parse config JSON")?;
    config.validate()?;
    Ok(config)
}

/// Resolve existing ancestors, including symlinks, without creating a path.
pub fn resolved(path: &Path) -> Result<PathBuf> {
    ensure!(
        path.is_absolute(),
        "path must be absolute: {}",
        path.display()
    );
    ensure!(
        !path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir)),
        "path must not contain '..': {}",
        path.display()
    );
    if path.exists() {
        return Ok(fs::canonicalize(path)?);
    }
    let parent = path.parent().context("path has no existing ancestor")?;
    Ok(resolved(parent)?.join(path.file_name().context("missing filename")?))
}

pub fn overlap(a: &Path, b: &Path) -> Result<bool> {
    let a = resolved(a)?;
    let b = resolved(b)?;
    Ok(a.starts_with(&b) || b.starts_with(&a))
}

impl Config {
    pub fn validate(&self) -> Result<()> {
        ensure!(self.config_version == 1, "unsupported config_version");
        crate::backends::Modules::from_choices(&self.backends)?;
        resolved(&self.state_dir)?;
        ensure!(
            self.shutdown_grace_seconds > 0 && self.shutdown_grace_seconds <= 86400,
            "shutdown_grace_seconds must be 1..86400"
        );
        let q = &self.queue;
        ensure!(
            q.max_pending_jobs > 0 && q.max_attempts > 0,
            "queue capacities and attempts must be positive"
        );
        ensure!(
            q.retry_initial_seconds > 0
                && q.retry_initial_seconds <= q.retry_max_seconds
                && q.retry_max_seconds <= 86400,
            "invalid retry intervals (maximum 86400 seconds)"
        );
        let r = &self.resources;
        ensure!(
            r.max_concurrent_snapshots > 0
                && r.max_concurrent_snapshots <= r.max_worker_threads
                && r.max_worker_threads <= 256,
            "invalid worker/concurrency limits (maximum 256)"
        );
        ensure!(
            r.memory_budget_bytes < r.memory_limit_bytes
                && r.memory_budget_bytes <= usize::MAX as u64,
            "memory budget must fit address space and be below memory limit"
        );
        let per_job = r.memory_budget_bytes / r.max_concurrent_snapshots as u64;
        ensure!(
            r.io_buffer_bytes > 0
                && (r.io_buffer_bytes as u64)
                    .saturating_mul(4)
                    .saturating_add(16 * 1024 * 1024)
                    < per_job,
            "memory budget too small for buffers and archive metadata"
        );
        ensure!(
            (1..=31_536_000).contains(&self.retention.sweep_interval_seconds),
            "invalid retention sweep interval"
        );
        ensure!(
            ["debug", "info", "warn", "error"].contains(&self.logging.level.as_str()),
            "invalid logging.level"
        );
        ensure!(
            ["json", "text"].contains(&self.logging.format.as_str()),
            "invalid logging.format"
        );
        let mut ids = HashSet::new();
        for t in &self.targets {
            ensure!(
                !t.id.is_empty()
                    && t.id.len() <= 64
                    && t.id
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'),
                "invalid target ID {:?}",
                t.id
            );
            ensure!(ids.insert(&t.id), "duplicate target ID {}", t.id);
            ensure!(
                (1..=31_536_000).contains(&t.backup_interval_seconds),
                "invalid backup interval for {}",
                t.id
            );
            for p in [&t.source_dir, &t.destination_dir] {
                resolved(p)?;
            }
            for (mount, path) in [
                (&t.required_source_mount, &t.source_dir),
                (&t.required_destination_mount, &t.destination_dir),
            ] {
                if let Some(mount) = mount {
                    ensure!(
                        resolved(path)?.starts_with(resolved(mount)?),
                        "directory must be beneath required mount for {}",
                        t.id
                    );
                }
            }
            ensure!(
                ["reject", "skip"].contains(&t.symlink_policy.as_str()),
                "invalid symlink policy for {}",
                t.id
            );
            ensure!(
                ["store", "deflate"].contains(&t.archive.compression.as_str()),
                "invalid compression for {}",
                t.id
            );
            ensure!(
                (0..=9).contains(&t.archive.compression_level),
                "compression_level must be 0..9"
            );
            ensure!(
                t.archive.max_entries > 0
                    && (1..=256).contains(&t.archive.max_depth)
                    && t.archive.max_archive_bytes > 0,
                "invalid archive limits for {}",
                t.id
            );
            let p = &t.retention;
            ensure!(
                t.storage.max_staging_bytes > 0,
                "max_staging_bytes must be positive"
            );
            ensure!(
                p.min_snapshots > 0 && p.min_snapshots <= p.max_snapshots,
                "invalid retention count for {}",
                t.id
            );
            ensure!(
                p.max_age_seconds != Some(0) && p.max_total_bytes != Some(0),
                "optional retention limits must be positive or null"
            );
            t.exclusions()?;
            ensure!(
                !overlap(&self.state_dir, &t.source_dir)?
                    && !overlap(&self.state_dir, &t.destination_dir)?,
                "state directory overlaps target {}",
                t.id
            );
            for other in &self.targets {
                ensure!(
                    !overlap(&t.destination_dir, &other.source_dir)?,
                    "destination {} overlaps source {}",
                    t.id,
                    other.id
                );
                if t.id != other.id {
                    ensure!(
                        !overlap(&t.destination_dir, &other.destination_dir)?,
                        "overlapping destinations for {} and {}",
                        t.id,
                        other.id
                    );
                }
            }
        }
        Ok(())
    }
}

impl Target {
    pub fn exclusions(&self) -> Result<GlobSet> {
        let mut builder = GlobSetBuilder::new();
        for pattern in &self.exclude_globs {
            builder.add(
                GlobBuilder::new(pattern)
                    .literal_separator(true)
                    .build()
                    .with_context(|| format!("invalid exclusion {pattern:?}"))?,
            );
        }
        Ok(builder.build()?)
    }

    /// Retention changes form a new cohort; old archives retain their policy.
    pub fn policy_id(&self) -> Result<String> {
        use sha2::{Digest, Sha256};
        let bytes = serde_json::to_vec(&(
            &self.id,
            &self.source_dir,
            &self.destination_dir,
            &self.retention,
        ))?;
        Ok(hex::encode(Sha256::digest(bytes)))
    }
}
