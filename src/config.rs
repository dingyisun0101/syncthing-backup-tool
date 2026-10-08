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
    pub io_cooldown_seconds: u64,
    #[serde(default)]
    pub scrub: ScrubConfig,
    #[serde(default)]
    pub rehearsal: RehearsalConfig,
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
    pub scripts: String,
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
            scripts: "local_process".into(),
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
    pub audit_file: Option<PathBuf>,
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub level: String,
    pub format: String,
}
impl Default for Logging {
    fn default() -> Self {
        Self {
            audit_file: None,
            max_file_bytes: 100 * 1024 * 1024,
            max_files: 10,
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
    #[serde(default)]
    pub manual_only: bool,
    pub source_dir: PathBuf,
    pub destination_dir: PathBuf,
    #[serde(default)]
    pub required_source_mount: Option<PathBuf>,
    #[serde(default)]
    pub required_destination_mount: Option<PathBuf>,
    #[serde(default = "interval")]
    pub backup_interval_seconds: u64,
    #[serde(default)]
    pub interval_anchor: Option<String>,
    #[serde(default = "yes")]
    pub run_on_startup: bool,
    #[serde(default)]
    pub exclude_globs: Vec<String>,
    #[serde(default)]
    pub include_paths: Vec<String>,
    #[serde(default)]
    pub cache: CacheConfig,
    #[serde(default)]
    pub skip_unchanged: bool,
    #[serde(default)]
    pub max_capture_age_seconds: Option<u64>,
    #[serde(default = "reject")]
    pub symlink_policy: String,
    #[serde(default)]
    pub archive: ArchiveConfig,
    #[serde(default)]
    pub storage: StorageConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub hooks: Hooks,
    #[serde(default)]
    pub schedule: Option<CalendarSchedule>,
    #[serde(default = "live")]
    pub consistency: String,
}
fn live() -> String {
    "live".into()
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

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Hooks {
    pub before_backup: Vec<Hook>,
    pub after_capture: Vec<Hook>,
    pub after_backup: Vec<Hook>,
    pub finally: Vec<Hook>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Hook {
    pub name: String,
    pub command: Vec<String>,
    #[serde(default = "hook_timeout")]
    pub timeout_seconds: u64,
    #[serde(default = "hook_fail")]
    pub on_error: String,
    #[serde(default)]
    pub environment: std::collections::BTreeMap<String, String>,
}
fn hook_timeout() -> u64 {
    60
}
fn hook_fail() -> String {
    "fail_job".into()
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CacheConfig {
    pub cachedir_tags: bool,
    pub cargo_build: bool,
    pub approved_paths: Vec<String>,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ScrubConfig {
    pub interval_seconds: Option<u64>,
    pub reopened_read: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RehearsalConfig {
    pub interval_seconds: Option<u64>,
    pub scratch_dir: Option<PathBuf>,
    pub sample_files: Option<usize>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CalendarSchedule {
    pub frequency: String,
    #[serde(default)]
    pub time: String,
    #[serde(default)]
    pub times: Vec<String>,
    pub timezone: String,
    #[serde(default)]
    pub weekday: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ArchiveConfig {
    pub compression: String,
    pub store_extensions: Vec<String>,
    pub reopened_verification: bool,
    pub compression_level: i64,
    pub max_archive_bytes: u64,
    pub max_entries: usize,
    pub max_depth: usize,
}
impl Default for ArchiveConfig {
    fn default() -> Self {
        Self {
            compression: "deflate".into(),
            store_extensions: Vec::new(),
            reopened_verification: false,
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
    pub cooldown_seconds: Option<u64>,
}
impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            min_free_bytes: 10 * 1024 * 1024 * 1024,
            max_staging_bytes: 500 * 1024 * 1024 * 1024,
            cooldown_seconds: None,
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
    pub fn audit_paths(&self) -> Vec<PathBuf> {
        self.logging
            .audit_file
            .as_ref()
            .map(|path| {
                std::iter::once(path.clone())
                    .chain(
                        (1..=self.logging.max_files)
                            .map(|index| PathBuf::from(format!("{}.{}", path.display(), index))),
                    )
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn validate(&self) -> Result<()> {
        ensure!(self.config_version == 1, "unsupported config_version");
        crate::backends::Modules::from_choices(&self.backends)?;
        resolved(&self.state_dir)?;
        ensure!(
            self.io_cooldown_seconds <= 86400,
            "io_cooldown_seconds must be 0..86400"
        );
        for interval in [self.scrub.interval_seconds, self.rehearsal.interval_seconds]
            .into_iter()
            .flatten()
        {
            ensure!(
                (1..=31_536_000).contains(&interval),
                "invalid inspection interval"
            );
        }
        ensure!(
            self.rehearsal.sample_files != Some(0),
            "rehearsal sample_files must be positive or null"
        );
        ensure!(
            self.rehearsal.interval_seconds.is_none() || self.rehearsal.scratch_dir.is_some(),
            "scheduled rehearsal requires scratch_dir"
        );
        if let Some(scratch) = &self.rehearsal.scratch_dir {
            resolved(scratch)?;
            ensure!(
                !overlap(scratch, &self.state_dir)?,
                "rehearsal scratch overlaps state"
            );
            for target in &self.targets {
                ensure!(
                    !overlap(scratch, &target.source_dir)?
                        && !overlap(scratch, &target.destination_dir)?,
                    "rehearsal scratch overlaps target data"
                );
            }
        }
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
        ensure!(
            self.logging.max_file_bytes >= 1024 * 1024
                && (1..=100).contains(&self.logging.max_files),
            "invalid log rotation limits"
        );
        let audit_paths = self.audit_paths();
        for path in &audit_paths {
            resolved(path)?;
            ensure!(
                !overlap(path, &self.state_dir)?,
                "audit path overlaps state data"
            );
        }
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
                ["reject", "skip", "preserve"].contains(&t.symlink_policy.as_str()),
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
            for path in t.include_paths.iter().chain(&t.cache.approved_paths) {
                ensure!(
                    !path.is_empty()
                        && path.len() <= 4096
                        && !path.contains('\\')
                        && Path::new(path)
                            .components()
                            .all(|c| matches!(c, std::path::Component::Normal(_))),
                    "include/cache paths must be nonempty source-relative paths"
                );
            }
            ensure!(
                t.storage.cooldown_seconds.is_none_or(|s| s <= 86400),
                "storage.cooldown_seconds must be 0..86400"
            );
            ensure!(
                t.max_capture_age_seconds != Some(0),
                "max_capture_age_seconds must be positive"
            );
            ensure!(
                !t.skip_unchanged || t.max_capture_age_seconds.is_some(),
                "skip_unchanged requires max_capture_age_seconds to bound capture age"
            );
            for suffix in &t.archive.store_extensions {
                ensure!(
                    suffix.starts_with('.')
                        && suffix.len() <= 32
                        && suffix
                            .chars()
                            .all(|c| c == '.' || c.is_ascii_alphanumeric()),
                    "store_extensions must be simple dot-prefixed suffixes"
                );
            }
            ensure!(
                t.schedule.is_none() || t.interval_anchor.is_none(),
                "calendar schedule and interval_anchor are mutually exclusive"
            );
            if let Some(anchor) = &t.interval_anchor {
                chrono::DateTime::parse_from_rfc3339(anchor)
                    .context("interval_anchor must be RFC3339 with explicit offset")?;
            }
            ensure!(
                ["live", "application_quiesced"].contains(&t.consistency.as_str()),
                "invalid consistency mode"
            );
            if let Some(schedule) = &t.schedule {
                crate::scheduler::calendar_next(schedule, chrono::Utc::now().timestamp_millis())?;
            }
            for (phase, hooks) in [
                ("before_backup", &t.hooks.before_backup),
                ("after_capture", &t.hooks.after_capture),
                ("after_backup", &t.hooks.after_backup),
                ("finally", &t.hooks.finally),
            ] {
                for hook in hooks {
                    ensure!(
                        !hook.name.is_empty()
                            && !hook.command.is_empty()
                            && Path::new(&hook.command[0]).is_absolute(),
                        "hook needs a name and absolute executable path"
                    );
                    ensure!(
                        (1..=3600).contains(&hook.timeout_seconds),
                        "hook timeout must be 1..3600"
                    );
                    ensure!(
                        ["skip_backup", "retry_backup", "fail_job", "continue"]
                            .contains(&hook.on_error.as_str()),
                        "invalid hook on_error"
                    );
                    ensure!(
                        phase != "after_backup"
                            || ["fail_job", "continue"].contains(&hook.on_error.as_str()),
                        "after_backup cannot skip or recapture an already published archive"
                    );
                    ensure!(
                        phase != "finally" || hook.on_error == "fail_job",
                        "finally hooks are mandatory and require on_error=fail_job"
                    );
                }
            }
            for path in &audit_paths {
                ensure!(
                    !overlap(path, &t.source_dir)? && !overlap(path, &t.destination_dir)?,
                    "audit path overlaps target data"
                );
            }

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

/// Reports expose settings without hook environment values or command arguments.
pub fn redacted(mut value: serde_json::Value) -> serde_json::Value {
    fn walk(value: &mut serde_json::Value) {
        match value {
            serde_json::Value::Object(object) => {
                if let Some(hooks) = object.get_mut("hooks").and_then(|h| h.as_object_mut()) {
                    for phase in hooks.values_mut() {
                        if let Some(hooks) = phase.as_array_mut() {
                            for hook in hooks {
                                if let Some(environment) =
                                    hook.get_mut("environment").and_then(|e| e.as_object_mut())
                                {
                                    for v in environment.values_mut() {
                                        *v = "<redacted>".into();
                                    }
                                }
                                if let Some(command) =
                                    hook.get_mut("command").and_then(|c| c.as_array_mut())
                                {
                                    for arg in command.iter_mut().skip(1) {
                                        *arg = "<redacted>".into();
                                    }
                                }
                            }
                        }
                    }
                }
                for item in object.values_mut() {
                    walk(item);
                }
            }
            serde_json::Value::Array(array) => {
                for item in array {
                    walk(item);
                }
            }
            _ => (),
        }
    }
    walk(&mut value);
    value
}
