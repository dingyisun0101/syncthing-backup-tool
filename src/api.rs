//! Versioned module contracts. Backends expose operations, never their internal
//! database connections, process handles, or filesystem locks.
use crate::{
    config::{Config, QueueConfig, Resources, RetentionConfig, Target},
    domain::{Job, JobSpec, Snapshot},
};
use anyhow::Result;
use std::{
    fs::File,
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
};

pub const INTERFACE_VERSION: u32 = 3;
pub type Monitor<'a> = &'a dyn Fn() -> Result<()>;

pub struct CopyRequest<'a> {
    pub source: &'a Path,
    pub destination: &'a Path,
    pub files_from: &'a Path,
    pub memory_limit: u64,
    pub file_limit: u64,
    pub cancel: &'a AtomicBool,
    pub monitor: Monitor<'a>,
}
pub trait Synchronizer: Send + Sync {
    fn copy(&self, request: CopyRequest<'_>) -> Result<()>;
}

pub trait SourceSession: Send {
    fn file(&self, path: &Path) -> Result<File>;
    fn rooted_path(&self) -> PathBuf;
    fn check(&self) -> Result<()>;
    fn consistency(&self) -> &str;
    fn inventory(
        &self,
        target: &Target,
        resources: &Resources,
    ) -> Result<Vec<crate::domain::Entry>>;
    fn fingerprint(&self, path: &Path, directory: bool) -> Result<crate::domain::Fingerprint>;
    fn symlink(&self, path: &Path) -> Result<(crate::domain::Fingerprint, String)>;
}
pub trait SourceProvider: Send + Sync {
    fn open(&self, target: &Target) -> Result<Box<dyn SourceSession>>;
}

pub struct PackRequest<'a> {
    pub tree: &'a Path,
    pub output: &'a Path,
    pub target: &'a Target,
    pub memory_limit: u64,
    pub cancel: &'a AtomicBool,
    pub monitor: Monitor<'a>,
}
pub struct TestRequest<'a> {
    pub archive: &'a Path,
    pub memory_limit: u64,
    pub file_limit: u64,
    pub cancel: &'a AtomicBool,
    pub monitor: Monitor<'a>,
}
pub trait Archiver: Send + Sync {
    fn pack(&self, request: PackRequest<'_>) -> Result<()>;
    fn test(&self, request: TestRequest<'_>) -> Result<()>;
}

pub trait OperationGuard: Send {}
impl<T: Send> OperationGuard for T {}
pub trait DestinationSession: Send {
    /// Pinned private working directory, usable by tool subprocesses.
    fn working_directory(&self) -> PathBuf;
    fn check(&self) -> Result<()>;
    fn check_space(&self, additional: u64) -> Result<()>;
    fn serialize_writes(&self, state_dir: &Path) -> Result<Box<dyn OperationGuard>>;
    fn archive(&self, name: &str) -> Result<File>;
    fn exists(&self, name: &str) -> Result<bool>;
    fn synchronize(&self) -> Result<()>;
    fn publish(&self, id: &str, name: &str) -> Result<()>;
    fn remove(&self, name: &str) -> Result<()>;
    fn remove_temporary(&self, id: &str) -> Result<()>;
    fn remove_staging(&self, id: &str) -> Result<()>;
}
pub trait StorageProvider: Send + Sync {
    fn open(
        &self,
        target: &Target,
        initialize: bool,
        state_dir: &Path,
    ) -> Result<Box<dyn DestinationSession>>;
}

pub trait SchedulingPolicy: Send + Sync {
    fn next_for(&self, target: &Target, previous: i64, now: i64) -> Result<i64> {
        if target.manual_only || target.schedule.is_some() || target.interval_anchor.is_some() {
            crate::scheduler::next_for(target, previous, now)
        } else {
            Ok(self.next_due(previous, now, target.backup_interval_seconds))
        }
    }

    fn next_due(&self, previous: i64, now: i64, interval_seconds: u64) -> i64;
}
pub trait QueuePolicy: Send + Sync {
    fn admit(&self, outstanding: bool, pending: usize, maximum: usize) -> bool;
    fn retry_at(
        &self,
        error: &anyhow::Error,
        attempts: u32,
        settings: &QueueConfig,
        now: i64,
    ) -> Option<i64>;
}
pub trait RetentionPolicy: Send + Sync {
    fn plan(&self, snapshots: Vec<Snapshot>, policy: &RetentionConfig, now: i64) -> Vec<Snapshot>;
}

pub trait StateStore: Send + Sync {
    fn io_activity(&self) -> Result<Vec<(String, i64, bool)>>;
    fn set_io_activity(&self, filesystem: &str, finished_ms: i64, active: bool) -> Result<()>;
    fn record_inspection(&self, kind: &str, report: &serde_json::Value) -> Result<()>;
    fn inspections(&self, kind: &str) -> Result<Vec<serde_json::Value>>;
    fn record_source_check(&self, target: &str, capture_ms: Option<i64>) -> Result<()>;
    fn scheduled_time(&self, job: &str, scheduled_ms: i64) -> Result<()>;
    fn register_cleanup(&self, job: &Job) -> Result<()>;
    fn clear_cleanup(&self, id: &str) -> Result<()>;
    fn pending_cleanups(&self) -> Result<Vec<Job>>;
    fn cleanup_pending(&self, target: &str) -> Result<bool>;
    fn finish_job(
        &self,
        job: &Job,
        status: &str,
        error: Option<&str>,
        snapshot: Option<&Snapshot>,
    ) -> Result<()>;
    fn job_status(&self, id: &str) -> Result<Option<crate::domain::JobStatus>>;
    fn progress(&self, id: &str, phase: &str) -> Result<()>;

    fn sync_schedules(&self, config: &Config, now: i64) -> Result<()>;
    fn due(&self, target: &str) -> Result<i64>;
    fn advance(&self, target: &str, due: i64) -> Result<()>;
    fn outstanding(&self, target: &str) -> Result<bool>;
    fn pending_count(&self) -> Result<usize>;
    fn enqueue(&self, spec: &JobSpec, now: i64) -> Result<String>;
    fn jobs(&self, only_due: Option<i64>) -> Result<Vec<Job>>;
    fn start(&self, id: &str) -> Result<()>;
    fn dispatch(&self, job: &Job) -> Result<()>;
    fn is_interrupted(&self, id: &str) -> Result<bool>;
    fn prepare(&self, snapshot: &Snapshot) -> Result<()>;
    fn intention(&self, id: &str) -> Result<Option<Snapshot>>;
    fn complete(&self, snapshot: &Snapshot) -> Result<()>;
    fn failed(&self, job: &Job, retry_at: Option<i64>, error: &str) -> Result<()>;
    fn catalog(&self) -> Result<Vec<(Snapshot, bool, bool)>>;
    fn mark_deleting(&self, id: &str) -> Result<()>;
    fn cancel_deletion(&self, id: &str) -> Result<()>;
    fn deleted(&self, id: &str) -> Result<()>;
    fn quarantine(&self, id: &str) -> Result<()>;
    fn status(&self, config: &Config) -> Result<serde_json::Value>;
    fn historical_targets(&self) -> Result<Vec<Target>>;
}

/// A synchronous public API is deliberate: the coordinator owns threading,
/// cancellation, and reservation lifetimes, while backends own their internals.
pub fn tool_memory(resources: &Resources) -> u64 {
    resources.memory_budget_bytes / resources.max_concurrent_snapshots as u64
}

pub struct HookRequest<'a> {
    pub hook: &'a crate::config::Hook,
    pub job: &'a Job,
    pub phase: &'a str,
    pub cancel: &'a AtomicBool,
}
pub trait ScriptRunner: Send + Sync {
    fn run(&self, request: HookRequest<'_>) -> Result<crate::domain::HookResult>;
}
pub trait EventSink: Send + Sync {
    fn emit(&self, event: &crate::domain::OperationEvent) -> Result<()>;
}
