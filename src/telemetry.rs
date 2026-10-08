use crate::{
    api::EventSink,
    config::Logging,
    domain::{Job, OperationEvent},
};
use anyhow::{Context, Result};
use serde_json::{Value, json};
use std::{
    cell::RefCell,
    fs::{self, File, OpenOptions},
    io::Write,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock, RwLock,
        atomic::{AtomicU64, Ordering},
    },
};

static SETTINGS: OnceLock<RwLock<Logging>> = OnceLock::new();
static SINK: OnceLock<RwLock<Option<Arc<dyn EventSink>>>> = OnceLock::new();
static SEQUENCE: AtomicU64 = AtomicU64::new(0);
thread_local! {static CONTEXT:RefCell<Option<(String,String)>>=const {RefCell::new(None)};}
pub struct ContextGuard(Option<(String, String)>);
impl Drop for ContextGuard {
    fn drop(&mut self) {
        CONTEXT.with(|c| *c.borrow_mut() = self.0.take());
    }
}
pub fn context(job: &Job) -> ContextGuard {
    CONTEXT.with(|c| ContextGuard(c.replace(Some((job.id.clone(), job.spec.target.id.clone())))))
}
pub fn current_context() -> Option<(String, String)> {
    CONTEXT.with(|c| c.borrow().clone())
}
pub fn set_context(value: Option<(String, String)>) -> ContextGuard {
    CONTEXT.with(|c| ContextGuard(c.replace(value)))
}

struct JsonLines {
    path: PathBuf,
    maximum: u64,
    files: usize,
    writer: Mutex<File>,
}
impl EventSink for JsonLines {
    fn emit(&self, event: &OperationEvent) -> Result<()> {
        let mut file = self
            .writer
            .lock()
            .map_err(|_| anyhow::anyhow!("audit writer poisoned"))?;
        let bytes = serde_json::to_vec(event)?;
        if file.metadata()?.len() + bytes.len() as u64 + 1 > self.maximum {
            file.sync_data()?;
            for index in (1..self.files).rev() {
                let from = PathBuf::from(format!("{}.{}", self.path.display(), index));
                let to = PathBuf::from(format!("{}.{}", self.path.display(), index + 1));
                if from.exists() {
                    fs::rename(from, to)?;
                }
            }
            fs::rename(
                &self.path,
                PathBuf::from(format!("{}.1", self.path.display())),
            )?;
            *file = private_file(&self.path)?;
        }
        file.write_all(&bytes)?;
        file.write_all(b"\n")?;
        if event.operation.starts_with("hook.")
            || matches!(
                event.operation.as_str(),
                "backup" | "archive.publish" | "retention.delete" | "config.reload"
            )
        {
            file.sync_data()?;
        }
        Ok(())
    }
}
fn private_file(path: &std::path::Path) -> Result<File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?)
}
pub struct PreparedLogging {
    settings: Logging,
    sink: Option<Arc<dyn EventSink>>,
}
pub fn prepare(settings: &Logging) -> Result<PreparedLogging> {
    let sink = if let Some(path) = &settings.audit_file {
        if let Some(parent) = path.parent() {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(parent)?;
        }
        Some(Arc::new(JsonLines {
            path: path.clone(),
            maximum: settings.max_file_bytes,
            files: settings.max_files,
            writer: Mutex::new(private_file(path).context("open audit log")?),
        }) as Arc<dyn EventSink>)
    } else {
        None
    };
    Ok(PreparedLogging {
        settings: settings.clone(),
        sink,
    })
}
pub fn activate(prepared: PreparedLogging) -> Result<()> {
    let mut sink = SINK
        .get_or_init(|| RwLock::new(None))
        .write()
        .map_err(|_| anyhow::anyhow!("audit settings poisoned"))?;
    let mut settings = SETTINGS
        .get_or_init(|| RwLock::new(prepared.settings.clone()))
        .write()
        .map_err(|_| anyhow::anyhow!("logging settings poisoned"))?;
    *sink = prepared.sink;
    *settings = prepared.settings;
    Ok(())
}
pub fn configure(settings: &Logging) -> Result<()> {
    activate(prepare(settings)?)
}
fn record(operation: &str, outcome: &str, details: Value) -> OperationEvent {
    let context = current_context();
    OperationEvent {
        timestamp: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
        sequence: SEQUENCE.fetch_add(1, Ordering::Relaxed),
        job_id: context.as_ref().map(|c| c.0.clone()),
        target_id: context.map(|c| c.1),
        operation: operation.into(),
        outcome: outcome.into(),
        details,
    }
}
fn emit(event: &OperationEvent) -> Result<()> {
    if let Some(sink) = SINK
        .get()
        .and_then(|s| s.read().ok())
        .and_then(|s| s.clone())
    {
        sink.emit(event)?;
    } else {
        eprintln!("{}", serde_json::to_string(event)?);
    }
    Ok(())
}
pub fn audit(operation: &str, outcome: &str, details: Value) -> Result<()> {
    emit(&record(operation, outcome, details))
}
/// Control requests must remain available to repair a failed logging destination.
pub fn audit_or_stderr(operation: &str, outcome: &str, details: Value) {
    let event = record(operation, outcome, details);
    if let Err(error) = emit(&event) {
        eprintln!(
            "{}",
            serde_json::to_string(&event).expect("serialize operation event")
        );
        eprintln!(
            "{}",
            serde_json::json!({"timestamp":chrono::Utc::now().to_rfc3339(),"operation":"audit.write","outcome":"failed","error":error.to_string()})
        );
    }
}
pub fn operation<T>(name: &str, details: Value, action: impl FnOnce() -> Result<T>) -> Result<T> {
    audit(name, "started", details.clone())?;
    let start = std::time::Instant::now();
    let result = action();
    let end = json!({"context":details,"duration_ms":start.elapsed().as_millis(),"error":result.as_ref().err().map(|e|format!("{e:#}"))});
    // Cleanup actions themselves must still run when the audit filesystem is unavailable.
    audit(
        name,
        if result.is_ok() {
            "succeeded"
        } else {
            "failed"
        },
        end,
    )?;
    result
}
fn priority(level: &str) -> u8 {
    match level {
        "debug" => 0,
        "info" => 1,
        "warn" => 2,
        _ => 3,
    }
}
pub fn event(level: &str, message: &str, details: Value) {
    if SINK
        .get()
        .and_then(|s| s.read().ok())
        .is_some_and(|s| s.is_some())
        && let Err(error) = audit(message, level, details.clone())
    {
        eprintln!(
            "{}",
            json!({"timestamp":chrono::Utc::now().to_rfc3339(),"level":"error","operation":"audit.write","error":error.to_string()})
        );
    }
    let settings = SETTINGS.get().and_then(|l| l.read().ok());
    if settings
        .as_ref()
        .is_some_and(|s| priority(level) < priority(&s.level))
    {
        return;
    }
    let at = chrono::Utc::now().to_rfc3339();
    if settings.as_ref().is_some_and(|s| s.format == "text") {
        eprintln!("{at} {level} {message} {details}");
    } else {
        eprintln!(
            "{}",
            json!({"time":at,"level":level,"message":message,"details":details})
        );
    }
}
