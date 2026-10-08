use crate::{
    api::StateStore,
    archive,
    backends::Modules,
    config::{self, Config},
    domain::{Job, JobSpec, Snapshot},
    retention, snapshot,
    telemetry::{self, event},
};
use anyhow::{Context, Result, ensure};
use fs2::FileExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub struct Instance {
    pub state: Arc<dyn StateStore>,
    _lock: File,
}
impl Instance {
    pub fn open(config: &Config) -> Result<Self> {
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(&config.state_dir)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(config.state_dir.join("instance.lock"))?;
        lock.try_lock_exclusive()
            .context("another process is using this state directory")?;
        let state = crate::state::open(config)?;
        validate_history(config, state.as_ref())?;
        Ok(Self { state, _lock: lock })
    }
}

pub fn validate_history(config: &Config, state: &dyn StateStore) -> Result<()> {
    for historical in state.historical_targets()? {
        ensure!(
            !config::overlap(&historical.destination_dir, &config.state_dir)?,
            "state directory overlaps a historical backup destination"
        );
        for target in &config.targets {
            ensure!(
                !config::overlap(&historical.destination_dir, &target.source_dir)?,
                "source overlaps historical destination {}",
                historical.id
            );
            if historical.destination_dir != target.destination_dir {
                ensure!(
                    !config::overlap(&historical.destination_dir, &target.destination_dir)?,
                    "new destination overlaps a historical destination"
                );
            } else {
                ensure!(
                    historical.id == target.id,
                    "destination belongs to a different target ID"
                );
            }
        }
    }
    ensure!(
        state.pending_count()? <= config.queue.max_pending_jobs,
        "pending jobs exceed new queue capacity; use the previous capacity until they drain"
    );
    Ok(())
}

pub fn recover(instance: &Instance, config: &Config) -> Result<()> {
    for job in instance.state.jobs(None)? {
        if job.attempts == 0 || !instance.state.is_interrupted(&job.id)? {
            continue;
        }
        match reconcile(instance.state.as_ref(), &job, &config.state_dir) {
            Ok(true) => (),
            Ok(false) => {
                let error = anyhow::anyhow!("interrupted backup; retrying a fresh capture");
                instance.state.failed(
                    &job,
                    Modules::from_choices(&config.backends)?.queue.retry_at(
                        &error,
                        job.attempts,
                        &config.queue,
                        chrono::Utc::now().timestamp_millis(),
                    ),
                    &error.to_string(),
                )?;
            }
            Err(e) => {
                if instance.state.intention(&job.id)?.is_none() {
                    instance.state.failed(
                        &job,
                        Modules::from_choices(&config.backends)?.queue.retry_at(
                            &e,
                            job.attempts,
                            &config.queue,
                            chrono::Utc::now().timestamp_millis(),
                        ),
                        &format!("{e:#}"),
                    )?;
                }
                event(
                    "warn",
                    "recovery postponed",
                    json!({"target":job.spec.target.id,"job":job.id,"error":format!("{e:#}")}),
                );
            }
        }
    }
    Ok(())
}

fn reconcile(state: &dyn StateStore, job: &Job, state_dir: &Path) -> Result<bool> {
    let modules = Modules::from_choices(&job.spec.backends)?;
    let destination = modules.storage.open(&job.spec.target, true, state_dir)?;
    if let Some(snapshot) = state.intention(&job.id)?
        && destination.exists(&snapshot.filename)?
    {
        archive::verify_snapshot(
            destination.as_ref(),
            &snapshot,
            &job.spec.resources,
            &AtomicBool::new(false),
        )?;
        destination.synchronize()?;
        state.complete(&snapshot)?;
        event(
            "info",
            "interrupted publication recovered",
            json!({"target":snapshot.target_id,"file":snapshot.filename}),
        );
        return Ok(true);
    }
    destination.remove_temporary(&job.id)?;
    destination.remove_staging(&job.id)?;
    Ok(false)
}

struct Worker {
    job: Job,
    handle: JoinHandle<Result<Snapshot>>,
    cancel: Arc<AtomicBool>,
}

pub fn run(config_path: &Path, socket_path: &Path) -> Result<()> {
    let config_path = fs::canonicalize(config_path)?;
    let mut config = config::load(&config_path)?;
    crate::resources::verify_service_limit(&config.resources)?;
    telemetry::configure(&config.logging);
    let instance = Instance::open(&config)?;
    instance
        .state
        .sync_schedules(&config, chrono::Utc::now().timestamp_millis())?;
    recover(&instance, &config)?;
    ensure!(
        socket_path.is_absolute(),
        "control socket path must be absolute"
    );
    if let Some(parent) = socket_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if socket_path.exists() {
        ensure!(
            UnixStream::connect(socket_path).is_err(),
            "control socket already has a running service"
        );
        fs::remove_file(socket_path)?;
    }
    let listener = UnixListener::bind(socket_path)?;
    fs::set_permissions(socket_path, fs::Permissions::from_mode(0o600))?;
    listener.set_nonblocking(true)?;
    let _socket_guard = SocketGuard(socket_path.to_owned());
    let stop = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGTERM, Arc::clone(&stop))?;
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&stop))?;
    let mut workers: HashMap<String, Worker> = HashMap::new();
    let mut cleanup: Option<(JoinHandle<Result<()>>, Arc<AtomicBool>)> = None;
    let mut sweep_due = Instant::now();
    let mut recovery_due = Instant::now() + Duration::from_secs(config.queue.retry_initial_seconds);
    let mut rotation = 0usize;
    let mut shutdown_at = None;
    event(
        "info",
        "service started",
        json!({"version":env!("CARGO_PKG_VERSION"),"config":config_path,"socket":socket_path}),
    );
    loop {
        let stopping = stop.load(Ordering::Relaxed);
        if stopping && shutdown_at.is_none() {
            shutdown_at = Some(Instant::now());
            event("info", "shutdown started", json!({}));
        }
        if let Some(at) = shutdown_at
            && at.elapsed().as_secs() >= config.shutdown_grace_seconds
        {
            for worker in workers.values() {
                worker.cancel.store(true, Ordering::Relaxed);
            }
            if let Some((_, cancel)) = &cleanup {
                cancel.store(true, Ordering::Relaxed);
            }
        }
        let finished: Vec<_> = workers
            .iter()
            .filter(|(_, w)| w.handle.is_finished())
            .map(|(id, _)| id.clone())
            .collect();
        for id in finished {
            let worker = workers.remove(&id).expect("finished worker exists");
            let result = worker
                .handle
                .join()
                .unwrap_or_else(|_| Err(anyhow::anyhow!("backup worker panicked")));
            match result {
                Ok(snapshot) => event(
                    "info",
                    "snapshot committed",
                    json!({"target":snapshot.target_id,"file":snapshot.filename,"bytes":snapshot.bytes}),
                ),
                Err(error) => {
                    if instance.state.intention(&id)?.is_some() {
                        match reconcile(instance.state.as_ref(), &worker.job, &config.state_dir) {
                            Ok(true) => continue,
                            Err(e) => {
                                event(
                                    "error",
                                    "publication recovery deferred",
                                    json!({"job":id,"error":format!("{e:#}")}),
                                );
                                continue;
                            }
                            Ok(false) => (),
                        }
                    }
                    let retry = Modules::from_choices(&config.backends)?.queue.retry_at(
                        &error,
                        worker.job.attempts,
                        &config.queue,
                        chrono::Utc::now().timestamp_millis(),
                    );
                    instance
                        .state
                        .failed(&worker.job, retry, &format!("{error:#}"))?;
                    event(
                        "error",
                        "backup failed",
                        json!({"target":worker.job.spec.target.id,"job":id,"retry_at_ms":retry,"error":format!("{error:#}")}),
                    );
                }
            }
        }
        if cleanup.as_ref().is_some_and(|(h, _)| h.is_finished()) {
            let (handle, _) = cleanup.take().expect("cleanup exists");
            if let Err(e) = handle
                .join()
                .unwrap_or_else(|_| Err(anyhow::anyhow!("retention worker panicked")))
            {
                event(
                    "error",
                    "retention sweep failed",
                    json!({"error":format!("{e:#}")}),
                );
            }
        }
        if stopping && workers.is_empty() && cleanup.is_none() {
            break;
        }
        if !stopping {
            if Instant::now() >= recovery_due {
                for job in instance.state.jobs(None)? {
                    if workers.contains_key(&job.id) || !instance.state.is_interrupted(&job.id)? {
                        continue;
                    }
                    match reconcile(instance.state.as_ref(), &job, &config.state_dir) {
                        Ok(true) => (),
                        Ok(false) => {
                            let error =
                                anyhow::anyhow!("interrupted publication; retrying capture");
                            instance.state.failed(
                                &job,
                                Modules::from_choices(&config.backends)?.queue.retry_at(
                                    &error,
                                    job.attempts,
                                    &config.queue,
                                    chrono::Utc::now().timestamp_millis(),
                                ),
                                &error.to_string(),
                            )?;
                        }
                        Err(e) => event(
                            "error",
                            "publication remains deferred",
                            json!({"job":job.id,"error":format!("{e:#}")}),
                        ),
                    }
                }
                recovery_due =
                    Instant::now() + Duration::from_secs(config.queue.retry_initial_seconds);
            }
            if let Ok((mut stream, _)) = listener.accept() {
                stream.set_read_timeout(Some(Duration::from_secs(2)))?;
                stream.set_write_timeout(Some(Duration::from_secs(2)))?;
                let response = control(
                    &mut stream,
                    &config_path,
                    &mut config,
                    instance.state.as_ref(),
                    workers.len() + usize::from(cleanup.is_some()),
                );
                let value = match response {
                    Ok(v) => json!({"ok":true,"result":v}),
                    Err(e) => json!({"ok":false,"error":format!("{e:#}")}),
                };
                let _ = serde_json::to_writer(&mut stream, &value);
                let _ = stream.write_all(b"\n");
            }
            let modules = Modules::from_choices(&config.backends)?;
            let now = chrono::Utc::now().timestamp_millis();
            let count = config.targets.len();
            for offset in 0..count {
                let target = &config.targets[(rotation + offset) % count];
                if !target.enabled {
                    continue;
                }
                let due = instance.state.due(&target.id)?;
                if now < due {
                    continue;
                }
                if !modules.queue.admit(
                    instance.state.outstanding(&target.id)?,
                    instance.state.pending_count()?,
                    config.queue.max_pending_jobs,
                ) {
                    event(
                        "warn",
                        "scheduled trigger skipped; target busy or queue full",
                        json!({"target":target.id}),
                    );
                } else {
                    let spec = JobSpec {
                        backends: config.backends.clone(),
                        target: target.clone(),
                        resources: config.resources.clone(),
                    };
                    instance.state.enqueue(&spec, now)?;
                }
                instance.state.advance(
                    &target.id,
                    modules
                        .scheduler
                        .next_due(due, now, target.backup_interval_seconds),
                )?;
            }
            if count > 0 {
                rotation = (rotation + 1) % count;
            }
            let slots = config
                .resources
                .max_concurrent_snapshots
                .min(config.resources.max_worker_threads);
            if Instant::now() >= sweep_due && cleanup.is_none() && workers.len() < slots {
                let state = instance.state.clone();
                let settings = config.clone();
                let cancel = archive::never_cancel();
                let flag = Arc::clone(&cancel);
                cleanup = Some((
                    thread::spawn(move || retention::sweep(state.as_ref(), &settings, &flag)),
                    cancel,
                ));
                sweep_due =
                    Instant::now() + Duration::from_secs(config.retention.sweep_interval_seconds);
            }
            for mut job in instance.state.jobs(Some(now))? {
                if workers.len() + usize::from(cleanup.is_some()) >= slots {
                    break;
                }
                if !config
                    .targets
                    .iter()
                    .any(|t| t.enabled && t.id == job.spec.target.id)
                {
                    instance.state.failed(
                        &job,
                        None,
                        "target removed or disabled; queued request cancelled",
                    )?;
                    continue;
                }
                job.spec.resources = config.resources.clone();
                instance.state.dispatch(&job)?;
                job.attempts += 1;
                let worker_job = job.clone();
                let state = instance.state.clone();
                let state_dir = config.state_dir.clone();
                let cancel = archive::never_cancel();
                let flag = Arc::clone(&cancel);
                let handle = thread::spawn(move || {
                    snapshot::create(&worker_job, state.as_ref(), &state_dir, &flag)
                });
                workers.insert(
                    job.id.clone(),
                    Worker {
                        job,
                        handle,
                        cancel,
                    },
                );
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
    event("info", "service stopped", json!({}));
    Ok(())
}

fn control(
    stream: &mut UnixStream,
    path: &Path,
    config: &mut Config,
    state: &dyn StateStore,
    running: usize,
) -> Result<Value> {
    let mut request = String::new();
    stream.take(128).read_to_string(&mut request)?;
    match request.trim() {
        "status" => state.status(config),
        "reload" => {
            let candidate = config::load(path)?;
            ensure!(
                candidate.backends.state == config.backends.state,
                "state backend change requires migration/restart"
            );
            ensure!(
                candidate.state_dir == config.state_dir,
                "state_dir cannot be changed by reload; restart required"
            );
            ensure!(
                candidate.resources.memory_limit_bytes == config.resources.memory_limit_bytes,
                "memory_limit_bytes requires regenerating the systemd unit and restarting"
            );
            ensure!(
                running == 0 || candidate.resources == config.resources,
                "wait for active backups to finish before changing resource limits"
            );
            validate_history(&candidate, state)?;
            state.sync_schedules(&candidate, chrono::Utc::now().timestamp_millis())?;
            telemetry::configure(&candidate.logging);
            *config = candidate;
            event(
                "info",
                "configuration explicitly reloaded",
                json!({"config":path}),
            );
            Ok(
                json!({"message":"configuration reloaded; existing jobs and snapshots retain their target settings"}),
            )
        }
        _ => anyhow::bail!("unknown control command"),
    }
}

pub fn request(socket: &Path, command: &str) -> Result<Value> {
    let mut stream = UnixStream::connect(socket)
        .context("connect to service control socket; run as root or the service user")?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.write_all(command.as_bytes())?;
    stream.shutdown(std::net::Shutdown::Write)?;
    let response: Value = serde_json::from_reader(stream)?;
    ensure!(
        response["ok"] == true,
        "{}",
        response["error"]
            .as_str()
            .unwrap_or("control request failed")
    );
    Ok(response["result"].clone())
}
struct SocketGuard(PathBuf);
impl Drop for SocketGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self._lock);
    }
}
