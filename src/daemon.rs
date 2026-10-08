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
    let audit_paths = config.audit_paths();
    for historical in state.historical_targets()? {
        for path in &audit_paths {
            ensure!(
                !config::overlap(path, &historical.source_dir)?
                    && !config::overlap(path, &historical.destination_dir)?,
                "audit path overlaps historical or in-flight target data"
            );
        }
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
    let io = crate::io_policy::Coordinator::new(instance.state.clone())?;
    recover_coordinated(instance, config, &io)
}
fn recover_coordinated(
    instance: &Instance,
    config: &Config,
    io: &crate::io_policy::Coordinator,
) -> Result<()> {
    crate::hooks::recover(instance.state.as_ref())?;
    for job in instance.state.jobs(None)? {
        if job.attempts == 0 || !instance.state.is_interrupted(&job.id)? {
            continue;
        }
        if instance.state.cleanup_pending(&job.spec.target.id)? {
            continue;
        }
        let filesystem = match crate::io_policy::filesystem(&job.spec.target) {
            Ok(id) => id,
            Err(_) => continue,
        };
        let Some(_permit) =
            io.try_acquire(&filesystem, crate::io_policy::cooldown(config, &filesystem))?
        else {
            continue;
        };
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
        crate::integrity::verify_checked(
            state,
            destination.as_ref(),
            &snapshot,
            &job.spec.resources,
            false,
            &AtomicBool::new(false),
        )?;
        destination.synchronize()?;
        state.complete(&snapshot)?;
        let _context = telemetry::context(job);
        match crate::hooks::run_phase(
            job,
            &modules,
            "after_backup",
            &job.spec.target.hooks.after_backup,
            &AtomicBool::new(false),
        ) {
            Ok(()) => state.finish_job(job, "succeeded", None, Some(&snapshot))?,
            Err(error) => state.finish_job(
                job,
                "completed_with_hook_failure",
                Some(&format!("{error:#}")),
                Some(&snapshot),
            )?,
        }

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
    let instance = Instance::open(&config)?;
    telemetry::configure(&config.logging)?;
    instance
        .state
        .sync_schedules(&config, chrono::Utc::now().timestamp_millis())?;
    let io = crate::io_policy::Coordinator::new(instance.state.clone())?;
    recover_coordinated(&instance, &config, &io)?;
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
    let mut maintenance_due = maintenance_deadlines(instance.state.as_ref(), &config)?;
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
                    if instance
                        .state
                        .job_status(&id)?
                        .is_some_and(|s| s.status == "unchanged")
                    {
                        "source verified unchanged"
                    } else {
                        "snapshot committed"
                    },
                    json!({"target":snapshot.target_id,"job":id,"file":snapshot.filename,"bytes":snapshot.bytes,"capture_ms":snapshot.capture_ms}),
                ),
                Err(error) => {
                    if instance
                        .state
                        .job_status(&id)?
                        .is_some_and(|s| s.terminal())
                    {
                        event(
                            "error",
                            "backup attempt finished without success",
                            json!({"job":id,"error":format!("{error:#}")}),
                        );
                        continue;
                    }
                    if instance.state.intention(&id)?.is_some() {
                        event(
                            "warn",
                            "publication recovery queued behind filesystem idle policy",
                            json!({"job":id}),
                        );
                        continue;
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
        let maintenance_finished = cleanup.as_ref().is_some_and(|(h, _)| h.is_finished());
        if maintenance_finished {
            let (handle, _) = cleanup.take().expect("cleanup exists");
            if let Err(e) = handle
                .join()
                .unwrap_or_else(|_| Err(anyhow::anyhow!("retention worker panicked")))
            {
                event(
                    "error",
                    "maintenance operation failed",
                    json!({"error":format!("{e:#}")}),
                );
            }
        }
        if maintenance_finished {
            maintenance_due = maintenance_deadlines(instance.state.as_ref(), &config)?;
        }
        if stopping && workers.is_empty() && cleanup.is_none() {
            break;
        }
        if !stopping {
            if Instant::now() >= recovery_due {
                crate::hooks::recover_except(
                    instance.state.as_ref(),
                    &workers.keys().cloned().collect(),
                )?;
                for job in instance.state.jobs(None)? {
                    if workers.contains_key(&job.id)
                        || instance.state.cleanup_pending(&job.spec.target.id)?
                        || !instance.state.is_interrupted(&job.id)?
                    {
                        continue;
                    }
                    let filesystem = match crate::io_policy::filesystem(&job.spec.target) {
                        Ok(id) => id,
                        Err(_) => continue,
                    };
                    let Some(_permit) = io.try_acquire(
                        &filesystem,
                        crate::io_policy::cooldown(&config, &filesystem),
                    )?
                    else {
                        continue;
                    };
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
                telemetry::audit_or_stderr("control.request", "started", json!({}));
                let previous_intervals = (
                    config.retention.sweep_interval_seconds,
                    config.scrub.interval_seconds,
                    config.rehearsal.interval_seconds,
                );
                let response = control(
                    &mut stream,
                    &config_path,
                    &mut config,
                    instance.state.as_ref(),
                    workers.len() + usize::from(cleanup.is_some()),
                    &io,
                );
                if previous_intervals
                    != (
                        config.retention.sweep_interval_seconds,
                        config.scrub.interval_seconds,
                        config.rehearsal.interval_seconds,
                    )
                {
                    maintenance_due = maintenance_deadlines(instance.state.as_ref(), &config)?;
                }
                telemetry::audit_or_stderr(
                    "control.request",
                    if response.is_ok() {
                        "succeeded"
                    } else {
                        "failed"
                    },
                    json!({"error":response.as_ref().err().map(|e|format!("{e:#}"))}),
                );
                let value = match response {
                    Ok(v) => json!({"ok":true,"result":config::redacted(v)}),
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
                if !target.enabled
                    || target.manual_only
                    || instance.state.cleanup_pending(&target.id)?
                {
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
                    let id = instance.state.enqueue(&spec, now)?;
                    instance.state.scheduled_time(&id, due)?;
                }
                instance
                    .state
                    .advance(&target.id, modules.scheduler.next_for(target, due, now)?)?;
            }
            if count > 0 {
                rotation = (rotation + 1) % count;
            }
            let slots = config
                .resources
                .max_concurrent_snapshots
                .min(config.resources.max_worker_threads);
            for mut job in instance.state.jobs(Some(now))? {
                if workers.len() + usize::from(cleanup.is_some()) >= slots {
                    break;
                }
                if instance.state.cleanup_pending(&job.spec.target.id)? {
                    continue;
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
                let filesystem = match crate::io_policy::filesystem(&job.spec.target) {
                    Ok(id) => id,
                    Err(e) => {
                        event(
                            "debug",
                            "destination unavailable; dispatch delayed",
                            json!({"job":job.id,"error":format!("{e:#}")}),
                        );
                        continue;
                    }
                };
                let seconds = crate::io_policy::cooldown(&config, &filesystem).max(
                    job.spec
                        .target
                        .storage
                        .cooldown_seconds
                        .unwrap_or(config.io_cooldown_seconds),
                );
                let Some(permit) = io.try_acquire(&filesystem, seconds)? else {
                    continue;
                };
                job.spec.resources = config.resources.clone();
                instance.state.dispatch(&job)?;
                job.attempts += 1;
                let worker_job = job.clone();
                let state = instance.state.clone();
                let state_dir = config.state_dir.clone();
                let cancel = archive::never_cancel();
                let flag = Arc::clone(&cancel);
                let handle = thread::spawn(move || {
                    let _permit = permit;
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
            if cleanup.is_none() && workers.len() < slots {
                let mut disks = std::collections::BTreeMap::<String, Config>::new();
                for target in config.targets.iter().filter(|t| t.enabled) {
                    let Ok(filesystem) = crate::io_policy::filesystem(target) else {
                        continue;
                    };
                    let disk = disks.entry(filesystem).or_insert_with(|| {
                        let mut c = config.clone();
                        c.targets.clear();
                        c
                    });
                    disk.targets.push(target.clone());
                }
                'dispatch_maintenance: for (filesystem, settings) in disks {
                    for (kind, interval) in [
                        ("retention", Some(config.retention.sweep_interval_seconds)),
                        ("scrub", config.scrub.interval_seconds),
                        ("rehearsal", config.rehearsal.interval_seconds),
                    ] {
                        let Some(interval) = interval else {
                            continue;
                        };
                        let key = (filesystem.clone(), kind.to_owned());
                        if kind != "retention" && !maintenance_due.contains_key(&key) {
                            let next = chrono::Utc::now()
                                .timestamp_millis()
                                .saturating_add(interval as i64 * 1000);
                            instance.state.record_inspection("maintenance",&json!({"id":format!("{kind}:{filesystem}"),"kind":kind,"filesystem":filesystem,"finished_ms":chrono::Utc::now().timestamp_millis(),"next_due_ms":next,"status":"waiting_first_run"}))?;
                            maintenance_due.insert(
                                key.clone(),
                                Instant::now() + Duration::from_secs(interval),
                            );
                        }
                        if maintenance_due
                            .get(&key)
                            .is_some_and(|due| Instant::now() < *due)
                        {
                            continue;
                        }
                        if kind == "retention" {
                            let catalog = instance.state.catalog()?;
                            let preview =
                                crate::planning::retention_preview(&settings, &catalog, now)?;
                            if !catalog
                                .iter()
                                .any(|(s, _, d)| *d && crate::planning::managed(&settings, s))
                                && !preview["cohorts"].as_array().unwrap().iter().any(|c| {
                                    !c["deletion_candidates"].as_array().unwrap().is_empty()
                                })
                            {
                                maintenance_due
                                    .insert(key, Instant::now() + Duration::from_secs(interval));
                                continue;
                            }
                        }
                        let mut reservations = vec![(
                            filesystem.clone(),
                            crate::io_policy::cooldown(&config, &filesystem),
                        )];
                        if kind == "rehearsal"
                            && let Some(scratch) = &config.rehearsal.scratch_dir
                        {
                            let other = crate::io_policy::filesystem_path(scratch)?;
                            if other != filesystem {
                                reservations.push((
                                    other.clone(),
                                    crate::io_policy::cooldown(&config, &other),
                                ));
                            }
                        }
                        let Some(permits) = io.try_acquire_all(&reservations)? else {
                            continue;
                        };
                        let state = instance.state.clone();
                        let flag = archive::never_cancel();
                        let cancel = flag.clone();
                        let settings = settings.clone();
                        let filesystem = filesystem.clone();
                        cleanup = Some((
                            thread::spawn(move || {
                                let _permits = permits;
                                let result = match kind {
                                    "retention" => {
                                        retention::sweep(state.as_ref(), &settings, &flag)
                                    }
                                    "scrub" => {
                                        crate::integrity::scrub(state.as_ref(), &settings, &flag)
                                            .and_then(|r| {
                                                ensure!(
                                                    r["healthy"] == true,
                                                    "scrub detected failures"
                                                );
                                                Ok(())
                                            })
                                    }
                                    "rehearsal" => {
                                        crate::restore::rehearse(state.as_ref(), &settings, &flag)
                                            .and_then(|r| {
                                                ensure!(
                                                    r["healthy"] == true,
                                                    "rehearsal detected failures"
                                                );
                                                Ok(())
                                            })
                                    }
                                    _ => unreachable!(),
                                };
                                state.record_inspection("maintenance",&json!({"id":format!("{kind}:{filesystem}"),"kind":kind,"filesystem":filesystem,"finished_ms":chrono::Utc::now().timestamp_millis(),"next_due_ms":chrono::Utc::now().timestamp_millis().saturating_add(interval as i64*1000),"status":"finished","healthy":result.is_ok(),"error":result.as_ref().err().map(|e|format!("{e:#}"))}))?;
                                result
                            }),
                            cancel,
                        ));
                        maintenance_due.insert(key, Instant::now() + Duration::from_secs(interval));
                        break 'dispatch_maintenance;
                    }
                }
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
    io: &crate::io_policy::Coordinator,
) -> Result<Value> {
    let mut request = String::new();
    stream.take(4096).read_to_string(&mut request)?;
    let command = serde_json::from_str::<Value>(&request)
        .ok()
        .and_then(|v| v["command"].as_str().map(str::to_owned))
        .unwrap_or_else(|| request.trim().to_owned());
    let command = if ["status", "reload", "trigger", "job"].contains(&command.as_str()) {
        command.as_str()
    } else {
        "unknown"
    };
    telemetry::audit_or_stderr("control.command", "received", json!({"command":command}));
    if request.trim_start().starts_with('{') {
        let input: Value = serde_json::from_str(&request)?;
        match input["command"].as_str() {
            Some("trigger") => {
                let selected = input["target"].as_str();
                let targets: Vec<_> = config
                    .targets
                    .iter()
                    .filter(|t| t.enabled && selected.is_none_or(|id| id == t.id))
                    .collect();
                ensure!(!targets.is_empty(), "unknown or disabled target");
                let mut jobs = Vec::new();
                for target in targets {
                    ensure!(
                        !state.cleanup_pending(&target.id)?,
                        "target {} has pending mandatory cleanup",
                        target.id
                    );
                    ensure!(
                        !state.outstanding(&target.id)?,
                        "target {} already has an outstanding backup",
                        target.id
                    );
                }
                ensure!(
                    state.pending_count()? + targets_count(config, selected)
                        <= config.queue.max_pending_jobs,
                    "backup queue is full"
                );
                for target in config
                    .targets
                    .iter()
                    .filter(|t| t.enabled && selected.is_none_or(|id| id == t.id))
                {
                    let spec = JobSpec {
                        backends: config.backends.clone(),
                        target: target.clone(),
                        resources: config.resources.clone(),
                    };
                    let id = state.enqueue(&spec, chrono::Utc::now().timestamp_millis())?;
                    telemetry::audit(
                        "queue.enqueue",
                        "requested",
                        json!({"target":target.id,"job":id,"origin":"immediate_command"}),
                    )?;
                    jobs.push(json!({"id":id,"target":target.id}));
                }
                return Ok(json!({"jobs":jobs}));
            }
            Some("job") => {
                let id = input["id"].as_str().context("missing job ID")?;
                return Ok(serde_json::to_value(
                    state.job_status(id)?.context("unknown job ID")?,
                )?);
            }
            _ => anyhow::bail!("unknown control command"),
        }
    }

    match request.trim() {
        "status" => {
            let mut result = state.status(config)?;
            result["io"] = io.status(config);
            Ok(result)
        }
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
            let logging = telemetry::prepare(&candidate.logging)?;
            let now = chrono::Utc::now().timestamp_millis();
            state.sync_schedules(&candidate, now)?;

            telemetry::activate(logging)?;
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

fn targets_count(config: &Config, target: Option<&str>) -> usize {
    config
        .targets
        .iter()
        .filter(|t| t.enabled && target.is_none_or(|id| id == t.id))
        .count()
}

fn maintenance_deadlines(
    state: &dyn StateStore,
    config: &Config,
) -> Result<HashMap<(String, String), Instant>> {
    let mut deadlines = HashMap::new();
    for record in state.inspections("maintenance")? {
        if let (Some(filesystem), Some(kind), Some(finished)) = (
            record["filesystem"].as_str(),
            record["kind"].as_str(),
            record["finished_ms"].as_i64(),
        ) {
            let interval = match kind {
                "retention" => Some(config.retention.sweep_interval_seconds),
                "scrub" => config.scrub.interval_seconds,
                "rehearsal" => config.rehearsal.interval_seconds,
                _ => None,
            };
            if let Some(interval) = interval {
                let remaining = finished
                    .saturating_add(interval as i64 * 1000)
                    .saturating_sub(chrono::Utc::now().timestamp_millis())
                    .max(0) as u64;
                deadlines.insert(
                    (filesystem.into(), kind.into()),
                    Instant::now() + Duration::from_millis(remaining),
                );
            }
        }
    }
    Ok(deadlines)
}
