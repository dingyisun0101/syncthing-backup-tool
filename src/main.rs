use anyhow::{Result, ensure};
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use syncthing_backup_tool::{
    config::{self, DEFAULT_CONFIG, DEFAULT_SOCKET},
    daemon,
    domain::{Job, JobSpec},
    retention, telemetry,
};

#[derive(Parser)]
#[command(version, about = "Scheduled, verified ZIP directory backups for Linux")]
struct Cli {
    #[arg(long,global=true,default_value=DEFAULT_CONFIG)]
    config: PathBuf,
    #[arg(long,global=true,default_value=DEFAULT_SOCKET)]
    control_socket: PathBuf,
    #[command(subcommand)]
    command: Option<Command>,
}
#[derive(Subcommand)]
enum Command {
    /// Run continuously in the foreground (also the default command).
    Run,
    /// Validate JSON, policies, and path separation without creating files.
    Validate,
    /// Explicitly reload the running service's configured JSON file.
    Reload,
    /// Show the running service's loaded settings and backup status.
    Status,
    /// Print stable JSON for local monitoring; --check returns failure for degraded health.
    Report {
        #[arg(long)]
        check: bool,
    },
    /// Take one backup of each enabled target, or select a target.
    Backup {
        #[arg(long)]
        target: Option<String>,
    },
    /// Request immediate backups from the running service.
    Trigger {
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        wait: bool,
        #[arg(long, default_value_t = 0)]
        timeout_seconds: u64,
    },
    /// Inspect one queued or completed job.
    Job { id: String },
    /// Run one retention sweep using policies recorded in existing archives.
    Retain,
    /// Preview selection, exclusions, historical cohorts, and shared-disk capacity without writing files.
    Plan {
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        proposed_config: Option<PathBuf>,
    },
    /// Preview all policy cohorts and possible retention deletions without verification or mutation.
    RetentionPlan {
        #[arg(long)]
        target: Option<String>,
    },
    /// Verify retained archives independently; record incidents without retention or backup creation.
    Scrub {
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        reopened_read: bool,
    },
    /// Inspect and verify an independent archive.
    Inspect {
        #[arg(long)]
        archive: PathBuf,
    },
    /// Restore into an empty destination outside source, archive, and state directories.
    Restore {
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        destination: PathBuf,
        #[arg(long)]
        path: Vec<String>,
        #[arg(long)]
        safe_symlinks: bool,
    },
    /// Rehearse restoration in an isolated scratch directory; record success separately.
    Rehearse {
        #[arg(long)]
        target: Option<String>,
        #[arg(long)]
        scratch_dir: Option<PathBuf>,
        #[arg(long)]
        full: bool,
    },
    /// Save a reviewed old-cohort retirement plan with a mandatory rollback window.
    TransitionPlan {
        #[arg(long)]
        target: String,
        #[arg(long)]
        from_policy: String,
        #[arg(long)]
        rollback_window_seconds: u64,
        #[arg(long)]
        output: PathBuf,
    },
    /// Apply or resume exactly the saved transition plan (requires the daemon to be stopped).
    TransitionApply {
        #[arg(long)]
        plan: PathBuf,
    },
    /// Show recorded integrity incidents without changing the catalog.
    Incidents,
    /// Record an operator review of an incident; keep evidence and require fresh verification for cleanup.
    IncidentResolve {
        #[arg(long)]
        id: String,
        #[arg(long)]
        reason: String,
    },
    /// Print a service unit using this config's memory limit and path.
    Unit,
}

fn main() {
    if let Err(error) = execute() {
        eprintln!("error: {error:#}");
        std::process::exit(1);
    }
}
fn execute() -> Result<()> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Run) {
        Command::Run => daemon::run(&cli.config, &cli.control_socket),
        Command::Reload => {
            println!(
                "{}",
                serde_json::to_string_pretty(&daemon::request(&cli.control_socket, "reload")?)?
            );
            Ok(())
        }
        Command::Report { check } => {
            let value = daemon::request(&cli.control_socket, "status")?;
            let healthy = value["targets"].as_array().is_some_and(|targets| {
                targets
                    .iter()
                    .all(|t| t["health"] == "healthy" || t["health"] == "disabled")
            }) && value["last_scrub"]
                .as_object()
                .is_none_or(|r| r.get("healthy") == Some(&serde_json::json!(true)))
                && value["last_rehearsal"]
                    .as_object()
                    .is_none_or(|r| r.get("healthy") == Some(&serde_json::json!(true)));
            print_json(value)?;
            ensure!(!check || healthy, "backup health is degraded");
            Ok(())
        }
        Command::Status => {
            println!(
                "{}",
                serde_json::to_string_pretty(&daemon::request(&cli.control_socket, "status")?)?
            );
            Ok(())
        }
        Command::Trigger {
            target,
            wait,
            timeout_seconds,
        } => {
            let result = daemon::request(
                &cli.control_socket,
                &serde_json::json!({"command":"trigger","target":target}).to_string(),
            )?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            if wait {
                let started = std::time::Instant::now();
                for job in result["jobs"].as_array().expect("trigger returns jobs") {
                    loop {
                        let status = daemon::request(
                            &cli.control_socket,
                            &serde_json::json!({"command":"job","id":job["id"]}).to_string(),
                        )?;
                        let value: syncthing_backup_tool::domain::JobStatus =
                            serde_json::from_value(status.clone())?;
                        if value.terminal() {
                            println!("{}", serde_json::to_string_pretty(&status)?);
                            ensure!(
                                ["succeeded", "unchanged"].contains(&value.status.as_str()),
                                "backup {} finished as {}: {}",
                                value.target_id,
                                value.status,
                                value.error.unwrap_or_default()
                            );
                            break;
                        }
                        ensure!(
                            timeout_seconds == 0 || started.elapsed().as_secs() < timeout_seconds,
                            "timed out waiting; backup continues in service"
                        );
                        std::thread::sleep(std::time::Duration::from_secs(1));
                    }
                }
            }
            Ok(())
        }
        Command::Job { id } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&daemon::request(
                    &cli.control_socket,
                    &serde_json::json!({"command":"job","id":id}).to_string()
                )?)?
            );
            Ok(())
        }
        command => {
            let config = config::load(&cli.config)?;
            match command {
                Command::Plan {
                    target,
                    proposed_config,
                } => print_json(syncthing_backup_tool::planning::preview_file(
                    &config,
                    proposed_config.as_deref(),
                    target.as_deref(),
                )?),
                Command::RetentionPlan { target } => {
                    if let Some(id) = &target {
                        ensure!(
                            config.targets.iter().any(|t| &t.id == id),
                            "unknown target {id}"
                        );
                    }
                    let catalog = syncthing_backup_tool::planning::catalog(&config)?
                        .into_iter()
                        .filter(|(s, _, _)| target.as_ref().is_none_or(|id| id == &s.target_id))
                        .collect::<Vec<_>>();
                    print_json(syncthing_backup_tool::planning::retention_preview(
                        &config,
                        &catalog,
                        chrono::Utc::now().timestamp_millis(),
                    )?)
                }
                Command::IncidentResolve { id, reason } => {
                    let instance = daemon::Instance::open(&config)?;
                    print_json(syncthing_backup_tool::integrity::resolve(
                        instance.state.as_ref(),
                        &id,
                        &reason,
                    )?)
                }
                Command::Incidents => {
                    let reports =
                        syncthing_backup_tool::state::State::read_only(&config.state_dir)?
                            .map(|s| s.inspections("integrity_incident"))
                            .transpose()?
                            .unwrap_or_default();
                    print_json(serde_json::json!({"incidents":reports}))
                }
                Command::Scrub {
                    target,
                    reopened_read,
                } => {
                    let mut settings = config.clone();
                    settings.scrub.reopened_read |= reopened_read;
                    bulk(&settings, target.as_deref(), "scrub")
                }
                Command::Rehearse {
                    target,
                    scratch_dir,
                    full,
                } => {
                    let mut settings = config.clone();
                    if let Some(path) = scratch_dir {
                        settings.rehearsal.scratch_dir = Some(path);
                    }
                    if full {
                        settings.rehearsal.sample_files = None;
                    }
                    settings.validate()?;
                    bulk(&settings, target.as_deref(), "rehearsal")
                }
                Command::TransitionPlan {
                    target,
                    from_policy,
                    rollback_window_seconds,
                    output,
                } => {
                    syncthing_backup_tool::restore::isolated(&config, &output)?;
                    let plan = syncthing_backup_tool::migration::plan(
                        &config,
                        &target,
                        &from_policy,
                        rollback_window_seconds,
                    )?;
                    use std::io::Write;
                    use std::os::unix::fs::OpenOptionsExt;
                    let mut file = std::fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .mode(0o600)
                        .open(&output)?;
                    file.write_all(&serde_json::to_vec_pretty(&plan)?)?;
                    file.sync_all()?;
                    print_json(
                        serde_json::json!({"plan":output,"not_before_ms":plan.not_before_ms,"deletion_candidate_count":plan.deletion_candidates.len()}),
                    )
                }
                Command::TransitionApply { plan } => {
                    use std::io::Read;
                    let mut bytes = Vec::new();
                    std::fs::File::open(plan)?
                        .take(1024 * 1024 + 1)
                        .read_to_end(&mut bytes)?;
                    ensure!(bytes.len() <= 1024 * 1024, "transition plan exceeds 1 MiB");
                    let plan: syncthing_backup_tool::migration::Transition =
                        serde_json::from_slice(&bytes)?;
                    let instance = daemon::Instance::open(&config)?;
                    telemetry::configure(&config.logging)?;
                    let io =
                        syncthing_backup_tool::io_policy::Coordinator::new(instance.state.clone())?;
                    let target = config
                        .targets
                        .iter()
                        .find(|t| t.id == plan.target)
                        .ok_or_else(|| anyhow::anyhow!("unknown transition target"))?;
                    let filesystem = syncthing_backup_tool::io_policy::filesystem(target)?;
                    let _permit = io.wait(
                        &filesystem,
                        syncthing_backup_tool::io_policy::cooldown(&config, &filesystem),
                        &std::sync::atomic::AtomicBool::new(false),
                    )?;
                    print_json(syncthing_backup_tool::migration::apply(
                        instance.state.as_ref(),
                        &config,
                        &plan,
                        &std::sync::atomic::AtomicBool::new(false),
                    )?)
                }
                Command::Inspect { archive } => {
                    let instance = daemon::Instance::open(&config)?;
                    let io =
                        syncthing_backup_tool::io_policy::Coordinator::new(instance.state.clone())?;
                    let filesystem = syncthing_backup_tool::io_policy::filesystem_path(&archive)?;
                    let _permit = io.wait(
                        &filesystem,
                        syncthing_backup_tool::io_policy::cooldown(&config, &filesystem),
                        &std::sync::atomic::AtomicBool::new(false),
                    )?;
                    print_json(serde_json::to_value(
                        syncthing_backup_tool::restore::inspect(
                            &config,
                            &archive,
                            &std::sync::atomic::AtomicBool::new(false),
                        )?,
                    )?)
                }
                Command::Restore {
                    archive,
                    destination,
                    path,
                    safe_symlinks,
                } => {
                    let instance = daemon::Instance::open(&config)?;
                    let io =
                        syncthing_backup_tool::io_policy::Coordinator::new(instance.state.clone())?;
                    let source = syncthing_backup_tool::io_policy::filesystem_path(&archive)?;
                    let output = syncthing_backup_tool::io_policy::filesystem_path(&destination)?;
                    let _source = io.wait(
                        &source,
                        syncthing_backup_tool::io_policy::cooldown(&config, &source),
                        &std::sync::atomic::AtomicBool::new(false),
                    )?;
                    let _output = if source != output {
                        Some(io.wait(
                            &output,
                            syncthing_backup_tool::io_policy::cooldown(&config, &output),
                            &std::sync::atomic::AtomicBool::new(false),
                        )?)
                    } else {
                        None
                    };
                    print_json(syncthing_backup_tool::restore::extract(
                        &config,
                        &archive,
                        &destination,
                        &path,
                        None,
                        safe_symlinks,
                        &std::sync::atomic::AtomicBool::new(false),
                    )?)
                }
                Command::Validate => {
                    println!("Configuration valid ({} targets)", config.targets.len());
                    Ok(())
                }
                Command::Unit => {
                    let config_path = std::fs::canonicalize(&cli.config)?;
                    ensure!(
                        !config_path
                            .to_string_lossy()
                            .contains(['\n', '\r', '%', '"', '\\', ' ', '\t']),
                        "unsupported characters in unit config path"
                    );
                    let template = include_str!("../packaging/syncthing-backup-tool.service");
                    print!(
                        "{}",
                        template
                            .replace(
                                "/etc/syncthing-backup-tool/config.json",
                                &config_path.to_string_lossy()
                            )
                            .replace(
                                "MemoryMax=536870912",
                                &format!("MemoryMax={}", config.resources.memory_limit_bytes)
                            )
                            .replace(
                                "TimeoutStopSec=90",
                                &format!(
                                    "TimeoutStopSec={}",
                                    config.shutdown_grace_seconds
                                        + config
                                            .targets
                                            .iter()
                                            .map(|t| t
                                                .hooks
                                                .finally
                                                .iter()
                                                .map(|h| h.timeout_seconds)
                                                .sum::<u64>())
                                            .max()
                                            .unwrap_or(0)
                                        + 30
                                )
                            )
                    );
                    Ok(())
                }
                Command::Backup { target } => {
                    if let Some(id) = &target {
                        ensure!(
                            config.targets.iter().any(|t| t.enabled && &t.id == id),
                            "unknown or disabled target {id}"
                        );
                    }
                    let instance = daemon::Instance::open(&config)?;
                    telemetry::configure(&config.logging)?;
                    instance
                        .state
                        .sync_schedules(&config, chrono::Utc::now().timestamp_millis())?;
                    daemon::recover(&instance, &config)?;
                    let io =
                        syncthing_backup_tool::io_policy::Coordinator::new(instance.state.clone())?;
                    for t in config
                        .targets
                        .iter()
                        .filter(|t| t.enabled && target.as_ref().is_none_or(|id| id == &t.id))
                    {
                        ensure!(
                            !instance.state.outstanding(&t.id)?,
                            "target {} has an outstanding recovered job; run the service to drain it",
                            t.id
                        );
                        let spec = JobSpec {
                            backends: config.backends.clone(),
                            target: t.clone(),
                            resources: config.resources.clone(),
                        };
                        let id = instance
                            .state
                            .enqueue(&spec, chrono::Utc::now().timestamp_millis())?;
                        let filesystem = syncthing_backup_tool::io_policy::filesystem(t)?;
                        let _permit = io.wait(
                            &filesystem,
                            syncthing_backup_tool::io_policy::cooldown(&config, &filesystem),
                            &std::sync::atomic::AtomicBool::new(false),
                        )?;
                        instance.state.start(&id)?;
                        let job = Job {
                            id,
                            spec,
                            attempts: 1,
                        };
                        match syncthing_backup_tool::snapshot::create(
                            &job,
                            instance.state.as_ref(),
                            &config.state_dir,
                            &std::sync::atomic::AtomicBool::new(false),
                        ) {
                            Ok(snapshot) => {
                                println!("{}", t.destination_dir.join(snapshot.filename).display())
                            }
                            Err(e) => {
                                if instance
                                    .state
                                    .job_status(&job.id)?
                                    .is_none_or(|s| !s.terminal())
                                    && instance.state.intention(&job.id)?.is_none()
                                {
                                    instance.state.failed(&job, None, &format!("{e:#}"))?;
                                }
                                return Err(e);
                            }
                        }
                    }
                    Ok(())
                }
                Command::Retain => bulk(&config, None, "retention"),
                _ => unreachable!(),
            }
        }
    }
}

fn print_json(value: serde_json::Value) -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&config::redacted(value))?
    );
    Ok(())
}
fn bulk(config: &config::Config, target: Option<&str>, kind: &str) -> Result<()> {
    if let Some(id) = target {
        ensure!(
            config.targets.iter().any(|t| t.enabled && t.id == id),
            "unknown or disabled target"
        );
    }
    let instance = daemon::Instance::open(config)?;
    telemetry::configure(&config.logging)?;
    syncthing_backup_tool::hooks::recover(instance.state.as_ref())?;
    let io = syncthing_backup_tool::io_policy::Coordinator::new(instance.state.clone())?;
    let cancel = std::sync::atomic::AtomicBool::new(false);
    let mut disks = std::collections::BTreeMap::<String, config::Config>::new();
    for selected in config
        .targets
        .iter()
        .filter(|t| t.enabled && target.is_none_or(|id| id == t.id))
    {
        let filesystem = syncthing_backup_tool::io_policy::filesystem(selected)?;
        let settings = disks.entry(filesystem).or_insert_with(|| {
            let mut c = config.clone();
            c.targets.clear();
            c
        });
        settings.targets.push(selected.clone());
    }
    let mut failed = false;
    for (filesystem, settings) in disks {
        let _permit = io.wait(
            &filesystem,
            syncthing_backup_tool::io_policy::cooldown(config, &filesystem),
            &cancel,
        )?;
        let _scratch = if kind == "rehearsal" {
            let path = config
                .rehearsal
                .scratch_dir
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("scratch_dir is required"))?;
            let other = syncthing_backup_tool::io_policy::filesystem_path(path)?;
            if other != filesystem {
                Some(io.wait(
                    &other,
                    syncthing_backup_tool::io_policy::cooldown(config, &other),
                    &cancel,
                )?)
            } else {
                None
            }
        } else {
            None
        };
        match kind {
            "retention" => retention::sweep(instance.state.as_ref(), &settings, &cancel)?,
            "scrub" => {
                let report = syncthing_backup_tool::integrity::scrub(
                    instance.state.as_ref(),
                    &settings,
                    &cancel,
                )?;
                failed |= report["healthy"] != true;
                print_json(report)?;
            }
            "rehearsal" => {
                let report = syncthing_backup_tool::restore::rehearse(
                    instance.state.as_ref(),
                    &settings,
                    &cancel,
                )?;
                failed |= report["healthy"] != true;
                print_json(report)?;
            }
            _ => unreachable!(),
        }
    }
    ensure!(!failed, "inspection failed; recorded evidence is preserved");
    Ok(())
}
