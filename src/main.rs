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
                                value.status == "succeeded",
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
            if !matches!(command, Command::Validate | Command::Unit) {
                telemetry::configure(&config.logging)?;
            }

            match command {
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
                    instance
                        .state
                        .sync_schedules(&config, chrono::Utc::now().timestamp_millis())?;
                    daemon::recover(&instance, &config)?;
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
                Command::Retain => {
                    let instance = daemon::Instance::open(&config)?;
                    daemon::recover(&instance, &config)?;
                    retention::sweep(
                        instance.state.as_ref(),
                        &config,
                        &std::sync::atomic::AtomicBool::new(false),
                    )
                }
                _ => unreachable!(),
            }
        }
    }
}
