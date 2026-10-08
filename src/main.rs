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
        command => {
            let config = config::load(&cli.config)?;
            telemetry::configure(&config.logging);
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
                                &format!("TimeoutStopSec={}", config.shutdown_grace_seconds + 30)
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
                                if instance.state.intention(&job.id)?.is_none() {
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
