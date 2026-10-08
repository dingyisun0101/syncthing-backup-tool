//! Bounded, cancellable supervision of established tools. No implicit shell.
use crate::telemetry;
use anyhow::{Context, Result, ensure};
use serde_json::json;
use std::{
    io::Read,
    os::unix::process::CommandExt,
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};

pub struct Output {
    pub code: Option<i32>,
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    pub cancelled: bool,
}
struct Group(Child);
impl Drop for Group {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            terminate(&mut self.0);
            let _ = self.0.wait();
        }
    }
}
fn terminate(child: &mut Child) {
    if let Some(pid) = rustix::process::Pid::from_raw(child.id() as i32) {
        let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
    }
    let _ = child.kill();
}
pub fn tool(program: &str, address_limit: u64, file_limit: u64) -> Command {
    let mut command = Command::new("/usr/bin/prlimit");
    command
        .args([
            format!("--as={address_limit}"),
            format!("--fsize={file_limit}"),
        ])
        .arg("--")
        .arg(program);
    command
        .env_remove("ZIPOPT")
        .env_remove("UNZIPOPT")
        .env("LC_ALL", "C.UTF-8");
    command
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}
fn capture(
    mut stream: impl Read + Send + 'static,
    label: &'static str,
    context: Option<(String, String)>,
) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let _context = telemetry::set_context(context);
        let mut tail = Vec::new();
        let mut buffer = [0u8; 4096];
        while let Ok(n) = stream.read(&mut buffer) {
            if n == 0 {
                break;
            }
            if tail.len() + n > 65536 {
                let remove = (tail.len() + n - 65536).min(tail.len());
                tail.drain(..remove);
            }
            tail.extend_from_slice(&buffer[..n]);
            let text = String::from_utf8_lossy(&buffer[..n]);
            let redacted =
                if text.to_lowercase().contains("password") || text.contains("MCRCON_PASS") {
                    "<credential-bearing output redacted>".to_owned()
                } else {
                    text.into_owned()
                };
            let _ = telemetry::audit(label, "output", json!({"text":redacted}));
        }
        String::from_utf8_lossy(&tail).into_owned()
    })
}
pub fn execute(
    command: &mut Command,
    cancel: &AtomicBool,
    mut check: impl FnMut() -> Result<()>,
    timeout: Option<Duration>,
) -> Result<Output> {
    let cleanup = command
        .get_envs()
        .any(|(key, value)| key == "BACKUP_PHASE" && value.is_some_and(|v| v == "finally"));
    let audit = telemetry::audit(
        "process.execute",
        "started",
        json!({"executable":command.get_program().to_string_lossy()}),
    );
    if !cleanup {
        audit?;
    }
    let mut group = Group(command.spawn().context("start backup tool or hook")?);
    let stdout = capture(
        group.0.stdout.take().context("capture stdout")?,
        "process.stdout",
        telemetry::current_context(),
    );
    let stderr = capture(
        group.0.stderr.take().context("capture stderr")?,
        "process.stderr",
        telemetry::current_context(),
    );
    let start = Instant::now();
    let mut failure = None;
    let mut timed_out = false;
    let mut cancelled = false;
    let status = loop {
        if let Some(status) = group.0.try_wait()? {
            break status;
        }
        cancelled = cancel.load(Ordering::Relaxed);
        timed_out = timeout.is_some_and(|t| start.elapsed() >= t);
        if cancelled || timed_out {
            terminate(&mut group.0);
            break group.0.wait()?;
        }
        if let Err(error) = check() {
            failure = Some(error);
            terminate(&mut group.0);
            break group.0.wait()?;
        }
        thread::sleep(Duration::from_millis(50));
    };
    let output = Output {
        code: status.code(),
        success: status.success(),
        stdout: stdout.join().unwrap_or_default(),
        stderr: stderr.join().unwrap_or_default(),
        timed_out,
        cancelled,
    };
    let audit = telemetry::audit(
        "process.execute",
        if output.success {
            "succeeded"
        } else {
            "failed"
        },
        json!({"exit_code":output.code,"timed_out":timed_out,"cancelled":cancelled,"duration_ms":start.elapsed().as_millis()}),
    );
    if !cleanup {
        audit?;
    }
    if let Some(error) = failure {
        return Err(error);
    }
    check()?;
    Ok(output)
}
pub fn run(
    command: &mut Command,
    cancel: &AtomicBool,
    check: impl FnMut() -> Result<()>,
) -> Result<()> {
    let result = execute(command, cancel, check, None)?;
    ensure!(
        result.success && !result.cancelled,
        "backup tool failed (exit {:?}): {}",
        result.code,
        result.stderr.trim()
    );
    Ok(())
}
