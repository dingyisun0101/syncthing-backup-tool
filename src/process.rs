//! Supervise established command-line tools without invoking a shell.
use anyhow::{Context, Result, ensure};
use std::{
    io::Read,
    os::unix::process::CommandExt,
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::Duration,
};

pub fn tool(program: &str, address_limit: u64, file_limit: u64) -> Command {
    let mut command = Command::new("/usr/bin/prlimit");
    command.args([
        format!("--as={address_limit}"),
        format!("--fsize={file_limit}"),
    ]);
    command.arg("--").arg(program);
    command
        .env_remove("ZIPOPT")
        .env_remove("UNZIPOPT")
        .env("LC_ALL", "C.UTF-8");
    command
        .process_group(0)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

pub fn run(
    command: &mut Command,
    cancel: &AtomicBool,
    mut check: impl FnMut() -> Result<()>,
) -> Result<()> {
    let mut child = command
        .spawn()
        .context("start backup tool (install rsync, zip, unzip, and util-linux)")?;
    let mut stderr = child.stderr.take().context("capture tool diagnostics")?;
    let reader = thread::spawn(move || {
        let mut collected = Vec::new();
        let mut buffer = [0u8; 4096];
        while let Ok(n) = stderr.read(&mut buffer) {
            if n == 0 {
                break;
            }
            let keep = n.min(65536usize.saturating_sub(collected.len()));
            collected.extend_from_slice(&buffer[..keep]);
        }
        String::from_utf8_lossy(&collected).into_owned()
    });
    let mut failure = None;
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        let result = if cancel.load(Ordering::Relaxed) {
            Err(anyhow::anyhow!("backup cancelled"))
        } else {
            check()
        };
        if let Err(error) = result {
            failure = Some(error);
            if let Some(pid) = rustix::process::Pid::from_raw(child.id() as i32) {
                let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
            }
            let _ = child.kill();
            break child.wait()?;
        }
        thread::sleep(Duration::from_millis(50));
    };
    let diagnostics = reader
        .join()
        .unwrap_or_else(|_| "diagnostic reader failed".into());
    if let Some(error) = failure {
        return Err(error);
    }
    ensure!(
        status.success(),
        "backup tool exited with {status}: {}",
        diagnostics.trim()
    );
    check()?;
    Ok(())
}
