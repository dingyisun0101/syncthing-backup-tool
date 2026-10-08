use crate::{
    api::{HookRequest, ScriptRunner, StateStore},
    backends::Modules,
    config::Hook,
    domain::{HookResult, Job, Permanent, Skipped},
    telemetry,
};
use anyhow::{Result, ensure};
use serde_json::json;
use std::{
    sync::atomic::AtomicBool,
    time::{Duration, Instant},
};

pub struct LocalProcess;
impl ScriptRunner for LocalProcess {
    fn run(&self, request: HookRequest<'_>) -> Result<HookResult> {
        let start = Instant::now();
        let mut command = crate::process::tool(
            &request.hook.command[0],
            crate::api::tool_memory(&request.job.spec.resources),
            request.job.spec.target.storage.max_staging_bytes,
        );
        command.args(&request.hook.command[1..]);
        command
            .envs(&request.hook.environment)
            .env("BACKUP_JOB_ID", &request.job.id)
            .env("BACKUP_TARGET_ID", &request.job.spec.target.id)
            .env("BACKUP_SOURCE", &request.job.spec.target.source_dir)
            .env(
                "BACKUP_DESTINATION",
                &request.job.spec.target.destination_dir,
            )
            .env("BACKUP_PHASE", request.phase);
        let output = crate::process::execute(
            &mut command,
            request.cancel,
            || Ok(()),
            Some(Duration::from_secs(request.hook.timeout_seconds)),
        )?;
        let status = if output.timed_out {
            "timed_out"
        } else if output.cancelled {
            "cancelled"
        } else if output.success {
            "succeeded"
        } else {
            "failed"
        };
        Ok(HookResult {
            status: status.into(),
            exit_code: output.code,
            duration_ms: start.elapsed().as_millis() as u64,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}
pub fn run_phase(
    job: &Job,
    modules: &Modules,
    phase: &str,
    hooks: &[Hook],
    cancel: &AtomicBool,
) -> Result<()> {
    for hook in hooks {
        let details = json!({"name":hook.name,"phase":phase,"executable":hook.command[0]});
        if phase == "finally" {
            let _ = telemetry::audit("hook.execute", "started", details.clone());
        } else {
            telemetry::audit("hook.execute", "started", details.clone())?;
        }
        let result = modules.scripts.run(HookRequest {
            hook,
            job,
            phase,
            cancel,
        });
        let success = result.as_ref().is_ok_and(|r| r.status == "succeeded");
        let detail = json!({"hook":details,"result":result.as_ref().ok().map(|r|json!({"status":r.status,"exit_code":r.exit_code,"duration_ms":r.duration_ms})),"error":result.as_ref().err().map(|e|format!("{e:#}"))});
        if phase == "finally" {
            let _ = telemetry::audit(
                "hook.execute",
                if success { "succeeded" } else { "failed" },
                detail,
            );
        } else {
            telemetry::audit(
                "hook.execute",
                if success { "succeeded" } else { "failed" },
                detail,
            )?;
        }
        if success {
            continue;
        }
        let reason = format!(
            "hook {} ({phase}) {}",
            hook.name,
            result
                .as_ref()
                .map(|r| format!("{}; exit {:?}", r.status, r.exit_code))
                .unwrap_or_else(|e| format!("{e:#}"))
        );
        match hook.on_error.as_str() {
            "continue" => {
                telemetry::event("error", "optional hook failed", json!({"reason":reason}))
            }
            "skip_backup" => return Err(Skipped(reason).into()),
            "retry_backup" => anyhow::bail!("{reason}"),
            _ => return Err(Permanent(reason).into()),
        }
    }
    Ok(())
}
pub fn cleanup(job: &Job, state: &dyn StateStore, modules: &Modules) -> Result<()> {
    let _context = telemetry::context(job);
    let mut errors = Vec::new();
    for hook in &job.spec.target.hooks.finally {
        if let Err(error) = run_phase(
            job,
            modules,
            "finally",
            std::slice::from_ref(hook),
            &AtomicBool::new(false),
        ) {
            errors.push(format!("{error:#}"));
        }
    }
    ensure!(
        errors.is_empty(),
        "mandatory cleanup failed: {}",
        errors.join("; ")
    );
    state.clear_cleanup(&job.id)?;
    Ok(())
}
pub fn recover(state: &dyn StateStore) -> Result<()> {
    recover_except(state, &std::collections::HashSet::new())
}
pub fn recover_except(
    state: &dyn StateStore,
    active: &std::collections::HashSet<String>,
) -> Result<()> {
    for job in state.pending_cleanups()? {
        if active.contains(&job.id) {
            continue;
        }
        let modules = Modules::from_choices(&job.spec.backends)?;
        match cleanup(&job, state, &modules) {
            Ok(()) => telemetry::event(
                "info",
                "pending hook cleanup recovered",
                json!({"target":job.spec.target.id,"job":job.id}),
            ),
            Err(error) => telemetry::event(
                "error",
                "target blocked by pending hook cleanup",
                json!({"target":job.spec.target.id,"job":job.id,"error":format!("{error:#}")}),
            ),
        }
    }
    Ok(())
}
