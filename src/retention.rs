use crate::{
    api::StateStore,
    archive,
    backends::Modules,
    config::{Config, RetentionConfig},
    domain::Snapshot,
    telemetry::event,
};
use anyhow::Result;
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, Ordering},
};

pub fn plan(mut snapshots: Vec<Snapshot>, policy: &RetentionConfig, now: i64) -> Vec<Snapshot> {
    snapshots.sort_by(|a, b| (a.capture_ms, &a.job_id).cmp(&(b.capture_ms, &b.job_id)));
    let mut bytes = snapshots.iter().map(|s| s.bytes as u128).sum::<u128>();
    let mut remaining = snapshots.len();
    let mut result = Vec::new();
    for snapshot in snapshots {
        if remaining <= policy.min_snapshots {
            break;
        }
        let old = policy.max_age_seconds.is_some_and(|age| {
            now.saturating_sub(snapshot.capture_ms).max(0) as u128 > age as u128 * 1000
        });
        if remaining <= policy.max_snapshots
            && !old
            && policy
                .max_total_bytes
                .is_none_or(|maximum| bytes <= maximum as u128)
        {
            break;
        }
        bytes -= snapshot.bytes as u128;
        remaining -= 1;
        result.push(snapshot);
    }
    result
}

pub fn sweep(state: &dyn StateStore, config: &Config, cancel: &AtomicBool) -> Result<()> {
    config.validate()?;
    let incidents = crate::integrity::unresolved(state)?;
    let mut groups: BTreeMap<String, Vec<Snapshot>> = BTreeMap::new();
    for (snapshot, healthy, deleting) in state.catalog()? {
        let managed = config.targets.iter().any(|t| {
            t.enabled
                && t.id == snapshot.target_id
                && t.source_dir == snapshot.target.source_dir
                && t.destination_dir == snapshot.target.destination_dir
        });
        if !managed
            || incidents
                .iter()
                .any(|incident| incident["target"] == snapshot.target_id)
        {
            continue;
        }
        let modules = Modules::from_choices(&snapshot.backends)?;
        if deleting {
            match modules
                .storage
                .open(&snapshot.target, false, &config.state_dir)
            {
                Ok(destination) => {
                    if !destination.exists(&snapshot.filename)? {
                        destination.synchronize()?;
                        state.deleted(&snapshot.job_id)?;
                        continue;
                    } else {
                        // Replan against freshly verified survivors after a crash;
                        // an old intention does not bypass minimum protection.
                        state.cancel_deletion(&snapshot.job_id)?;
                    }
                }
                Err(e) => {
                    event(
                        "debug",
                        "retention postponed",
                        json!({"target":snapshot.target_id,"error":format!("{e:#}")}),
                    );
                    continue;
                }
            }
        }
        if healthy {
            groups
                .entry(snapshot.policy_id.clone())
                .or_default()
                .push(snapshot);
        }
    }
    for snapshots in groups.into_values() {
        if cancel.load(Ordering::Relaxed) {
            break;
        }
        let modules = Modules::from_choices(&snapshots[0].backends)?;
        let target = &snapshots[0].target;
        let policy = target.retention.clone();
        if modules
            .retention
            .plan(
                snapshots.clone(),
                &policy,
                chrono::Utc::now().timestamp_millis(),
            )
            .is_empty()
        {
            continue;
        }
        let destination = match modules.storage.open(target, false, &config.state_dir) {
            Ok(d) => d,
            Err(e) => {
                event(
                    "debug",
                    "retention postponed",
                    json!({"target":target.id,"error":format!("{e:#}")}),
                );
                continue;
            }
        };
        let mut healthy = Vec::new();
        let _filesystem_lock = destination.serialize_writes(&config.state_dir)?;
        let mut incident = false;
        for snapshot in snapshots {
            archive::check_cancel(cancel)?;
            match crate::integrity::verify_checked(
                state,
                destination.as_ref(),
                &snapshot,
                &config.resources,
                config.scrub.reopened_read,
                cancel,
            ) {
                Ok(_) => healthy.push(snapshot),
                Err(e) => {
                    if cancel.load(Ordering::Relaxed) {
                        return Err(e);
                    }
                    state.quarantine(&snapshot.job_id)?;
                    incident = true;
                    event(
                        "error",
                        "snapshot quarantined; file preserved",
                        json!({"target":snapshot.target_id,"file":snapshot.filename,"error":format!("{e:#}")}),
                    );
                }
            }
        }
        if incident {
            continue;
        }
        for snapshot in
            modules
                .retention
                .plan(healthy, &policy, chrono::Utc::now().timestamp_millis())
        {
            archive::check_cancel(cancel)?;
            state.mark_deleting(&snapshot.job_id)?;
            match destination.remove(&snapshot.filename) {
                Ok(()) => {
                    state.deleted(&snapshot.job_id)?;
                    event(
                        "info",
                        "snapshot retired",
                        json!({"target":snapshot.target_id,"file":snapshot.filename,"bytes":snapshot.bytes}),
                    );
                }
                Err(e) => {
                    event(
                        "error",
                        "snapshot deletion deferred",
                        json!({"target":snapshot.target_id,"error":format!("{e:#}")}),
                    );
                    break;
                }
            }
        }
    }
    Ok(())
}

pub struct OldestFirst;
impl crate::api::RetentionPolicy for OldestFirst {
    fn plan(&self, snapshots: Vec<Snapshot>, policy: &RetentionConfig, now: i64) -> Vec<Snapshot> {
        plan(snapshots, policy, now)
    }
}
