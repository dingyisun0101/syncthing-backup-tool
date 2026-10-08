//! Reviewed administrative policy retirement; ZIP manifests remain immutable.
use crate::{
    api::StateStore,
    archive,
    backends::Modules,
    config::Config,
    domain::{JobSpec, Snapshot},
    integrity, planning,
};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{collections::HashSet, sync::atomic::AtomicBool};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transition {
    pub id: String,
    pub target: String,
    pub from_policy: String,
    pub to_policy: String,
    pub created_ms: i64,
    pub not_before_ms: i64,
    pub rollback_window_seconds: u64,
    pub replacements: Vec<String>,
    pub deletion_candidates: Vec<Snapshot>,
}
pub fn plan(config: &Config, target: &str, from: &str, window: u64) -> Result<Transition> {
    ensure!(
        (1..=31_536_000).contains(&window),
        "rollback window must be 1..31536000 seconds"
    );
    let target = config
        .targets
        .iter()
        .find(|t| t.enabled && t.id == target)
        .context("unknown or disabled target")?;
    let to = JobSpec {
        backends: config.backends.clone(),
        target: target.clone(),
        resources: config.resources.clone(),
    }
    .policy_id()?;
    ensure!(from != to, "cannot retire the current policy");
    let catalog = planning::catalog(config)?;
    let replacements = catalog
        .iter()
        .filter(|(s, h, _)| *h && s.policy_id == to && planning::managed(config, s))
        .map(|(s, _, _)| s.job_id.clone())
        .collect::<Vec<_>>();
    ensure!(
        replacements.len() >= target.retention.min_snapshots,
        "new policy has insufficient replacement captures"
    );
    let candidates = catalog
        .into_iter()
        .filter(|(s, h, _)| {
            *h && s.policy_id == from && s.target_id == target.id && planning::managed(config, s)
        })
        .map(|(s, _, _)| s)
        .collect::<Vec<_>>();
    ensure!(
        !candidates.is_empty(),
        "no managed healthy snapshots in the requested old cohort"
    );
    let now = chrono::Utc::now().timestamp_millis();
    Ok(Transition {
        id: uuid::Uuid::new_v4().to_string(),
        target: target.id.clone(),
        from_policy: from.into(),
        to_policy: to,
        created_ms: now,
        not_before_ms: now.saturating_add(window as i64 * 1000),
        rollback_window_seconds: window,
        replacements,
        deletion_candidates: candidates,
    })
}
pub fn apply(
    state: &dyn StateStore,
    config: &Config,
    plan: &Transition,
    cancel: &AtomicBool,
) -> Result<Value> {
    ensure!(
        uuid::Uuid::parse_str(&plan.id).is_ok(),
        "invalid transition ID"
    );
    ensure!(
        (1..=31_536_000).contains(&plan.rollback_window_seconds)
            && plan.not_before_ms
                == plan
                    .created_ms
                    .saturating_add(plan.rollback_window_seconds as i64 * 1000),
        "invalid rollback window"
    );
    ensure!(
        !integrity::unresolved(state)?
            .iter()
            .any(|incident| incident["target"] == plan.target),
        "unresolved integrity incident blocks policy retirement"
    );
    let target = config
        .targets
        .iter()
        .find(|t| t.enabled && t.id == plan.target)
        .context("transition target is not enabled")?;
    let current = JobSpec {
        backends: config.backends.clone(),
        target: target.clone(),
        resources: config.resources.clone(),
    }
    .policy_id()?;
    ensure!(
        current == plan.to_policy && current != plan.from_policy,
        "current policy differs from the reviewed transition"
    );
    ensure!(
        !state.outstanding(&plan.target)? && !state.cleanup_pending(&plan.target)?,
        "drain target jobs and mandatory cleanup before policy retirement"
    );
    let previous = state
        .inspections("retention_transition")?
        .into_iter()
        .find(|r| r["id"] == plan.id);
    if let Some(previous) = &previous {
        ensure!(
            previous["plan"] == json!(plan),
            "saved transition differs from the reviewed plan"
        );
        if previous["status"] == "completed" {
            return Ok(previous.clone());
        }
    }
    let mut record = previous
        .unwrap_or_else(|| json!({"id":plan.id,"plan":plan,"status":"protected","deleted":[]}));
    state.record_inspection("retention_transition", &record)?;
    if chrono::Utc::now().timestamp_millis() < plan.not_before_ms {
        record["message"]="old cohort remains protected through the rollback window; apply this same plan again afterward".into();
        state.record_inspection("retention_transition", &record)?;
        return Ok(record);
    }
    let catalog = state.catalog()?;
    let replacements = catalog
        .iter()
        .filter(|(s, h, _)| {
            *h && plan.replacements.contains(&s.job_id)
                && s.policy_id == current
                && planning::managed(config, s)
        })
        .map(|(s, _, _)| s.clone())
        .collect::<Vec<_>>();
    ensure!(
        replacements.len() >= target.retention.min_snapshots,
        "reviewed replacement captures are missing or unhealthy"
    );
    let modules = Modules::from_choices(&config.backends)?;
    let destination = modules.storage.open(target, false, &config.state_dir)?;
    let _lock = destination.serialize_writes(&config.state_dir)?;
    for replacement in &replacements {
        integrity::verify_checked(
            state,
            destination.as_ref(),
            replacement,
            &config.resources,
            config.scrub.reopened_read,
            cancel,
        )?;
    }
    let mut ids = HashSet::new();
    for candidate in &plan.deletion_candidates {
        ensure!(
            ids.insert(&candidate.job_id)
                && candidate.policy_id == plan.from_policy
                && candidate.target_id == target.id
                && planning::managed(config, candidate),
            "invalid transition candidate"
        );
        if record["deleted"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id == &candidate.job_id)
        {
            continue;
        }
        let live = catalog
            .iter()
            .find(|(s, _, _)| s.job_id == candidate.job_id)
            .context("reviewed candidate missing from catalog")?;
        ensure!(
            live.1 && serde_json::to_value(&live.0)? == serde_json::to_value(candidate)?,
            "reviewed candidate changed or unhealthy"
        );
        if !destination.exists(&candidate.filename)? && live.2 {
            continue;
        }
        integrity::verify_checked(
            state,
            destination.as_ref(),
            candidate,
            &config.resources,
            config.scrub.reopened_read,
            cancel,
        )?;
    }
    record["status"] = "deleting".into();
    state.record_inspection("retention_transition", &record)?;
    for candidate in &plan.deletion_candidates {
        archive::check_cancel(cancel)?;
        if record["deleted"]
            .as_array()
            .unwrap()
            .iter()
            .any(|id| id == &candidate.job_id)
        {
            continue;
        }
        state.mark_deleting(&candidate.job_id)?;
        if destination.exists(&candidate.filename)? {
            destination.remove(&candidate.filename)?;
        } else {
            destination.synchronize()?;
        }
        // Record progress before catalog removal so either interruption point resumes.
        record["deleted"]
            .as_array_mut()
            .unwrap()
            .push(json!(candidate.job_id));
        state.record_inspection("retention_transition", &record)?;
        state.deleted(&candidate.job_id)?;
    }
    for id in record["deleted"].as_array().unwrap() {
        if let Some(id) = id.as_str() {
            state.deleted(id)?;
        }
    }
    record["status"] = "completed".into();
    record["finished_ms"] = chrono::Utc::now().timestamp_millis().into();
    state.record_inspection("retention_transition", &record)?;
    Ok(record)
}
