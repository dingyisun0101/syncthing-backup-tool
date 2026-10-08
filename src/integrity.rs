//! Independent verification and persistent, controlled reread evidence.
use crate::{
    api::{DestinationSession, StateStore},
    archive,
    backends::Modules,
    config::{Config, Resources},
    domain::Snapshot,
    source, telemetry,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{fs::File, sync::atomic::AtomicBool};
fn read(
    destination: &dyn DestinationSession,
    snapshot: &Snapshot,
    resources: &Resources,
    cancel: &AtomicBool,
) -> Result<Value> {
    let file = destination.archive(&snapshot.filename)?;
    let before = source::fingerprint(&file.metadata()?);
    let actual = archive::digest(file, resources.io_buffer_bytes, cancel)?;
    let mut phase = "archive_digest";
    let mut result = if actual == snapshot.sha256 {
        Ok(())
    } else {
        Err(archive::Mismatch {
            phase: phase.into(),
            path: snapshot.filename.clone(),
            expected: snapshot.sha256.clone(),
            actual: actual.clone(),
        }
        .into())
    };
    if result.is_ok() {
        phase = "zip_crc_and_manifest";
        result = archive::verify(
            destination.archive(&snapshot.filename)?,
            resources,
            Some(&snapshot.job_id),
            Some(&snapshot.target),
            cancel,
        )
        .map(|_| ());
    }
    let after = source::fingerprint(&destination.archive(&snapshot.filename)?.metadata()?);
    let changed = before != after || before.size != snapshot.bytes;
    let mismatch = result
        .as_ref()
        .err()
        .and_then(|e| e.downcast_ref::<archive::Mismatch>())
        .map(|e| json!(e));
    let status = if changed {
        "metadata_changed"
    } else if result.is_ok() {
        "ok"
    } else {
        "stable_metadata_mismatch"
    };
    Ok(
        json!({"status":status,"phase":phase,"path":snapshot.target.destination_dir.join(&snapshot.filename),"expected_sha256":snapshot.sha256,"actual_sha256":actual,"metadata_before":before,"metadata_after":after,"metadata_changed":changed,"mismatch":mismatch,"error":result.err().map(|e|format!("{e:#}"))}),
    )
}
fn failed_read(error: &anyhow::Error) -> Value {
    let denied = error
        .chain()
        .filter_map(|e| e.downcast_ref::<std::io::Error>())
        .any(|e| e.kind() == std::io::ErrorKind::PermissionDenied);
    json!({"status":if denied{"permission_error"}else{"read_error"},"error":format!("{error:#}")})
}
pub fn evict(file: &File) -> Value {
    let sync = file.sync_all().err().map(|e| e.to_string());
    let hint = rustix::fs::fadvise(file, 0, None, rustix::fs::Advice::DontNeed)
        .err()
        .map(|e| e.to_string());
    json!({"sync_error":sync,"cache_hint_error":hint,"cache_eviction_advisory":true})
}
pub fn verify_checked(
    state: &dyn StateStore,
    destination: &dyn DestinationSession,
    snapshot: &Snapshot,
    resources: &Resources,
    reopened: bool,
    cancel: &AtomicBool,
) -> Result<Value> {
    let first = read(destination, snapshot, resources, cancel).unwrap_or_else(|e| failed_read(&e));
    archive::check_cancel(cancel)?;
    let mut report = json!({"id":uuid::Uuid::new_v4().to_string(),"timestamp_ms":chrono::Utc::now().timestamp_millis(),"job_id":snapshot.job_id,"target":snapshot.target_id,"method":if reopened{"synchronized_reopened_advisory_eviction"}else{"standard"},"first_read":first});
    if reopened || report["first_read"]["status"] != "ok" {
        report["reopen_preparation"] = destination
            .archive(&snapshot.filename)
            .map(|f| evict(&f))
            .unwrap_or_else(|e| failed_read(&e));
        report["controlled_reread"] =
            read(destination, snapshot, resources, cancel).unwrap_or_else(|e| failed_read(&e));
    }
    let healthy = report["first_read"]["status"] == "ok"
        && (!reopened || report["controlled_reread"]["status"] == "ok");
    report["healthy"] = healthy.into();
    state.record_inspection(
        if healthy {
            "verification"
        } else {
            "integrity_incident"
        },
        &report,
    )?;
    ensure!(
        healthy,
        "integrity verification failed; evidence {} persisted; archive preserved",
        report["id"]
    );
    Ok(report)
}
pub fn scrub(state: &dyn StateStore, config: &Config, cancel: &AtomicBool) -> Result<Value> {
    let mut records = Vec::new();
    let mut failed = 0usize;
    for (snapshot, _, _) in state.catalog()? {
        if !crate::planning::managed(config, &snapshot) {
            continue;
        }
        archive::check_cancel(cancel)?;
        let result = (|| -> Result<Value> {
            let modules = Modules::from_choices(&snapshot.backends)?;
            let destination = modules
                .storage
                .open(&snapshot.target, false, &config.state_dir)?;
            let _lock = destination.serialize_writes(&config.state_dir)?;
            verify_checked(
                state,
                destination.as_ref(),
                &snapshot,
                &config.resources,
                config.scrub.reopened_read,
                cancel,
            )
        })();
        match result {
            Ok(report) => records.push(report),
            Err(e) => {
                archive::check_cancel(cancel)?;
                failed += 1;
                records.push(json!({"job_id":snapshot.job_id,"target":snapshot.target_id,"error":format!("{e:#}")}));
                telemetry::event(
                    "error",
                    "scrub failed; archive preserved",
                    json!({"job_id":snapshot.job_id,"error":format!("{e:#}")}),
                );
            }
        }
    }
    let report = json!({"id":uuid::Uuid::new_v4().to_string(),"finished_ms":chrono::Utc::now().timestamp_millis(),"healthy":failed==0,"failed":failed,"records":records});
    state.record_inspection("scrub_summary", &report)?;
    Ok(report)
}

pub fn unresolved(state: &dyn StateStore) -> Result<Vec<Value>> {
    let resolutions = state.inspections("incident_resolution")?;
    Ok(state
        .inspections("integrity_incident")?
        .into_iter()
        .filter(|incident| {
            !resolutions
                .iter()
                .any(|resolution| resolution["incident_id"] == incident["id"])
        })
        .collect())
}
pub fn resolve(state: &dyn StateStore, id: &str, reason: &str) -> Result<Value> {
    ensure!(
        !reason.trim().is_empty() && reason.len() <= 4096,
        "a review reason of at most 4096 bytes is required"
    );
    let incident = state
        .inspections("integrity_incident")?
        .into_iter()
        .find(|r| r["id"] == id)
        .ok_or_else(|| anyhow::anyhow!("unknown integrity incident"))?;
    let report = json!({"id":uuid::Uuid::new_v4().to_string(),"incident_id":id,"target":incident["target"],"reason":reason,"resolved_ms":chrono::Utc::now().timestamp_millis(),"evidence_preserved":true});
    state.record_inspection("incident_resolution", &report)?;
    Ok(report)
}
