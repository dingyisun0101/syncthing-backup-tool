//! Read-only selection, cohort retention, and aggregate capacity reports.
use crate::{
    config::{Config, Target},
    domain::{JobSpec, Snapshot},
    io_policy, retention, source,
    state::State,
};
use anyhow::{Result, ensure};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    fs::File,
    io::Read,
    path::Path,
};

pub fn managed(config: &Config, snapshot: &Snapshot) -> bool {
    config.targets.iter().any(|t| {
        t.enabled
            && t.id == snapshot.target_id
            && t.source_dir == snapshot.target.source_dir
            && t.destination_dir == snapshot.target.destination_dir
    })
}
pub fn catalog(config: &Config) -> Result<Vec<(Snapshot, bool, bool)>> {
    match State::read_only(&config.state_dir)? {
        Some(state) => state.catalog(),
        None => Ok(Vec::new()),
    }
}
pub fn retention_preview(
    config: &Config,
    catalog: &[(Snapshot, bool, bool)],
    now: i64,
) -> Result<Value> {
    let mut groups: BTreeMap<String, Vec<&(Snapshot, bool, bool)>> = BTreeMap::new();
    for item in catalog {
        groups
            .entry(item.0.policy_id.clone())
            .or_default()
            .push(item);
    }
    let mut cohorts = Vec::new();
    for (id, items) in groups {
        let sample = &items[0].0;
        let is_managed = managed(config, sample);
        let healthy = items
            .iter()
            .filter(|(_, h, _)| *h)
            .map(|(s, _, _)| s.clone())
            .collect::<Vec<_>>();
        let candidates = if is_managed {
            retention::plan(healthy.clone(), &sample.target.retention, now)
        } else {
            Vec::new()
        };
        let deleting: HashSet<_> = candidates.iter().map(|s| &s.job_id).collect();
        let current = config
            .targets
            .iter()
            .find(|t| t.id == sample.target_id)
            .map(|t| {
                JobSpec {
                    backends: config.backends.clone(),
                    target: t.clone(),
                    resources: config.resources.clone(),
                }
                .policy_id()
            })
            .transpose()?;
        cohorts.push(json!({"policy_id":id,"target":sample.target_id,"destination":sample.target.destination_dir,"managed":is_managed,"current_policy":current.as_deref()==Some(&id),"policy":sample.target.retention,"snapshot_count":items.len(),"healthy_count":healthy.len(),"total_bytes":items.iter().map(|(s,_,_)|s.bytes as u128).sum::<u128>().min(u64::MAX as u128) as u64,"protected_healthy":healthy.iter().filter(|s|!deleting.contains(&s.job_id)).map(|s|json!({"job_id":s.job_id,"file":s.filename,"bytes":s.bytes})).collect::<Vec<_>>(),"deletion_candidates":candidates,"corrupt_or_unknown_health":items.iter().filter(|(_,h,_)|!*h).map(|(s,_,_)|json!({"job_id":s.job_id,"file":s.filename})).collect::<Vec<_>>()}));
    }
    let mut destinations = std::collections::BTreeSet::new();
    destinations.extend(config.targets.iter().map(|t| t.destination_dir.clone()));
    destinations.extend(
        catalog
            .iter()
            .map(|(s, _, _)| s.target.destination_dir.clone()),
    );
    let mut unindexed = Vec::new();
    for destination in destinations {
        let known = catalog
            .iter()
            .filter(|(s, _, _)| s.target.destination_dir == destination)
            .map(|(s, _, _)| s.filename.as_str())
            .collect::<HashSet<_>>();
        let Ok(entries) = std::fs::read_dir(&destination) else {
            continue;
        };
        for entry in entries {
            if unindexed.len() >= 1000 {
                break;
            }
            let Ok(entry) = entry else {
                continue;
            };
            let name = entry.file_name();
            if name == ".snapshot-owner.json"
                || name == ".partial"
                || name.to_str().is_some_and(|n| known.contains(n))
            {
                continue;
            }
            unindexed.push(json!({"path":entry.path(),"owned":false,"bytes":std::fs::symlink_metadata(entry.path()).ok().filter(|m|m.is_file()).map(|m|m.len())}));
        }
    }
    Ok(
        json!({"cohorts":cohorts,"unindexed_entries":unindexed,"unindexed_listing_limit":1000,"total_bytes":catalog.iter().map(|(s,_,_)|s.bytes as u128).sum::<u128>().min(u64::MAX as u128) as u64,"requires_fresh_verification_before_deletion":true}),
    )
}
fn historical_bytes(snapshot: &Snapshot, config: &Config) -> Option<u64> {
    if let Some(bytes) = snapshot.selected_bytes {
        return Some(bytes);
    }
    let file = File::open(snapshot.target.destination_dir.join(&snapshot.filename)).ok()?;
    let mut zip = zip::ZipArchive::new(file).ok()?;
    let limit = crate::resources::MemoryBudget::new(&config.resources).manifest_limit();
    let mut bytes = Vec::new();
    zip.by_name("meta/manifest.json")
        .ok()?
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .ok()?;
    if bytes.len() as u64 > limit {
        return None;
    }
    let manifest: crate::domain::Manifest = serde_json::from_slice(&bytes).ok()?;
    Some(
        manifest
            .entries
            .iter()
            .filter(|e| e.kind == "file")
            .map(|e| e.metadata.size)
            .sum(),
    )
}
fn selection(config: &Config, target: &Target) -> Value {
    match source::Source::open(target).and_then(|s| source::inspect(&s, target, &config.resources))
    {
        Ok(report) => json!(report),
        Err(e) => json!({"errors":[format!("{e:#}")],"selected_bytes":null}),
    }
}
pub fn plan(config: &Config, proposed: Option<&Config>, selected: Option<&str>) -> Result<Value> {
    if let Some(id) = selected {
        ensure!(
            proposed
                .unwrap_or(config)
                .targets
                .iter()
                .any(|t| t.id == id),
            "unknown target {id}"
        );
    }
    let catalog = catalog(config)?;
    let mut selections = Vec::new();
    let mut errors = Vec::new();
    let mut disks: BTreeMap<String, Value> = BTreeMap::new();
    let effective = proposed.unwrap_or(config);
    for target in &effective.targets {
        let report = selection(effective, target);
        if report["errors"]
            .as_array()
            .is_some_and(|errors| !errors.is_empty())
        {
            errors.push(json!({"target":target.id,"selection_errors":report["errors"]}));
        }
        let current = if proposed.is_some() {
            config
                .targets
                .iter()
                .find(|t| t.id == target.id)
                .map(|t| selection(config, t))
        } else {
            None
        };
        let selected_bytes = report["selected_bytes"].as_u64();
        let id = io_policy::filesystem(target);
        let mut capacity_error = None;
        if let Ok(id) = id {
            let disk=disks.entry(id.clone()).or_insert_with(||json!({"filesystem":id,"targets":[],"retained_forecast_bytes":0,"retained_upper_estimate_bytes":0,"historical_cohort_bytes":0,"next_zip_bytes":0,"selected_staging_bytes":0,"reserve_bytes":0,"incomplete":false,"observed_ratio_samples":0}));
            disk["targets"]
                .as_array_mut()
                .unwrap()
                .push(json!(target.id));
            if target.enabled {
                let source = selected_bytes.unwrap_or(0);
                let mut ratios = Vec::new();
                for (s, healthy, _) in &catalog {
                    if *healthy
                        && s.target_id == target.id
                        && s.target.source_dir == target.source_dir
                        && s.target.destination_dir == target.destination_dir
                        && s.target.exclude_globs == target.exclude_globs
                        && s.target.cache.approved_paths == target.cache.approved_paths
                        && let Some(bytes) =
                            historical_bytes(s, effective).filter(|bytes| *bytes > 0)
                    {
                        ratios.push(s.bytes as f64 / bytes as f64);
                    }
                }
                let ratio = if ratios.is_empty() {
                    1.0
                } else {
                    ratios.iter().sum::<f64>() / ratios.len() as f64
                };
                let next = ((source as f64 * ratio) as u64).saturating_add(1024 * 1024);
                let metadata = report["entries"]
                    .as_array()
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|e| {
                                1024u64
                                    + e["path"].as_str().map(|p| p.len() as u64 * 8).unwrap_or(0)
                            })
                            .fold(2 * 1024 * 1024, u64::saturating_add)
                    })
                    .unwrap_or(16 * 1024 * 1024);
                let upper = source.saturating_add(source / 50).saturating_add(metadata);
                let policy = JobSpec {
                    backends: effective.backends.clone(),
                    target: target.clone(),
                    resources: effective.resources.clone(),
                }
                .policy_id()?;
                let current_bytes = catalog
                    .iter()
                    .filter(|(s, _, _)| s.policy_id == policy)
                    .map(|(s, _, _)| s.bytes as u128)
                    .sum::<u128>()
                    .min(u64::MAX as u128) as u64;
                for (key, amount) in [
                    (
                        "retained_forecast_bytes",
                        next.saturating_mul(target.retention.max_snapshots as u64)
                            .max(current_bytes),
                    ),
                    (
                        "retained_upper_estimate_bytes",
                        upper
                            .saturating_mul(target.retention.max_snapshots as u64)
                            .max(current_bytes),
                    ),
                    ("observed_ratio_samples", ratios.len() as u64),
                ] {
                    disk[key] = disk[key].as_u64().unwrap().saturating_add(amount).into();
                }
                disk["next_zip_bytes"] = disk["next_zip_bytes"].as_u64().unwrap().max(upper).into();
                disk["selected_staging_bytes"] = disk["selected_staging_bytes"]
                    .as_u64()
                    .unwrap()
                    .max(source)
                    .into();
                disk["reserve_bytes"] = disk["reserve_bytes"]
                    .as_u64()
                    .unwrap()
                    .max(target.storage.min_free_bytes)
                    .into();
                if selected_bytes.is_none()
                    || report["errors"].as_array().is_some_and(|e| !e.is_empty())
                {
                    disk["incomplete"] = true.into();
                }
            }
        } else {
            capacity_error = id.err().map(|e| format!("{e:#}"));
            errors.push(json!({"target":target.id,"filesystem_error":capacity_error}));
        }
        if selected.is_none_or(|id| id == target.id) {
            selections.push(json!({"target":target.id,"enabled":target.enabled,"selection":report,"loaded_selection":current,"capacity_error":capacity_error}));
        }
    }
    let current_ids = effective
        .targets
        .iter()
        .map(|t| {
            JobSpec {
                backends: effective.backends.clone(),
                target: t.clone(),
                resources: effective.resources.clone(),
            }
            .policy_id()
        })
        .collect::<Result<HashSet<_>>>()?;
    for (s, _, _) in &catalog {
        let Ok(id) = io_policy::filesystem(&s.target) else {
            continue;
        };
        let disk=disks.entry(id.clone()).or_insert_with(||json!({"filesystem":id,"targets":[],"retained_forecast_bytes":0,"retained_upper_estimate_bytes":0,"historical_cohort_bytes":0,"next_zip_bytes":0,"selected_staging_bytes":0,"reserve_bytes":0,"incomplete":false,"observed_ratio_samples":0}));
        if !current_ids.contains(&s.policy_id) {
            disk["historical_cohort_bytes"] = disk["historical_cohort_bytes"]
                .as_u64()
                .unwrap()
                .saturating_add(s.bytes)
                .into();
        }
    }
    for (id, disk) in &mut disks {
        let target = effective
            .targets
            .iter()
            .find(|t| io_policy::filesystem(t).is_ok_and(|f| &f == id));
        let available = target.and_then(|t| {
            let mut p = t.destination_dir.as_path();
            while !p.exists() {
                p = p.parent()?;
            }
            fs2::available_space(p).ok()
        });
        let current = catalog
            .iter()
            .filter(|(s, _, _)| io_policy::filesystem(&s.target).is_ok_and(|f| &f == id))
            .map(|(s, _, _)| {
                std::fs::symlink_metadata(s.target.destination_dir.join(&s.filename))
                    .ok()
                    .filter(|m| m.is_file())
                    .map(|m| m.len().min(s.bytes))
                    .unwrap_or(0)
            })
            .fold(0u64, u64::saturating_add);
        let peak = disk["historical_cohort_bytes"]
            .as_u64()
            .unwrap()
            .saturating_add(disk["retained_upper_estimate_bytes"].as_u64().unwrap())
            .saturating_add(disk["selected_staging_bytes"].as_u64().unwrap())
            .saturating_add(disk["next_zip_bytes"].as_u64().unwrap())
            .saturating_add(disk["reserve_bytes"].as_u64().unwrap());
        disk["peak_required_bytes"] = peak.into();
        disk["available_bytes"] = json!(available);
        disk["current_catalog_bytes"] = current.into();
        disk["affordable"] =
            json!(available.map(|free| !disk["incomplete"].as_bool().unwrap()
                && peak.saturating_sub(current) <= free));
    }
    Ok(
        json!({"read_only":true,"complete":errors.is_empty(),"errors":errors,"targets":selections,"filesystems":disks.into_values().collect::<Vec<_>>(),"retention":retention_preview(effective,&catalog,chrono::Utc::now().timestamp_millis())?,"uncertainty":"Historical measurements may not represent changed selection or compression. Upper estimates allow ZIP overhead; retention limits can be exceeded by protected minima and preserved incidents. One bulk operation per filesystem is assumed."}),
    )
}
pub fn preview_file(
    config: &Config,
    proposed: Option<&Path>,
    target: Option<&str>,
) -> Result<Value> {
    let proposed = proposed.map(crate::config::load).transpose()?;
    plan(config, proposed.as_ref(), target)
}
