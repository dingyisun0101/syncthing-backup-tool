//! Coordinator reservations, durable idle periods, and monotonic running waits.
use crate::{
    api::StateStore,
    config::{Config, Target},
    storage, telemetry,
};
use anyhow::Result;
use serde_json::json;
use std::{
    collections::HashMap,
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub fn filesystem(target: &Target) -> Result<String> {
    if let Some(mount) = &target.required_destination_mount {
        storage::mount_present(mount)?;
    }
    filesystem_path(&target.destination_dir)
}
pub fn filesystem_path(path: &Path) -> Result<String> {
    let mut existing = path;
    while !existing.exists() {
        existing = existing
            .parent()
            .ok_or_else(|| anyhow::anyhow!("no filesystem ancestor"))?;
    }
    let info = rustix::fs::fstatfs(File::open(existing)?)?;
    // The filesystem ID survives process restarts and block-device renumbering.
    Ok(format!("{}:{:?}", info.f_type, info.f_fsid))
}
struct Activity {
    busy: bool,
    finished: Instant,
    finished_ms: i64,
    failed: bool,
}
#[derive(Clone)]
pub struct Coordinator {
    state: Arc<dyn StateStore>,
    disks: Arc<Mutex<HashMap<String, Activity>>>,
}
impl Coordinator {
    pub fn try_acquire_all(&self, requests: &[(String, u64)]) -> Result<Option<Vec<Permit>>> {
        let mut disks = self
            .disks
            .lock()
            .map_err(|_| anyhow::anyhow!("I/O coordinator poisoned"))?;
        let mut seen = std::collections::HashSet::new();
        for (id, seconds) in requests {
            anyhow::ensure!(seen.insert(id), "duplicate filesystem reservation");
            if disks.get(id).is_some_and(|a| {
                a.busy || a.failed || a.finished.elapsed() < Duration::from_secs(*seconds)
            }) {
                return Ok(None);
            }
        }
        let mut permits = Vec::new();
        for (id, _) in requests {
            let now = chrono::Utc::now().timestamp_millis();
            if let Err(e) = self.state.set_io_activity(id, now, true) {
                drop(disks);
                return Err(e);
            }
            disks.insert(
                id.clone(),
                Activity {
                    busy: true,
                    finished: Instant::now(),
                    finished_ms: now,
                    failed: false,
                },
            );
            permits.push(Permit {
                coordinator: self.clone(),
                id: id.clone(),
            });
        }
        Ok(Some(permits))
    }
    pub fn new(state: Arc<dyn StateStore>) -> Result<Self> {
        let now = chrono::Utc::now().timestamp_millis();
        let clock = Instant::now();
        let mut disks = HashMap::new();
        for (id, previous, active) in state.io_activity()? {
            let finished_ms = if active { now } else { previous };
            if active {
                state.set_io_activity(&id, now, false)?;
            }
            let elapsed = now.saturating_sub(finished_ms).max(0) as u64;
            disks.insert(
                id,
                Activity {
                    busy: false,
                    finished: clock
                        .checked_sub(Duration::from_millis(elapsed))
                        .unwrap_or(clock),
                    finished_ms,
                    failed: false,
                },
            );
        }
        Ok(Self {
            state,
            disks: Arc::new(Mutex::new(disks)),
        })
    }
    pub fn delay(&self, id: &str, seconds: u64) -> Result<Option<(i64, &'static str)>> {
        let disks = self
            .disks
            .lock()
            .map_err(|_| anyhow::anyhow!("I/O coordinator poisoned"))?;
        Ok(disks.get(id).and_then(|a| {
            if a.busy || a.failed {
                Some((
                    i64::MAX,
                    if a.failed {
                        "activity persistence failed"
                    } else {
                        "filesystem operation active"
                    },
                ))
            } else {
                let remaining = Duration::from_secs(seconds).saturating_sub(a.finished.elapsed());
                (!remaining.is_zero()).then(|| {
                    (
                        chrono::Utc::now()
                            .timestamp_millis()
                            .saturating_add(remaining.as_millis().min(i64::MAX as u128) as i64),
                        "filesystem cooldown",
                    )
                })
            }
        }))
    }
    pub fn try_acquire(&self, id: &str, seconds: u64) -> Result<Option<Permit>> {
        let mut disks = self
            .disks
            .lock()
            .map_err(|_| anyhow::anyhow!("I/O coordinator poisoned"))?;
        if disks.get(id).is_some_and(|a| {
            a.busy || a.failed || a.finished.elapsed() < Duration::from_secs(seconds)
        }) {
            return Ok(None);
        }
        let now = chrono::Utc::now().timestamp_millis();
        self.state.set_io_activity(id, now, true)?;
        disks.insert(
            id.to_owned(),
            Activity {
                busy: true,
                finished: Instant::now(),
                finished_ms: now,
                failed: false,
            },
        );
        Ok(Some(Permit {
            coordinator: self.clone(),
            id: id.to_owned(),
        }))
    }
    pub fn wait(
        &self,
        id: &str,
        seconds: u64,
        cancel: &std::sync::atomic::AtomicBool,
    ) -> Result<Permit> {
        loop {
            crate::archive::check_cancel(cancel)?;
            if let Some(permit) = self.try_acquire(id, seconds)? {
                return Ok(permit);
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    }
    pub fn status(&self, config: &Config) -> serde_json::Value {
        let mut targets = Vec::new();
        for target in &config.targets {
            let value = (|| -> Result<_> {
                let id = filesystem(target)?;
                let seconds = cooldown(config, &id);
                let delay = self.delay(&id, seconds)?;
                let finished_ms = self
                    .disks
                    .lock()
                    .map_err(|_| anyhow::anyhow!("I/O coordinator poisoned"))?
                    .get(&id)
                    .map(|a| a.finished_ms);
                Ok(
                    json!({"target":target.id,"filesystem":id,"cooldown_seconds":seconds,"last_io_finished_ms":finished_ms,"next_eligible_ms":delay.filter(|(at,_)| *at != i64::MAX).map(|(at,_)|at),"delay_reason":delay.map(|(_,reason)|reason)}),
                )
            })();
            targets.push(
                value.unwrap_or_else(
                    |e| json!({"target":target.id,"delay_reason":format!("{e:#}")}),
                ),
            );
        }
        json!(targets)
    }
}
pub struct Permit {
    coordinator: Coordinator,
    id: String,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let now = chrono::Utc::now().timestamp_millis();
        let result = self.coordinator.state.set_io_activity(&self.id, now, false);
        if let Ok(mut disks) = self.coordinator.disks.lock() {
            disks.insert(
                self.id.clone(),
                Activity {
                    busy: false,
                    finished: Instant::now(),
                    finished_ms: now,
                    failed: result.is_err(),
                },
            );
        }
        if let Err(e) = result {
            telemetry::event(
                "error",
                "filesystem dispatch blocked; cooldown persistence failed",
                json!({"filesystem":self.id,"error":format!("{e:#}")}),
            );
        }
    }
}
pub fn cooldown(config: &Config, id: &str) -> u64 {
    config
        .targets
        .iter()
        .filter(|t| filesystem(t).is_ok_and(|fs| fs == id))
        .map(|t| {
            t.storage
                .cooldown_seconds
                .unwrap_or(config.io_cooldown_seconds)
        })
        .max()
        .unwrap_or(config.io_cooldown_seconds)
}
