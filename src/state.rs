use crate::{
    config::{Config, Target},
    domain::{Job, JobSpec, Snapshot},
};
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone)]
pub struct State(Arc<Mutex<Connection>>);

impl State {
    pub fn scheduled_time(&self, job: &str, scheduled_ms: i64) -> Result<()> {
        let db = self.db()?;
        let info: String =
            db.query_row("SELECT info FROM job_timings WHERE id=?1", [job], |r| {
                r.get(0)
            })?;
        let mut metrics: serde_json::Value = serde_json::from_str(&info)?;
        metrics["scheduled_ms"] = scheduled_ms.into();
        db.execute(
            "UPDATE job_timings SET info=?2 WHERE id=?1",
            params![job, metrics.to_string()],
        )?;
        Ok(())
    }
    pub fn read_only(directory: &Path) -> Result<Option<Self>> {
        let path = directory.join("state.sqlite3");
        if !path.exists() {
            return Ok(None);
        }
        let db = Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        db.execute_batch("PRAGMA query_only=ON;")?;
        Ok(Some(Self(Arc::new(Mutex::new(db)))))
    }
    pub fn io_activity(&self) -> Result<Vec<(String, i64, bool)>> {
        let db = self.db()?;
        let mut statement = db.prepare("SELECT filesystem,finished_ms,active FROM io_activity")?;
        Ok(statement
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<_>>()?)
    }
    pub fn set_io_activity(&self, filesystem: &str, finished_ms: i64, active: bool) -> Result<()> {
        self.db()?.execute(
            "INSERT OR REPLACE INTO io_activity VALUES(?1,?2,?3)",
            params![filesystem, finished_ms, active],
        )?;
        Ok(())
    }
    pub fn record_inspection(&self, kind: &str, report: &serde_json::Value) -> Result<()> {
        let id = report["id"]
            .as_str()
            .map(str::to_owned)
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        self.db()?.execute(
            "INSERT OR REPLACE INTO inspections VALUES(?1,?2,?3,?4)",
            params![
                id,
                kind,
                chrono::Utc::now().timestamp_millis(),
                report.to_string()
            ],
        )?;
        Ok(())
    }
    pub fn inspections(&self, kind: &str) -> Result<Vec<serde_json::Value>> {
        let db = self.db()?;
        let mut statement =
            db.prepare("SELECT info FROM inspections WHERE kind=?1 ORDER BY created_ms DESC")?;
        let rows = statement.query_map([kind], |r| r.get::<_, String>(0))?;
        let mut reports = Vec::new();
        for row in rows {
            reports.push(serde_json::from_str(&row?)?);
        }
        Ok(reports)
    }
    pub fn record_source_check(&self, target: &str, capture_ms: Option<i64>) -> Result<()> {
        self.db()?.execute(
            "INSERT OR REPLACE INTO source_checks VALUES(?1,?2,?3)",
            params![target, chrono::Utc::now().timestamp_millis(), capture_ms],
        )?;
        Ok(())
    }
    pub fn open(directory: &Path) -> Result<Self> {
        let connection = Connection::open(directory.join("state.sqlite3"))?;
        connection.busy_timeout(std::time::Duration::from_secs(10))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(
            version <= 3,
            "state database is from a newer software version"
        );
        let check: String = connection.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
        ensure!(
            check == "ok",
            "state database integrity check failed: {check}"
        );
        connection.execute_batch("BEGIN;
            CREATE TABLE IF NOT EXISTS schedules(target_id TEXT PRIMARY KEY, next_due INTEGER NOT NULL, last_error TEXT);
            CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY, target_id TEXT NOT NULL, spec TEXT NOT NULL,
                status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, next_at INTEGER NOT NULL, intention TEXT, error TEXT);
            DROP INDEX IF EXISTS one_outstanding;
            CREATE UNIQUE INDEX one_outstanding ON jobs(target_id) WHERE status IN ('queued','running','publishing','retry','post_processing');
            CREATE TABLE IF NOT EXISTS snapshots(job_id TEXT PRIMARY KEY, info TEXT NOT NULL, healthy INTEGER NOT NULL DEFAULT 1, deleting INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS hook_cleanups(job_id TEXT PRIMARY KEY,target_id TEXT NOT NULL,spec TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS job_results(id TEXT PRIMARY KEY,target_id TEXT NOT NULL,finished_ms INTEGER NOT NULL,info TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS job_progress(id TEXT PRIMARY KEY,phase TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS io_activity(filesystem TEXT PRIMARY KEY,finished_ms INTEGER NOT NULL,active INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS inspections(id TEXT PRIMARY KEY,kind TEXT NOT NULL,created_ms INTEGER NOT NULL,info TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS source_checks(target_id TEXT PRIMARY KEY,checked_ms INTEGER NOT NULL,capture_ms INTEGER);
            CREATE TABLE IF NOT EXISTS job_timings(id TEXT PRIMARY KEY,info TEXT NOT NULL);
            COMMIT;")?;
        if version < 3 {
            connection.execute_batch("BEGIN; ALTER TABLE schedules ADD COLUMN signature TEXT; PRAGMA user_version=3; COMMIT;")?;
        }
        Ok(Self(Arc::new(Mutex::new(connection))))
    }
    fn db(&self) -> Result<MutexGuard<'_, Connection>> {
        self.0
            .lock()
            .map_err(|_| anyhow::anyhow!("state mutex poisoned"))
    }

    pub fn sync_schedules(&self, config: &Config, now: i64) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction()?;
        for target in &config.targets {
            let due = if target.run_on_startup && !target.manual_only {
                now
            } else {
                crate::scheduler::next_for(target, now, now)?
            };
            let signature = serde_json::to_string(&(
                &target.schedule,
                &target.interval_anchor,
                target.backup_interval_seconds,
                target.manual_only,
            ))?;
            let previous: Option<Option<String>> = tx
                .query_row(
                    "SELECT signature FROM schedules WHERE target_id=?1",
                    [&target.id],
                    |r| r.get(0),
                )
                .optional()?;
            match previous {
                None => {
                    tx.execute(
                        "INSERT INTO schedules(target_id,next_due,signature) VALUES(?1,?2,?3)",
                        params![target.id, due, signature],
                    )?;
                }
                Some(None) => {
                    tx.execute(
                        "UPDATE schedules SET signature=?2 WHERE target_id=?1",
                        params![target.id, signature],
                    )?;
                }
                Some(Some(old)) if old != signature => {
                    tx.execute(
                        "UPDATE schedules SET next_due=?2,signature=?3 WHERE target_id=?1",
                        params![
                            target.id,
                            crate::scheduler::next_for(target, now, now)?,
                            signature
                        ],
                    )?;
                }
                _ => (),
            }
        }
        tx.commit()?;
        Ok(())
    }
    pub fn due(&self, target: &str) -> Result<i64> {
        Ok(self.db()?.query_row(
            "SELECT next_due FROM schedules WHERE target_id=?1",
            [target],
            |r| r.get(0),
        )?)
    }
    pub fn advance(&self, target: &str, due: i64) -> Result<()> {
        self.db()?.execute(
            "UPDATE schedules SET next_due=?2 WHERE target_id=?1",
            params![target, due],
        )?;
        Ok(())
    }
    pub fn outstanding(&self, target: &str) -> Result<bool> {
        Ok(self.db()?.query_row("SELECT EXISTS(SELECT 1 FROM jobs WHERE target_id=?1 AND status IN ('queued','running','publishing','retry','post_processing'))", [target], |r| r.get(0))?)
    }
    pub fn pending_count(&self) -> Result<usize> {
        Ok(self.db()?.query_row(
            "SELECT COUNT(*) FROM jobs WHERE status IN ('queued','retry')",
            [],
            |r| r.get::<_, i64>(0),
        )? as usize)
    }
    pub fn enqueue(&self, spec: &JobSpec, now: i64) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        self.db()?.execute(
            "INSERT INTO jobs(id,target_id,spec,status,next_at) VALUES(?1,?2,?3,'queued',?4)",
            params![id, spec.target.id, serde_json::to_string(spec)?, now],
        )?;
        self.db()?.execute("INSERT INTO job_timings(id,info) VALUES(?1,?2)",params![id,serde_json::json!({"requested_ms":now,"scheduled_ms":null,"phase_durations_ms":{},"last_phase_ms":now}).to_string()])?;
        Ok(id)
    }
    pub fn jobs(&self, only_due: Option<i64>) -> Result<Vec<Job>> {
        let db = self.db()?;
        let mut statement = db.prepare("SELECT id,spec,attempts FROM jobs WHERE status IN ('queued','retry','running','publishing','post_processing') AND (?1 IS NULL OR (status IN ('queued','retry') AND next_at<=?1)) ORDER BY next_at,rowid")?;
        let rows = statement.query_map([only_due], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, u32>(2)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (id, spec, attempts) = row?;
            result.push(Job {
                id,
                spec: serde_json::from_str(&spec)?,
                attempts,
            });
        }
        Ok(result)
    }
    pub fn start(&self, id: &str) -> Result<()> {
        self.db()?.execute(
            "UPDATE jobs SET status='running',attempts=attempts+1 WHERE id=?1",
            [id],
        )?;
        self.progress(id, "dispatch")?;
        Ok(())
    }
    pub fn dispatch(&self, job: &Job) -> Result<()> {
        self.db()?.execute(
            "UPDATE jobs SET status='running',attempts=attempts+1,spec=?2 WHERE id=?1",
            params![job.id, serde_json::to_string(&job.spec)?],
        )?;
        self.progress(&job.id, "dispatch")?;
        Ok(())
    }
    pub fn is_interrupted(&self, id: &str) -> Result<bool> {
        Ok(self.db()?.query_row(
            "SELECT status IN ('running','publishing','post_processing') FROM jobs WHERE id=?1",
            [id],
            |r| r.get(0),
        )?)
    }
    pub fn prepare(&self, snapshot: &Snapshot) -> Result<()> {
        self.db()?.execute(
            "UPDATE jobs SET status='publishing',intention=?2 WHERE id=?1",
            params![snapshot.job_id, serde_json::to_string(snapshot)?],
        )?;
        Ok(())
    }
    pub fn intention(&self, id: &str) -> Result<Option<Snapshot>> {
        let json: Option<String> = self
            .db()?
            .query_row("SELECT intention FROM jobs WHERE id=?1", [id], |r| r.get(0))
            .optional()?
            .flatten();
        json.map(|j| serde_json::from_str(&j).context("invalid publication intention"))
            .transpose()
    }
    pub fn complete(&self, snapshot: &Snapshot) -> Result<()> {
        let mut db = self.db()?;
        let tx = db.transaction()?;
        tx.execute(
            "INSERT OR IGNORE INTO snapshots(job_id,info) VALUES(?1,?2)",
            params![snapshot.job_id, serde_json::to_string(snapshot)?],
        )?;
        tx.execute(
            "UPDATE schedules SET last_error=NULL WHERE target_id=?1",
            [&snapshot.target_id],
        )?;
        tx.execute(
            "UPDATE jobs SET status='post_processing',intention=?2 WHERE id=?1",
            params![snapshot.job_id, serde_json::to_string(snapshot)?],
        )?;
        tx.commit()?;
        Ok(())
    }
    pub fn failed(&self, job: &Job, retry_at: Option<i64>, error: &str) -> Result<()> {
        if let Some(at) = retry_at {
            let mut db = self.db()?;
            let tx = db.transaction()?;
            tx.execute(
                "UPDATE schedules SET last_error=?2 WHERE target_id=?1",
                params![job.spec.target.id, error],
            )?;
            tx.execute(
                "UPDATE jobs SET status='retry',next_at=?2,error=?3,intention=NULL WHERE id=?1",
                params![job.id, at, error],
            )?;
            tx.commit()?;
            Ok(())
        } else {
            self.finish_job(job, "failed", Some(error), None)
        }
    }
    pub fn catalog(&self) -> Result<Vec<(Snapshot, bool, bool)>> {
        let db = self.db()?;
        let mut statement = db.prepare("SELECT info,healthy,deleting FROM snapshots")?;
        let rows = statement.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, bool>(1)?,
                r.get::<_, bool>(2)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (j, healthy, deleting) = row?;
            result.push((serde_json::from_str(&j)?, healthy, deleting));
        }
        Ok(result)
    }
    pub fn mark_deleting(&self, id: &str) -> Result<()> {
        self.db()?
            .execute("UPDATE snapshots SET deleting=1 WHERE job_id=?1", [id])?;
        Ok(())
    }
    pub fn cancel_deletion(&self, id: &str) -> Result<()> {
        self.db()?
            .execute("UPDATE snapshots SET deleting=0 WHERE job_id=?1", [id])?;
        Ok(())
    }
    pub fn deleted(&self, id: &str) -> Result<()> {
        self.db()?
            .execute("DELETE FROM snapshots WHERE job_id=?1", [id])?;
        Ok(())
    }
    pub fn quarantine(&self, id: &str) -> Result<()> {
        self.db()?.execute(
            "UPDATE snapshots SET healthy=0,deleting=0 WHERE job_id=?1",
            [id],
        )?;
        Ok(())
    }
    pub fn status(&self, config: &Config) -> Result<serde_json::Value> {
        let catalog = self.catalog()?;
        let incidents = crate::integrity::unresolved(self)?;
        let jobs = self.jobs(None)?;
        let db = self.db()?;
        let mut targets = Vec::new();
        for t in &config.targets {
            let last = catalog
                .iter()
                .filter(|(s, h, _)| *h && s.target_id == t.id)
                .map(|(s, _, _)| s.capture_ms)
                .max();
            let error: Option<String> = db
                .query_row(
                    "SELECT last_error FROM schedules WHERE target_id=?1",
                    [&t.id],
                    |r| r.get(0),
                )
                .optional()?
                .flatten();
            let checked: Option<i64> = db
                .query_row(
                    "SELECT checked_ms FROM source_checks WHERE target_id=?1",
                    [&t.id],
                    |r| r.get(0),
                )
                .optional()?;
            let cleanup: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM hook_cleanups WHERE target_id=?1)",
                [&t.id],
                |r| r.get(0),
            )?;
            let age = last.map(|at| (chrono::Utc::now().timestamp_millis() - at).max(0) / 1000);
            let overdue = t.enabled
                && (last.is_none()
                    || t.max_capture_age_seconds
                        .is_some_and(|limit| age.is_none_or(|age| age as u64 > limit)));
            let mut result_rows=db.prepare("SELECT info FROM job_results WHERE target_id=?1 ORDER BY finished_ms DESC LIMIT 100")?;
            let results = result_rows
                .query_map([&t.id], |r| r.get::<_, String>(0))?
                .map(|s| Ok(serde_json::from_str::<crate::domain::JobStatus>(&s?)?))
                .collect::<Result<Vec<_>>>()?;
            let durations = results
                .iter()
                .filter(|r| r.status == "succeeded")
                .filter_map(|r| {
                    Some(
                        r.metrics["finished_ms"]
                            .as_i64()?
                            .saturating_sub(r.metrics["actual_start_ms"].as_i64()?),
                    )
                })
                .collect::<Vec<_>>();
            let degraded = cleanup
                || overdue
                || error.is_some()
                || incidents.iter().any(|incident| incident["target"] == t.id);
            targets.push(serde_json::json!({"id":t.id,"enabled":t.enabled,"manual_only":t.manual_only,"health":if !t.enabled{"disabled"}else if degraded{"degraded"}else{"healthy"},"capture_overdue":overdue,"max_capture_age_seconds":t.max_capture_age_seconds,"last_success_ms":last,"last_capture_ms":last,"last_source_check_ms":checked,
                "last_success_age_seconds":age,"last_error":error,"next_due_ms":if t.manual_only{None}else{Some(self_due(&db,&t.id)?)},
                "cleanup_pending":cleanup,"outstanding_jobs":jobs.iter().filter(|j|j.spec.target.id==t.id).count(),
                "healthy_snapshots":catalog.iter().filter(|(s,h,_)|*h && s.target_id==t.id).count(),
                "stored_bytes":catalog.iter().filter(|(s,_,_)|s.target_id==t.id).map(|(s,_,_)|s.bytes as u128).sum::<u128>().min(u64::MAX as u128) as u64,
                "last_job":results.first(),"duration_sample_count":durations.len(),"mean_duration_ms":if durations.is_empty(){None}else{Some(durations.iter().map(|n|*n as i128).sum::<i128>()/durations.len() as i128)}}));
        }
        drop(db);
        let summary = crate::planning::retention_preview(
            config,
            &catalog,
            chrono::Utc::now().timestamp_millis(),
        )?;
        Ok(crate::config::redacted(
            serde_json::json!({"schema_version":1,"version":env!("CARGO_PKG_VERSION"),"config":config,"targets":targets,"retention":summary,
            "maintenance":self.inspections("maintenance")?,"last_scrub":self.inspections("scrub_summary")?.first(),"last_rehearsal":self.inspections("rehearsal_summary")?.first(),
            "recent_integrity_incidents":self.inspections("integrity_incident")?.into_iter().take(20).collect::<Vec<_>>()}),
        ))
    }
    pub fn historical_targets(&self) -> Result<Vec<Target>> {
        let mut targets: Vec<_> = self
            .catalog()?
            .into_iter()
            .map(|(s, _, _)| s.target)
            .collect();
        targets.extend(self.jobs(None)?.into_iter().map(|j| j.spec.target));
        targets.extend(self.pending_cleanups()?.into_iter().map(|j| j.spec.target));
        Ok(targets)
    }
    pub fn register_cleanup(&self, job: &Job) -> Result<()> {
        self.db()?.execute(
            "INSERT OR REPLACE INTO hook_cleanups(job_id,target_id,spec) VALUES(?1,?2,?3)",
            params![job.id, job.spec.target.id, serde_json::to_string(job)?],
        )?;
        Ok(())
    }
    pub fn clear_cleanup(&self, id: &str) -> Result<()> {
        self.db()?
            .execute("DELETE FROM hook_cleanups WHERE job_id=?1", [id])?;
        Ok(())
    }
    pub fn pending_cleanups(&self) -> Result<Vec<Job>> {
        let db = self.db()?;
        let mut statement = db.prepare("SELECT spec FROM hook_cleanups ORDER BY rowid")?;
        let rows = statement.query_map([], |r| r.get::<_, String>(0))?;
        let mut result = Vec::new();
        for row in rows {
            result.push(serde_json::from_str(&row?)?);
        }
        Ok(result)
    }
    pub fn cleanup_pending(&self, target: &str) -> Result<bool> {
        Ok(self.db()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM hook_cleanups WHERE target_id=?1)",
            [target],
            |r| r.get(0),
        )?)
    }
    pub fn finish_job(
        &self,
        job: &Job,
        status: &str,
        error: Option<&str>,
        snapshot: Option<&Snapshot>,
    ) -> Result<()> {
        self.progress(&job.id, "finished")?;
        let mut db = self.db()?;
        let tx = db.transaction()?;
        let intention: Option<String> = tx
            .query_row("SELECT intention FROM jobs WHERE id=?1", [&job.id], |r| {
                r.get(0)
            })
            .optional()?
            .flatten();
        let snapshot = snapshot
            .cloned()
            .or(intention.map(|j| serde_json::from_str(&j)).transpose()?);
        let classification = error.map(|e| {
            if e.contains("source changed") || e.contains("source selection changed") {
                "changing_source"
            } else if e.contains("Permission denied") {
                "permission_error"
            } else if e.contains("mismatch") || e.contains("integrity") {
                "integrity_failure"
            } else {
                "operation_failure"
            }
        });
        let result = crate::domain::JobStatus {
            id: job.id.clone(),
            target_id: job.spec.target.id.clone(),
            status: status.into(),
            phase: "finished".into(),
            attempts: job.attempts,
            error: error.map(str::to_owned),
            snapshot,
            metrics: tx
                .query_row("SELECT info FROM job_timings WHERE id=?1", [&job.id], |r| {
                    r.get::<_, String>(0)
                })
                .optional()?
                .map(|s| serde_json::from_str(&s))
                .transpose()?
                .unwrap_or_default(),
        };
        let mut result = result;
        if !result.metrics.is_object() {
            result.metrics = serde_json::json!({});
        }
        result.metrics["error_kind"] = serde_json::json!(classification);
        tx.execute(
            "INSERT OR REPLACE INTO job_results(id,target_id,finished_ms,info) VALUES(?1,?2,?3,?4)",
            params![
                job.id,
                job.spec.target.id,
                chrono::Utc::now().timestamp_millis(),
                serde_json::to_string(&result)?
            ],
        )?;
        tx.execute(
            "UPDATE schedules SET last_error=?2 WHERE target_id=?1",
            params![job.spec.target.id, error],
        )?;
        tx.execute("DELETE FROM jobs WHERE id=?1", [&job.id])?;
        tx.execute("DELETE FROM job_progress WHERE id=?1", [&job.id])?;
        tx.execute("DELETE FROM job_timings WHERE id=?1", [&job.id])?;
        tx.execute("DELETE FROM job_results WHERE id NOT IN (SELECT id FROM job_results ORDER BY finished_ms DESC LIMIT 1000)",[])?;
        tx.commit()?;
        Ok(())
    }
    pub fn progress(&self, id: &str, phase: &str) -> Result<()> {
        let db = self.db()?;
        let prior: Option<String> = db
            .query_row("SELECT info FROM job_timings WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(prior) = prior {
            let mut metrics: serde_json::Value = serde_json::from_str(&prior)?;
            let now = chrono::Utc::now().timestamp_millis();
            if let Some(old) = metrics["phase"].as_str().map(str::to_owned) {
                let elapsed = now
                    .saturating_sub(metrics["last_phase_ms"].as_i64().unwrap_or(now))
                    .max(0);
                let sum = metrics["phase_durations_ms"][&old]
                    .as_i64()
                    .unwrap_or(0)
                    .saturating_add(elapsed);
                metrics["phase_durations_ms"][old] = sum.into();
            }
            if phase == "dispatch" {
                metrics["actual_start_ms"] = now.into();
                if let Some(scheduled) = metrics["scheduled_ms"].as_i64() {
                    metrics["schedule_delay_ms"] = now.saturating_sub(scheduled).max(0).into();
                }
                metrics["queue_delay_ms"] = now
                    .saturating_sub(metrics["requested_ms"].as_i64().unwrap_or(now))
                    .max(0)
                    .into();
            }
            if phase == "finished" {
                metrics["finished_ms"] = now.into();
                metrics["total_elapsed_ms"] = now
                    .saturating_sub(metrics["requested_ms"].as_i64().unwrap_or(now))
                    .max(0)
                    .into();
            }
            metrics["phase"] = phase.into();
            metrics["last_phase_ms"] = now.into();
            db.execute(
                "UPDATE job_timings SET info=?2 WHERE id=?1",
                params![id, metrics.to_string()],
            )?;
        }
        db.execute(
            "INSERT OR REPLACE INTO job_progress(id,phase) VALUES(?1,?2)",
            params![id, phase],
        )?;
        Ok(())
    }
    pub fn job_status(&self, id: &str) -> Result<Option<crate::domain::JobStatus>> {
        let db = self.db()?;
        type ActiveJobRow = (
            String,
            String,
            u32,
            Option<String>,
            Option<String>,
            Option<String>,
        );
        let active:Option<ActiveJobRow>=db.query_row("SELECT j.target_id,j.status,j.attempts,j.error,j.intention,p.phase FROM jobs j LEFT JOIN job_progress p ON p.id=j.id WHERE j.id=?1",[id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?))).optional()?;
        if let Some((target_id, status, attempts, error, intention, phase)) = active {
            return Ok(Some(crate::domain::JobStatus {
                id: id.into(),
                target_id,
                phase: phase.unwrap_or_else(|| status.clone()),
                status,
                attempts,
                error,
                snapshot: intention.map(|j| serde_json::from_str(&j)).transpose()?,
                metrics: db
                    .query_row("SELECT info FROM job_timings WHERE id=?1", [id], |r| {
                        r.get::<_, String>(0)
                    })
                    .optional()?
                    .map(|s| serde_json::from_str(&s))
                    .transpose()?
                    .unwrap_or_default(),
            }));
        }
        let result: Option<String> = db
            .query_row("SELECT info FROM job_results WHERE id=?1", [id], |r| {
                r.get(0)
            })
            .optional()?;
        result.map(|j| Ok(serde_json::from_str(&j)?)).transpose()
    }
}
fn self_due(db: &Connection, id: &str) -> Result<i64> {
    Ok(db.query_row(
        "SELECT next_due FROM schedules WHERE target_id=?1",
        [id],
        |r| r.get(0),
    )?)
}

impl crate::api::StateStore for State {
    fn scheduled_time(&self, job: &str, scheduled_ms: i64) -> Result<()> {
        State::scheduled_time(self, job, scheduled_ms)
    }
    fn io_activity(&self) -> Result<Vec<(String, i64, bool)>> {
        State::io_activity(self)
    }
    fn set_io_activity(&self, filesystem: &str, finished_ms: i64, active: bool) -> Result<()> {
        State::set_io_activity(self, filesystem, finished_ms, active)
    }
    fn record_inspection(&self, kind: &str, report: &serde_json::Value) -> Result<()> {
        State::record_inspection(self, kind, report)
    }
    fn inspections(&self, kind: &str) -> Result<Vec<serde_json::Value>> {
        State::inspections(self, kind)
    }
    fn record_source_check(&self, target: &str, capture_ms: Option<i64>) -> Result<()> {
        State::record_source_check(self, target, capture_ms)
    }
    fn register_cleanup(&self, job: &Job) -> Result<()> {
        State::register_cleanup(self, job)
    }
    fn clear_cleanup(&self, id: &str) -> Result<()> {
        State::clear_cleanup(self, id)
    }
    fn pending_cleanups(&self) -> Result<Vec<Job>> {
        State::pending_cleanups(self)
    }
    fn cleanup_pending(&self, target: &str) -> Result<bool> {
        State::cleanup_pending(self, target)
    }
    fn finish_job(
        &self,
        job: &Job,
        status: &str,
        error: Option<&str>,
        snapshot: Option<&Snapshot>,
    ) -> Result<()> {
        State::finish_job(self, job, status, error, snapshot)
    }
    fn job_status(&self, id: &str) -> Result<Option<crate::domain::JobStatus>> {
        State::job_status(self, id)
    }
    fn progress(&self, id: &str, phase: &str) -> Result<()> {
        State::progress(self, id, phase)
    }
    fn sync_schedules(&self, config: &Config, now: i64) -> Result<()> {
        State::sync_schedules(self, config, now)
    }
    fn due(&self, target: &str) -> Result<i64> {
        State::due(self, target)
    }
    fn advance(&self, target: &str, due: i64) -> Result<()> {
        State::advance(self, target, due)
    }
    fn outstanding(&self, target: &str) -> Result<bool> {
        State::outstanding(self, target)
    }
    fn pending_count(&self) -> Result<usize> {
        State::pending_count(self)
    }
    fn enqueue(&self, spec: &JobSpec, now: i64) -> Result<String> {
        State::enqueue(self, spec, now)
    }
    fn jobs(&self, only_due: Option<i64>) -> Result<Vec<Job>> {
        State::jobs(self, only_due)
    }
    fn start(&self, id: &str) -> Result<()> {
        State::start(self, id)
    }
    fn dispatch(&self, job: &Job) -> Result<()> {
        State::dispatch(self, job)
    }
    fn is_interrupted(&self, id: &str) -> Result<bool> {
        State::is_interrupted(self, id)
    }
    fn prepare(&self, snapshot: &Snapshot) -> Result<()> {
        State::prepare(self, snapshot)
    }
    fn intention(&self, id: &str) -> Result<Option<Snapshot>> {
        State::intention(self, id)
    }
    fn complete(&self, snapshot: &Snapshot) -> Result<()> {
        State::complete(self, snapshot)
    }
    fn failed(&self, job: &Job, retry_at: Option<i64>, error: &str) -> Result<()> {
        State::failed(self, job, retry_at, error)
    }
    fn catalog(&self) -> Result<Vec<(Snapshot, bool, bool)>> {
        State::catalog(self)
    }
    fn mark_deleting(&self, id: &str) -> Result<()> {
        State::mark_deleting(self, id)
    }
    fn cancel_deletion(&self, id: &str) -> Result<()> {
        State::cancel_deletion(self, id)
    }
    fn deleted(&self, id: &str) -> Result<()> {
        State::deleted(self, id)
    }
    fn quarantine(&self, id: &str) -> Result<()> {
        State::quarantine(self, id)
    }
    fn status(&self, config: &Config) -> Result<serde_json::Value> {
        State::status(self, config)
    }
    fn historical_targets(&self) -> Result<Vec<Target>> {
        State::historical_targets(self)
    }
}

pub fn open(config: &Config) -> Result<std::sync::Arc<dyn crate::api::StateStore>> {
    ensure!(
        config.backends.state == "sqlite",
        "unsupported state backend"
    );
    Ok(std::sync::Arc::new(State::open(&config.state_dir)?))
}
