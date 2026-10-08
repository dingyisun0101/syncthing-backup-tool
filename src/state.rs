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
    pub fn open(directory: &Path) -> Result<Self> {
        let connection = Connection::open(directory.join("state.sqlite3"))?;
        connection.busy_timeout(std::time::Duration::from_secs(10))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA foreign_keys=ON;",
        )?;
        let version: i64 = connection.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(
            version <= 2,
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
            CREATE UNIQUE INDEX one_outstanding ON jobs(target_id) WHERE status IN ('queued','running','publishing','retry','post_processing','post_processing');
            CREATE TABLE IF NOT EXISTS snapshots(job_id TEXT PRIMARY KEY, info TEXT NOT NULL, healthy INTEGER NOT NULL DEFAULT 1, deleting INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS hook_cleanups(job_id TEXT PRIMARY KEY,target_id TEXT NOT NULL,spec TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS job_results(id TEXT PRIMARY KEY,target_id TEXT NOT NULL,finished_ms INTEGER NOT NULL,info TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS job_progress(id TEXT PRIMARY KEY,phase TEXT NOT NULL);
            PRAGMA user_version=2; COMMIT;")?;
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
            let due = if target.run_on_startup {
                now
            } else {
                crate::scheduler::next_for(target, now, now)?
            };
            tx.execute(
                "INSERT OR IGNORE INTO schedules(target_id,next_due) VALUES(?1,?2)",
                params![target.id, due],
            )?;
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
        Ok(())
    }
    pub fn dispatch(&self, job: &Job) -> Result<()> {
        self.db()?.execute(
            "UPDATE jobs SET status='running',attempts=attempts+1,spec=?2 WHERE id=?1",
            params![job.id, serde_json::to_string(&job.spec)?],
        )?;
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
            targets.push(serde_json::json!({"id":t.id,"enabled":t.enabled,"last_success_ms":last,
                "last_success_age_seconds":last.map(|at| (chrono::Utc::now().timestamp_millis()-at).max(0)/1000),
                "last_error":error,"next_due_ms":self_due(&db,&t.id)?,
                "cleanup_pending":db.query_row("SELECT EXISTS(SELECT 1 FROM hook_cleanups WHERE target_id=?1)",[&t.id],|r|r.get::<_,bool>(0))?,
                "outstanding_jobs":jobs.iter().filter(|j| j.spec.target.id==t.id).count(),
                "healthy_snapshots":catalog.iter().filter(|(s,h,_)| *h && s.target_id==t.id).count(),
                "stored_bytes":catalog.iter().filter(|(s,_,_)| s.target_id==t.id).map(|(s,_,_)| s.bytes).sum::<u64>()}));
        }
        Ok(
            serde_json::json!({"version":env!("CARGO_PKG_VERSION"),"config":config,"targets":targets}),
        )
    }
    pub fn historical_targets(&self) -> Result<Vec<Target>> {
        let mut targets: Vec<_> = self
            .catalog()?
            .into_iter()
            .map(|(s, _, _)| s.target)
            .collect();
        targets.extend(self.jobs(None)?.into_iter().map(|j| j.spec.target));
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
        let result = crate::domain::JobStatus {
            id: job.id.clone(),
            target_id: job.spec.target.id.clone(),
            status: status.into(),
            phase: "finished".into(),
            attempts: job.attempts,
            error: error.map(str::to_owned),
            snapshot,
        };
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
        tx.execute("DELETE FROM job_results WHERE id NOT IN (SELECT id FROM job_results ORDER BY finished_ms DESC LIMIT 1000)",[])?;
        tx.commit()?;
        Ok(())
    }
    pub fn progress(&self, id: &str, phase: &str) -> Result<()> {
        self.db()?.execute(
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
