use chrono::TimeZone;
use serde_json::json;
use std::{fs, path::Path, sync::atomic::AtomicBool, time::Duration};
use syncthing_backup_tool::{
    config::Config,
    daemon::Instance,
    domain::{Job, JobSpec, Snapshot},
    integrity, io_policy, migration, planning, restore, snapshot, source,
};
struct Fixture {
    temp: tempfile::TempDir,
    config: Config,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        fs::create_dir(root.join("source")).unwrap();
        let config:Config=serde_json::from_value(json!({"config_version":1,"state_dir":root.join("state"),"targets":[{"id":"test","source_dir":root.join("source"),"destination_dir":root.join("archives"),"run_on_startup":false,"storage":{"min_free_bytes":0},"retention":{"min_snapshots":2,"max_snapshots":4}}]})).unwrap();
        config.validate().unwrap();
        Self { temp, config }
    }
    fn source(&self) -> &Path {
        &self.config.targets[0].source_dir
    }
    fn root(&self) -> &Path {
        self.temp.path()
    }
    fn instance(&self) -> Instance {
        let instance = Instance::open(&self.config).unwrap();
        instance
            .state
            .sync_schedules(&self.config, chrono::Utc::now().timestamp_millis())
            .unwrap();
        instance
    }
    fn capture(&self, instance: &Instance) -> Snapshot {
        let spec = JobSpec {
            backends: self.config.backends.clone(),
            target: self.config.targets[0].clone(),
            resources: self.config.resources.clone(),
        };
        let id = instance
            .state
            .enqueue(&spec, chrono::Utc::now().timestamp_millis())
            .unwrap();
        instance.state.start(&id).unwrap();
        snapshot::create(
            &Job {
                id,
                spec,
                attempts: 1,
            },
            instance.state.as_ref(),
            &self.config.state_dir,
            &AtomicBool::new(false),
        )
        .unwrap()
    }
    fn archive(&self, s: &Snapshot) -> std::path::PathBuf {
        s.target.destination_dir.join(&s.filename)
    }
}
#[test]
fn multi_slots_and_anchored_intervals_distinguish_both_dst_transitions() {
    let tz = chrono_tz::America::Los_Angeles;
    let schedule=serde_json::from_value(json!({"frequency":"daily","times":["00:30","06:30","12:30","18:30"],"timezone":"America/Los_Angeles"})).unwrap();
    for (month, day, elapsed) in [(3, 8, 5), (11, 1, 7)] {
        let start = tz.with_ymd_and_hms(2026, month, day, 0, 30, 0).unwrap();
        let next =
            syncthing_backup_tool::scheduler::calendar_next(&schedule, start.timestamp_millis())
                .unwrap();
        assert_eq!(
            next,
            tz.with_ymd_and_hms(2026, month, day, 6, 30, 0)
                .unwrap()
                .timestamp_millis()
        );
        assert_eq!(next - start.timestamp_millis(), elapsed * 3600 * 1000);
        let mut f = Fixture::new();
        f.config.targets[0].interval_anchor = Some(start.to_rfc3339());
        f.config.targets[0].backup_interval_seconds = 21600;
        assert_eq!(
            syncthing_backup_tool::scheduler::next_for(
                &f.config.targets[0],
                0,
                start.timestamp_millis()
            )
            .unwrap(),
            start.timestamp_millis() + 21600 * 1000
        );
    }
    let schedule = serde_json::from_value(
        json!({"frequency":"daily","times":["01:30","03:30"],"timezone":"America/Los_Angeles"}),
    )
    .unwrap();
    let second_fold = tz.with_ymd_and_hms(2026, 11, 1, 1, 0, 0).latest().unwrap();
    assert_eq!(
        syncthing_backup_tool::scheduler::calendar_next(&schedule, second_fold.timestamp_millis())
            .unwrap(),
        tz.with_ymd_and_hms(2026, 11, 1, 3, 30, 0)
            .unwrap()
            .timestamp_millis()
    );
}
#[test]
fn reload_and_restart_preserve_phases_and_manual_deadlines() {
    let mut f = Fixture::new();
    let anchor = "2026-01-01T00:30:00Z";
    f.config.targets[0].interval_anchor = Some(anchor.into());
    let now = chrono::DateTime::parse_from_rfc3339("2026-10-08T04:00:00Z")
        .unwrap()
        .timestamp_millis();
    let instance = f.instance();
    instance.state.sync_schedules(&f.config, now).unwrap();
    let due = instance.state.due("test").unwrap();
    let _ = f.capture(&instance);
    assert_eq!(instance.state.due("test").unwrap(), due);
    drop(instance);
    let instance = f.instance();
    assert_eq!(instance.state.due("test").unwrap(), due);
    f.config.targets[0].interval_anchor = Some("2026-01-01T01:00:00Z".into());
    instance.state.sync_schedules(&f.config, now).unwrap();
    assert_eq!(
        instance.state.due("test").unwrap(),
        chrono::DateTime::parse_from_rfc3339("2026-10-08T07:00:00Z")
            .unwrap()
            .timestamp_millis()
    );
    f.config.targets[0].manual_only = true;
    instance.state.sync_schedules(&f.config, now).unwrap();
    assert_eq!(instance.state.due("test").unwrap(), i64::MAX);
    let _ = f.capture(&instance);
    assert_eq!(instance.state.due("test").unwrap(), i64::MAX);
}
#[test]
fn planning_is_read_only_and_counts_the_entire_shared_disk() {
    let mut f = Fixture::new();
    fs::write(f.source().join("keep"), b"12345").unwrap();
    fs::write(f.source().join("omit.tmp"), b"123").unwrap();
    f.config.targets[0].exclude_globs = vec!["*.tmp".into(), "gone/**".into()];
    f.config.logging.audit_file = Some(f.root().join("audit.jsonl"));
    let mut second = f.config.targets[0].clone();
    second.id = "second".into();
    second.destination_dir = f.root().join("second-archives");
    second.source_dir = f.root().join("second-source");
    fs::create_dir(&second.source_dir).unwrap();
    fs::write(second.source_dir.join("data"), b"1234567890").unwrap();
    second.storage.min_free_bytes = fs2::available_space(f.root()).unwrap() + 1;
    f.config.targets.push(second);
    let report = planning::plan(&f.config, None, Some("test")).unwrap();
    assert_eq!(report["targets"].as_array().unwrap().len(), 1);
    assert_eq!(report["targets"][0]["selection"]["file_count"], 1);
    assert_eq!(
        report["targets"][0]["selection"]["unmatched_exclusions"],
        json!(["gone/**"])
    );
    assert_eq!(report["filesystems"].as_array().unwrap().len(), 1);
    assert_eq!(
        report["filesystems"][0]["targets"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(report["filesystems"][0]["affordable"], false);
    assert!(!f.config.state_dir.exists());
    assert!(!f.config.targets[0].destination_dir.exists());
    assert!(!f.config.targets[1].destination_dir.exists());
    assert!(!f.config.logging.audit_file.as_ref().unwrap().exists());
    let mut proposed = f.config.clone();
    proposed.targets[0].exclude_globs.clear();
    let diff = planning::plan(&f.config, Some(&proposed), Some("test")).unwrap();
    assert_eq!(diff["targets"][0]["selection"]["file_count"], 2);
    assert_eq!(diff["targets"][0]["loaded_selection"]["file_count"], 1);
}
#[test]
fn planning_does_not_modify_an_existing_catalog_or_archives() {
    let f = Fixture::new();
    fs::write(f.source().join("data"), b"abc").unwrap();
    let instance = f.instance();
    let capture = f.capture(&instance);
    let before = fs::read(f.config.state_dir.join("state.sqlite3-wal")).unwrap();
    let archive = fs::read(f.archive(&capture)).unwrap();
    let _ = planning::plan(&f.config, None, None).unwrap();
    assert_eq!(
        fs::read(f.config.state_dir.join("state.sqlite3-wal")).unwrap(),
        before
    );
    assert_eq!(fs::read(f.archive(&capture)).unwrap(), archive);
    assert_eq!(instance.state.catalog().unwrap().len(), 1);
}
#[test]
fn cache_tags_require_exact_regular_proof_and_review_with_include_overrides() {
    let mut f = Fixture::new();
    let header = b"Signature: 8a477f597d28d172789f06886806bc55\n";
    for name in ["approved", "new", "invalid", "symlink", "target"] {
        fs::create_dir(f.source().join(name)).unwrap();
        fs::write(f.source().join(name).join("data"), b"precious").unwrap();
    }
    for name in ["approved", "new", "target"] {
        fs::write(f.source().join(name).join("CACHEDIR.TAG"), header).unwrap();
    }
    fs::write(
        f.source().join("invalid/CACHEDIR.TAG"),
        b" Signature: 8a477f597d28d172789f06886806bc55",
    )
    .unwrap();
    std::os::unix::fs::symlink(
        "../approved/CACHEDIR.TAG",
        f.source().join("symlink/CACHEDIR.TAG"),
    )
    .unwrap();
    fs::create_dir(f.source().join("approved/research")).unwrap();
    fs::write(f.source().join("approved/research/result"), b"keep").unwrap();
    f.config.targets[0].symlink_policy = "preserve".into();
    f.config.targets[0].cache.cachedir_tags = true;
    f.config.targets[0].cache.approved_paths =
        vec!["approved".into(), "invalid".into(), "symlink".into()];
    f.config.targets[0].include_paths = vec!["approved/research".into()];
    let report = source::inspect(
        &source::Source::open(&f.config.targets[0]).unwrap(),
        &f.config.targets[0],
        &f.config.resources,
    )
    .unwrap();
    let paths = report
        .entries
        .iter()
        .map(|e| e.path.as_str())
        .collect::<Vec<_>>();
    assert!(paths.contains(&"approved/research/result"));
    assert!(!paths.contains(&"approved/data"));
    assert!(paths.contains(&"new/data"));
    assert!(paths.contains(&"invalid/data"));
    assert!(paths.contains(&"symlink/data"));
    assert!(
        report
            .cache_candidates
            .iter()
            .any(|c| c.path == "new" && !c.approved)
    );
    assert!(
        !report
            .cache_candidates
            .iter()
            .any(|c| c.path == "invalid" || c.path == "symlink")
    );
    f.config.targets[0].cache.cachedir_tags = false;
    f.config.targets[0].cache.cargo_build = true;
    fs::write(
        f.source().join("Cargo.toml"),
        b"[package]\nname='example'\n",
    )
    .unwrap();
    fs::write(
        f.source().join("target/.rustc_info.json"),
        b"{\"rustc_fingerprint\":123}",
    )
    .unwrap();
    let report = source::inspect(
        &source::Source::open(&f.config.targets[0]).unwrap(),
        &f.config.targets[0],
        &f.config.resources,
    )
    .unwrap();
    assert!(report.cache_candidates.iter().any(|c| c.path == "target"));
    assert!(report.entries.iter().any(|e| e.path == "target/data"));
}
#[test]
fn cooldown_survives_restart_and_crash_and_other_disks_are_independent() {
    let f = Fixture::new();
    let instance = f.instance();
    let id = io_policy::filesystem(&f.config.targets[0]).unwrap();
    let io = io_policy::Coordinator::new(instance.state.clone()).unwrap();
    let permit = io.try_acquire(&id, 1800).unwrap().unwrap();
    assert!(io.try_acquire(&id, 0).unwrap().is_none());
    drop(permit);
    let (eligible, reason) = io.delay(&id, 1800).unwrap().unwrap();
    assert_eq!(reason, "filesystem cooldown");
    assert!(eligible - chrono::Utc::now().timestamp_millis() > 1_798_000);
    let io = io_policy::Coordinator::new(instance.state.clone()).unwrap();
    assert!(io.try_acquire(&id, 1800).unwrap().is_none());
    let other = tempfile::tempdir_in("/dev/shm").unwrap();
    let other_id = io_policy::filesystem_path(other.path()).unwrap();
    assert_ne!(id, other_id);
    assert!(io.try_acquire(&other_id, 1800).unwrap().is_some());
    instance.state.set_io_activity(&id, 0, true).unwrap();
    let io = io_policy::Coordinator::new(instance.state.clone()).unwrap();
    assert!(io.try_acquire(&id, 1800).unwrap().is_none());
    assert!(
        !instance
            .state
            .io_activity()
            .unwrap()
            .iter()
            .find(|(fs, _, _)| fs == &id)
            .unwrap()
            .2
    );
}
#[test]
fn scrub_detects_an_old_corrupt_archive_without_capture_or_retention_and_persists_evidence() {
    let f = Fixture::new();
    fs::write(f.source().join("data"), b"original").unwrap();
    let instance = f.instance();
    let s = f.capture(&instance);
    let mut bytes = fs::read(f.archive(&s)).unwrap();
    bytes[20] ^= 1;
    fs::write(f.archive(&s), &bytes).unwrap();
    let report =
        integrity::scrub(instance.state.as_ref(), &f.config, &AtomicBool::new(false)).unwrap();
    assert_eq!(report["healthy"], false);
    assert_eq!(report["failed"], 1);
    assert_eq!(instance.state.catalog().unwrap().len(), 1);
    assert!(instance.state.catalog().unwrap()[0].1);
    assert_eq!(fs::read(f.archive(&s)).unwrap(), bytes);
    let incidents = instance.state.inspections("integrity_incident").unwrap();
    assert_eq!(incidents.len(), 1);
    assert_eq!(
        incidents[0]["first_read"]["status"],
        "stable_metadata_mismatch"
    );
    assert_eq!(
        incidents[0]["controlled_reread"]["status"],
        "stable_metadata_mismatch"
    );
    assert_ne!(
        incidents[0]["first_read"]["actual_sha256"],
        incidents[0]["first_read"]["expected_sha256"]
    );
    drop(instance);
    let instance = f.instance();
    assert_eq!(
        instance
            .state
            .inspections("integrity_incident")
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn restore_verifies_contents_metadata_and_rejects_live_or_nonempty_destinations() {
    let f = Fixture::new();
    fs::create_dir(f.source().join("folder")).unwrap();
    fs::create_dir(f.source().join("empty")).unwrap();
    fs::write(f.source().join("folder/data"), b"recoverable").unwrap();
    let instance = f.instance();
    let s = f.capture(&instance);
    let cancel = AtomicBool::new(false);
    let output = f.root().join("restored");
    let result = restore::extract(
        &f.config,
        &f.archive(&s),
        &output,
        &[],
        None,
        false,
        &cancel,
    )
    .unwrap();
    assert_eq!(result["verified"], true);
    assert_eq!(
        fs::read(output.join("folder/data")).unwrap(),
        b"recoverable"
    );
    assert!(output.join("empty").is_dir());
    assert!(
        restore::extract(
            &f.config,
            &f.archive(&s),
            f.source(),
            &[],
            None,
            false,
            &cancel
        )
        .is_err()
    );
    assert!(
        restore::extract(
            &f.config,
            &f.archive(&s),
            &output,
            &[],
            None,
            false,
            &cancel
        )
        .is_err()
    );
    assert!(
        restore::extract(
            &f.config,
            &f.archive(&s),
            &f.root().join("bad"),
            &["../escape".into()],
            None,
            false,
            &cancel
        )
        .is_err()
    );
    assert!(!f.root().join("bad").exists());
    assert!(restore::safe_relative("/absolute").is_err());
    assert!(restore::safe_relative("a/../../escape").is_err());
    assert!(restore::safe_relative("a//b").is_err());
    assert!(restore::safe_relative("a/./b").is_err());
}
#[test]
fn safe_restore_rejects_symlink_chain_escape() {
    let mut f = Fixture::new();
    f.config.targets[0].symlink_policy = "preserve".into();
    fs::create_dir(f.source().join("d")).unwrap();
    std::os::unix::fs::symlink("..", f.source().join("d/up")).unwrap();
    std::os::unix::fs::symlink("up/../outside", f.source().join("d/exploit")).unwrap();
    let instance = f.instance();
    let s = f.capture(&instance);
    let output = f.root().join("restore");
    assert!(
        restore::extract(
            &f.config,
            &f.archive(&s),
            &output,
            &[],
            None,
            true,
            &AtomicBool::new(false)
        )
        .is_err()
    );
    assert!(!output.join("d/up").exists());
    assert!(!output.join("d/exploit").exists());
}
#[test]
fn unchanged_requires_protected_captures_and_hashes_content_even_with_equal_mtime() {
    let mut f = Fixture::new();
    f.config.targets[0].skip_unchanged = true;
    f.config.targets[0].max_capture_age_seconds = Some(3600);
    fs::write(f.source().join("data"), b"original").unwrap();
    let instance = f.instance();
    let _first = f.capture(&instance);
    let latest = f.capture(&instance);
    assert_eq!(instance.state.catalog().unwrap().len(), 2);
    let unchanged = f.capture(&instance);
    assert_eq!(unchanged.job_id, latest.job_id);
    assert_eq!(instance.state.catalog().unwrap().len(), 2);
    let before = fs::metadata(f.source().join("data"))
        .unwrap()
        .modified()
        .unwrap();
    fs::write(f.source().join("data"), b"modified").unwrap();
    fs::File::open(f.source().join("data"))
        .unwrap()
        .set_times(fs::FileTimes::new().set_modified(before))
        .unwrap();
    let changed = f.capture(&instance);
    assert_ne!(changed.job_id, latest.job_id);
    assert_eq!(instance.state.catalog().unwrap().len(), 3);
}
#[test]
fn mixed_compression_keeps_independent_verified_zips() {
    let mut f = Fixture::new();
    f.config.targets[0].archive.store_extensions = vec![".jpg".into()];
    let text = b"repeated text ".repeat(1024);
    fs::write(f.source().join("photo.jpg"), &text).unwrap();
    fs::write(f.source().join("notes.txt"), &text).unwrap();
    let instance = f.instance();
    let s = f.capture(&instance);
    let mut zip = zip::ZipArchive::new(fs::File::open(f.archive(&s)).unwrap()).unwrap();
    assert_eq!(
        zip.by_name("data/photo.jpg").unwrap().compression(),
        zip::CompressionMethod::Stored
    );
    assert_eq!(
        zip.by_name("data/notes.txt").unwrap().compression(),
        zip::CompressionMethod::Deflated
    );
}
#[test]
fn reviewed_transition_waits_then_resumes_interrupted_deletion_without_rewriting_manifests() {
    let mut f = Fixture::new();
    fs::write(f.source().join("data"), b"original").unwrap();
    let instance = f.instance();
    let old = f.capture(&instance);
    let from = old.policy_id.clone();
    f.config.targets[0].retention.max_snapshots = 3;
    let one = f.capture(&instance);
    assert!(migration::plan(&f.config, "test", &from, 1).is_err());
    let two = f.capture(&instance);
    let before = fs::read(f.archive(&two)).unwrap();
    let plan = migration::plan(&f.config, "test", &from, 1).unwrap();
    let protected = migration::apply(
        instance.state.as_ref(),
        &f.config,
        &plan,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(protected["status"], "protected");
    assert!(f.archive(&old).exists());
    std::thread::sleep(Duration::from_millis(1100));
    instance.state.mark_deleting(&old.job_id).unwrap();
    fs::remove_file(f.archive(&old)).unwrap();
    let result = migration::apply(
        instance.state.as_ref(),
        &f.config,
        &plan,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(result["status"], "completed");
    assert_eq!(instance.state.catalog().unwrap().len(), 2);
    assert!(f.archive(&one).exists());
    assert_eq!(fs::read(f.archive(&two)).unwrap(), before);
    assert_eq!(
        migration::apply(
            instance.state.as_ref(),
            &f.config,
            &plan,
            &AtomicBool::new(false)
        )
        .unwrap()["status"],
        "completed"
    );
}
#[test]
fn corrupt_replacement_cannot_retire_a_previous_policy() {
    let mut f = Fixture::new();
    fs::write(f.source().join("data"), b"original").unwrap();
    let instance = f.instance();
    let old = f.capture(&instance);
    f.config.targets[0].retention.max_snapshots = 3;
    let replacement = f.capture(&instance);
    let _ = f.capture(&instance);
    let mut plan = migration::plan(&f.config, "test", &old.policy_id, 1).unwrap();
    plan.created_ms -= 2000;
    plan.not_before_ms -= 2000;
    fs::write(f.archive(&replacement), b"damaged").unwrap();
    assert!(
        migration::apply(
            instance.state.as_ref(),
            &f.config,
            &plan,
            &AtomicBool::new(false)
        )
        .is_err()
    );
    assert!(f.archive(&old).exists());
    assert_eq!(instance.state.catalog().unwrap().len(), 3);
}
#[test]
fn reporting_redacts_credentials_and_separates_capture_from_source_checks() {
    let mut f = Fixture::new();
    f.config.targets[0].hooks.before_backup=serde_json::from_value(json!([{"name":"secret","command":["/usr/bin/true","secret-token"],"environment":{"TOKEN":"secret-token"}}])).unwrap();
    f.config.targets[0].max_capture_age_seconds = Some(1);
    let instance = f.instance();
    let _ = f.capture(&instance);
    instance
        .state
        .record_source_check(&f.config.targets[0], None)
        .unwrap();
    let report = instance.state.status(&f.config).unwrap();
    assert!(!report.to_string().contains("secret-token"));
    assert_eq!(report["schema_version"], 1);
    assert!(report["targets"][0]["last_capture_ms"].is_number());
    assert!(report["targets"][0]["last_source_check_ms"].is_number());
    assert_eq!(report["targets"][0]["duration_sample_count"], 1);
}
#[test]
fn rehearsal_records_a_verified_sample_separately_without_creating_an_archive() {
    let mut f = Fixture::new();
    f.config.rehearsal.scratch_dir = Some(f.root().join("scratch"));
    f.config.rehearsal.sample_files = Some(1);
    fs::write(f.source().join("a"), b"restore me").unwrap();
    fs::write(f.source().join("b"), b"restore me too").unwrap();
    let instance = f.instance();
    let _ = f.capture(&instance);
    let report =
        restore::rehearse(instance.state.as_ref(), &f.config, &AtomicBool::new(false)).unwrap();
    assert_eq!(report["healthy"], true);
    assert_eq!(report["results"][0]["file_count"], 1);
    assert_eq!(instance.state.catalog().unwrap().len(), 1);
    assert_eq!(
        instance
            .state
            .inspections("rehearsal_summary")
            .unwrap()
            .len(),
        1
    );
    assert!(
        fs::read_dir(f.config.rehearsal.scratch_dir.as_ref().unwrap())
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn an_integrity_incident_blocks_later_cleanup_until_review_and_never_deletes_the_bad_copy() {
    let mut f = Fixture::new();
    f.config.targets[0].retention.max_snapshots = 2;
    fs::write(f.source().join("data"), b"safe").unwrap();
    let instance = f.instance();
    for _ in 0..4 {
        f.capture(&instance);
    }
    let bad = f.capture(&instance);
    fs::write(f.archive(&bad), b"corrupt").unwrap();
    syncthing_backup_tool::retention::sweep(
        instance.state.as_ref(),
        &f.config,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(instance.state.catalog().unwrap().len(), 5);
    syncthing_backup_tool::retention::sweep(
        instance.state.as_ref(),
        &f.config,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(instance.state.catalog().unwrap().len(), 5);
    let incident = integrity::unresolved(instance.state.as_ref())
        .unwrap()
        .remove(0);
    integrity::resolve(
        instance.state.as_ref(),
        incident["id"].as_str().unwrap(),
        "Reviewed preserved damage; retain suspect copy and reverify all healthy survivors",
    )
    .unwrap();
    syncthing_backup_tool::retention::sweep(
        instance.state.as_ref(),
        &f.config,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(instance.state.catalog().unwrap().len(), 3);
    assert!(f.archive(&bad).exists());
    assert_eq!(
        instance
            .state
            .catalog()
            .unwrap()
            .iter()
            .filter(|(_, h, _)| *h)
            .count(),
        2
    );
    assert_eq!(
        instance
            .state
            .inspections("integrity_incident")
            .unwrap()
            .len(),
        1
    );
}
#[test]
fn schema_two_upgrade_preserves_the_deadline_catalog_and_old_zip() {
    let f = Fixture::new();
    fs::write(f.source().join("data"), b"legacy").unwrap();
    let instance = f.instance();
    let s = f.capture(&instance);
    let before = fs::read(f.archive(&s)).unwrap();
    let due = instance.state.due("test").unwrap();
    drop(instance);
    let db = rusqlite::Connection::open(f.config.state_dir.join("state.sqlite3")).unwrap();
    db.execute_batch("ALTER TABLE schedules DROP COLUMN signature; DROP TABLE io_activity; DROP TABLE inspections; DROP TABLE source_checks; DROP TABLE job_timings; PRAGMA user_version=2;").unwrap();
    let mut value: serde_json::Value = serde_json::from_str(
        &db.query_row("SELECT info FROM snapshots", [], |r| r.get::<_, String>(0))
            .unwrap(),
    )
    .unwrap();
    value.as_object_mut().unwrap().remove("selected_bytes");
    for field in [
        "manual_only",
        "interval_anchor",
        "include_paths",
        "cache",
        "skip_unchanged",
        "max_capture_age_seconds",
    ] {
        value["target"].as_object_mut().unwrap().remove(field);
    }
    value["target"]["archive"]
        .as_object_mut()
        .unwrap()
        .remove("store_extensions");
    value["target"]["archive"]
        .as_object_mut()
        .unwrap()
        .remove("reopened_verification");
    value["target"]["storage"]
        .as_object_mut()
        .unwrap()
        .remove("cooldown_seconds");
    db.execute("UPDATE snapshots SET info=?1", [value.to_string()])
        .unwrap();
    drop(db);
    let instance = f.instance();
    assert_eq!(instance.state.due("test").unwrap(), due);
    assert_eq!(instance.state.catalog().unwrap().len(), 1);
    assert_eq!(fs::read(f.archive(&s)).unwrap(), before);
    let d = syncthing_backup_tool::storage::Destination::open(
        &f.config.targets[0],
        false,
        &f.config.state_dir,
    )
    .unwrap();
    syncthing_backup_tool::archive::verify_snapshot(
        &d,
        &instance.state.catalog().unwrap()[0].0,
        &f.config.resources,
        &AtomicBool::new(false),
    )
    .unwrap();
}
