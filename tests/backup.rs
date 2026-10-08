use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::AtomicBool,
};
use syncthing_backup_tool::{
    archive,
    config::{Config, Resources},
    daemon::{self, Instance},
    domain::{Job, JobSpec},
    retention,
    storage::Destination,
};

struct Fixture {
    _temp: tempfile::TempDir,
    config: Config,
}
impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path();
        fs::create_dir(root.join("source")).unwrap();
        let config: Config = serde_json::from_value(serde_json::json!({
        "config_version":1,"state_dir":root.join("state"),"targets":[{
            "id":"test","source_dir":root.join("source"),"destination_dir":root.join("backups"),
            "storage":{"min_free_bytes":0},"retention":{"min_snapshots":1,"max_snapshots":3}
        }]}))
        .unwrap();
        config.validate().unwrap();
        Self {
            _temp: temp,
            config,
        }
    }
    fn source(&self) -> &Path {
        &self.config.targets[0].source_dir
    }
    fn destination(&self) -> &Path {
        &self.config.targets[0].destination_dir
    }
    fn instance(&self) -> Instance {
        let instance = Instance::open(&self.config).unwrap();
        instance
            .state
            .sync_schedules(&self.config, chrono::Utc::now().timestamp_millis())
            .unwrap();
        instance
    }
    fn job(&self, instance: &Instance) -> Job {
        let spec = JobSpec {
            backends: self.config.backends.clone(),
            target: self.config.targets[0].clone(),
            resources: self.config.resources.clone(),
        };
        let id = instance.state.enqueue(&spec, 0).unwrap();
        instance.state.start(&id).unwrap();
        Job {
            id,
            spec,
            attempts: 1,
        }
    }
    fn backup(&self, instance: &Instance) -> syncthing_backup_tool::domain::Snapshot {
        syncthing_backup_tool::snapshot::create(
            &self.job(instance),
            instance.state.as_ref(),
            &self.config.state_dir,
            &AtomicBool::new(false),
        )
        .unwrap()
    }
    fn zips(&self) -> Vec<PathBuf> {
        fs::read_dir(self.destination())
            .unwrap()
            .map(|r| r.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "zip"))
            .collect()
    }
}

#[test]
fn roundtrip_exclusions_empty_directories_and_manifest() {
    let mut f = Fixture::new();
    fs::write(f.source().join("hello.txt"), b"original bytes\n").unwrap();
    fs::create_dir(f.source().join("empty")).unwrap();
    fs::create_dir(f.source().join("omit")).unwrap();
    fs::write(f.source().join("omit/secret"), b"excluded").unwrap();
    f.config.targets[0].exclude_globs = vec!["omit/**".into()];
    let instance = f.instance();
    let snapshot = f.backup(&instance);
    let mut zip =
        zip::ZipArchive::new(fs::File::open(f.destination().join(&snapshot.filename)).unwrap())
            .unwrap();
    let mut data = Vec::new();
    zip.by_name("data/hello.txt")
        .unwrap()
        .read_to_end(&mut data)
        .unwrap();
    assert_eq!(data, b"original bytes\n");
    assert!(zip.by_name("data/empty/").unwrap().is_dir());
    assert!(zip.by_name("data/omit/secret").is_err());
    assert!(zip.by_name("data/omit/").is_err());
    let manifest = archive::verify(
        fs::File::open(f.destination().join(&snapshot.filename)).unwrap(),
        &f.config.resources,
        Some(&snapshot.job_id),
        Some(&f.config.targets[0]),
        &AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(manifest.consistency, "live");
    assert_eq!(manifest.entries.len(), 3);
    assert!(instance.state.jobs(None).unwrap().is_empty());
    assert_eq!(fs::read(f.source().join("hello.txt")).unwrap(), data);
}

#[test]
fn symlinks_fail_without_publishing_or_deleting_good_copy() {
    let f = Fixture::new();
    fs::write(f.source().join("file"), b"good").unwrap();
    let instance = f.instance();
    f.backup(&instance);
    std::os::unix::fs::symlink("/etc/passwd", f.source().join("link")).unwrap();
    let job = f.job(&instance);
    let error = syncthing_backup_tool::snapshot::create(
        &job,
        instance.state.as_ref(),
        &f.config.state_dir,
        &AtomicBool::new(false),
    )
    .unwrap_err();
    assert!(error.is::<syncthing_backup_tool::domain::Permanent>());
    assert_eq!(f.zips().len(), 1);
    assert!(
        fs::read_dir(f.destination().join(".partial"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn explicit_symlink_skip_is_recorded_without_following() {
    let mut f = Fixture::new();
    f.config.targets[0].symlink_policy = "skip".into();
    std::os::unix::fs::symlink("/etc/passwd", f.source().join("link")).unwrap();
    let instance = f.instance();
    let snapshot = f.backup(&instance);
    let manifest = archive::verify(
        fs::File::open(f.destination().join(snapshot.filename)).unwrap(),
        &f.config.resources,
        None,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();
    assert!(
        manifest
            .entries
            .iter()
            .any(|e| e.path == "link" && e.kind == "skipped_symlink")
    );
}

#[test]
fn archive_limit_and_cancellation_leave_no_final_files() {
    let mut f = Fixture::new();
    fs::write(f.source().join("file"), b"hello").unwrap();
    f.config.targets[0].archive.max_archive_bytes = 64;
    let instance = f.instance();
    let job = f.job(&instance);
    assert!(
        syncthing_backup_tool::snapshot::create(
            &job,
            instance.state.as_ref(),
            &f.config.state_dir,
            &AtomicBool::new(false)
        )
        .is_err()
    );
    instance
        .state
        .failed(&job, None, "expected limit failure")
        .unwrap();
    assert!(f.zips().is_empty());
    f.config.targets[0].archive.max_archive_bytes = 1024 * 1024;
    let job = f.job(&instance);
    assert!(
        syncthing_backup_tool::snapshot::create(
            &job,
            instance.state.as_ref(),
            &f.config.state_dir,
            &AtomicBool::new(true)
        )
        .is_err()
    );
    assert!(f.zips().is_empty());
}

#[test]
fn metadata_memory_and_entry_limits_are_enforced() {
    let mut f = Fixture::new();
    for i in 0..600 {
        fs::write(f.source().join(format!("file-{i}")), b"").unwrap();
    }
    f.config.resources = Resources {
        memory_budget_bytes: 20 * 1024 * 1024,
        io_buffer_bytes: 65536,
        ..Resources::default()
    };
    let instance = f.instance();
    let job = f.job(&instance);
    assert!(
        syncthing_backup_tool::snapshot::create(
            &job,
            instance.state.as_ref(),
            &f.config.state_dir,
            &AtomicBool::new(false)
        )
        .unwrap_err()
        .is::<syncthing_backup_tool::domain::Permanent>()
    );
    assert!(f.zips().is_empty());
}

#[test]
fn count_retention_preserves_unknown_files_and_sources() {
    let f = Fixture::new();
    fs::write(f.source().join("file"), b"source").unwrap();
    let instance = f.instance();
    for _ in 0..5 {
        f.backup(&instance);
    }
    fs::write(f.destination().join("unrelated.zip"), b"not owned").unwrap();
    retention::sweep(instance.state.as_ref(), &f.config, &AtomicBool::new(false)).unwrap();
    assert_eq!(instance.state.catalog().unwrap().len(), 3);
    assert_eq!(f.zips().len(), 4);
    assert_eq!(fs::read(f.source().join("file")).unwrap(), b"source");
    assert_eq!(
        fs::read(f.destination().join("unrelated.zip")).unwrap(),
        b"not owned"
    );
}

#[test]
fn changed_retention_applies_only_to_new_cohort() {
    let mut f = Fixture::new();
    fs::write(f.source().join("file"), b"source").unwrap();
    let instance = f.instance();
    for _ in 0..4 {
        f.backup(&instance);
    }
    f.config.targets[0].retention.max_snapshots = 1;
    for _ in 0..2 {
        f.backup(&instance);
    }
    retention::sweep(instance.state.as_ref(), &f.config, &AtomicBool::new(false)).unwrap();
    assert_eq!(
        f.zips().len(),
        4,
        "old policy retains three and new policy retains one"
    );
    assert_eq!(
        instance
            .state
            .catalog()
            .unwrap()
            .iter()
            .filter(|(s, _, _)| s.target.retention.max_snapshots == 3)
            .count(),
        3
    );
}

#[test]
fn corrupt_newest_snapshot_does_not_destroy_last_good_copy() {
    let mut f = Fixture::new();
    f.config.targets[0].retention.min_snapshots = 2;
    f.config.targets[0].retention.max_snapshots = 2;
    let instance = f.instance();
    let _ = f.backup(&instance);
    let _ = f.backup(&instance);
    let newest = f.backup(&instance);
    fs::write(f.destination().join(newest.filename), b"corrupt").unwrap();
    retention::sweep(instance.state.as_ref(), &f.config, &AtomicBool::new(false)).unwrap();
    assert_eq!(f.zips().len(), 3);
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
}

#[test]
fn minimum_precedes_age_and_byte_limits() {
    let mut f = Fixture::new();
    f.config.targets[0].retention.min_snapshots = 2;
    f.config.targets[0].retention.max_age_seconds = Some(1);
    f.config.targets[0].retention.max_total_bytes = Some(1);
    let instance = f.instance();
    let snapshots: Vec<_> = (0..4).map(|_| f.backup(&instance)).collect();
    let plan = retention::plan(snapshots, &f.config.targets[0].retention, i64::MAX);
    assert_eq!(plan.len(), 2);
}

#[test]
fn unowned_destination_and_different_state_directory_are_rejected() {
    let f = Fixture::new();
    fs::create_dir(f.destination()).unwrap();
    fs::write(f.destination().join("existing"), b"preserve").unwrap();
    let instance = f.instance();
    assert!(Destination::open(&f.config.targets[0], true, &f.config.state_dir).is_err());
    fs::remove_file(f.destination().join("existing")).unwrap();
    f.backup(&instance);
    let other = f._temp.path().join("other-state");
    fs::create_dir(&other).unwrap();
    assert!(Destination::open(&f.config.targets[0], true, &other).is_err());
}

#[test]
fn crash_after_rename_recovers_exactly_one_snapshot() {
    let f = Fixture::new();
    fs::write(f.source().join("file"), b"safe").unwrap();
    let instance = f.instance();
    let job = f.job(&instance);
    let snapshot = syncthing_backup_tool::snapshot::create(
        &job,
        instance.state.as_ref(),
        &f.config.state_dir,
        &AtomicBool::new(false),
    )
    .unwrap();
    let db = rusqlite::Connection::open(f.config.state_dir.join("state.sqlite3")).unwrap();
    db.execute("DELETE FROM snapshots", []).unwrap();
    db.execute("INSERT INTO jobs(id,target_id,spec,status,attempts,next_at,intention) VALUES(?1,?2,?3,'publishing',1,0,?4)",
        rusqlite::params![job.id,job.spec.target.id,serde_json::to_string(&job.spec).unwrap(),serde_json::to_string(&snapshot).unwrap()]).unwrap();
    drop(db);
    drop(instance);
    let instance = f.instance();
    daemon::recover(&instance, &f.config).unwrap();
    assert!(instance.state.jobs(None).unwrap().is_empty());
    assert_eq!(instance.state.catalog().unwrap().len(), 1);
    assert_eq!(f.zips().len(), 1);
}

#[test]
fn crash_during_write_removes_only_identified_temporary_file() {
    let f = Fixture::new();
    let instance = f.instance();
    let job = f.job(&instance);
    let destination = Destination::open(&f.config.targets[0], true, &f.config.state_dir).unwrap();
    destination
        .temporary(&job.id)
        .unwrap()
        .write_all(b"incomplete")
        .unwrap();
    drop(destination);
    fs::write(
        f.destination().join(".partial/unknown.zip.part"),
        b"preserve",
    )
    .unwrap();
    let tree = f.destination().join(format!(".partial/{}.tree", job.id));
    fs::create_dir(&tree).unwrap();
    fs::write(tree.join("ziunfinished"), b"interrupted Info-ZIP output").unwrap();
    let unknown = f.destination().join(".partial/unknown.tree");
    fs::create_dir(&unknown).unwrap();
    fs::write(unknown.join("ziunknown"), b"preserve").unwrap();
    daemon::recover(&instance, &f.config).unwrap();
    assert!(
        !f.destination()
            .join(format!(".partial/{}.zip.part", job.id))
            .exists()
    );
    assert!(f.destination().join(".partial/unknown.zip.part").exists());
    assert!(!tree.exists());
    assert!(unknown.join("ziunknown").exists());
    assert_eq!(instance.state.pending_count().unwrap(), 1);
}

#[test]
fn audit_and_rotation_paths_cannot_overlap_state_or_target_data() {
    let mut f = Fixture::new();
    f.config.logging.audit_file = Some(f.config.state_dir.join("state.sqlite3"));
    assert!(f.config.validate().is_err());
    f.config.logging.audit_file = Some(f._temp.path().join("audit.jsonl"));
    f.config.targets[0].source_dir = f._temp.path().join("audit.jsonl.1");
    assert!(f.config.validate().is_err());
    f.config.targets[0].source_dir = f._temp.path().join("source");
    f.config.targets[0].destination_dir = f._temp.path().join("audit.jsonl.10");
    assert!(f.config.validate().is_err());
}

#[test]
fn audit_cannot_write_to_removed_target_archives_or_in_flight_sources() {
    let f = Fixture::new();
    fs::write(f.source().join("file"), b"protected").unwrap();
    let instance = f.instance();
    let snapshot = f.backup(&instance);
    let archive = f.destination().join(snapshot.filename);
    let original = fs::read(&archive).unwrap();
    let mut candidate = f.config.clone();
    candidate.targets.clear();
    candidate.logging.audit_file = Some(archive.clone());
    candidate.validate().unwrap();
    assert!(daemon::validate_history(&candidate, instance.state.as_ref()).is_err());
    drop(instance);
    let mut status = std::process::Command::new(env!("CARGO_BIN_EXE_syncthing-backup-tool"));
    status
        .arg("--config")
        .arg(f._temp.path().join("unsafe.json"))
        .arg("retain");
    fs::write(
        f._temp.path().join("unsafe.json"),
        serde_json::to_vec(&candidate).unwrap(),
    )
    .unwrap();
    let output = status.output().unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read(&archive).unwrap(), original);
    let instance = f.instance();
    let job = f.job(&instance);
    candidate.logging.audit_file = Some(job.spec.target.source_dir.join("audit.jsonl"));
    assert!(daemon::validate_history(&candidate, instance.state.as_ref()).is_err());
}

#[test]
fn configuration_rejects_alias_overlap_unknown_fields_and_invalid_limits() {
    let mut f = Fixture::new();
    let mut value = serde_json::to_value(&f.config).unwrap();
    value["typo"] = true.into();
    assert!(serde_json::from_value::<Config>(value).is_err());
    let alias = f._temp.path().join("alias");
    std::os::unix::fs::symlink(f.source(), &alias).unwrap();
    f.config.targets[0].destination_dir = alias.join("nested");
    assert!(f.config.validate().is_err());
    f.config.targets[0].destination_dir = f._temp.path().join("backups");
    f.config.resources.max_concurrent_snapshots = 3;
    assert!(f.config.validate().is_err());
}

#[test]
fn missing_mount_never_creates_destination() {
    let mut f = Fixture::new();
    let nonexistent_mount = f._temp.path().join("hdd");
    f.config.targets[0].destination_dir = nonexistent_mount.join("backups");
    f.config.targets[0].required_destination_mount = Some(nonexistent_mount);
    let instance = f.instance();
    let job = f.job(&instance);
    assert!(
        syncthing_backup_tool::snapshot::create(
            &job,
            instance.state.as_ref(),
            &f.config.state_dir,
            &AtomicBool::new(false)
        )
        .is_err()
    );
    assert!(!f.destination().exists());
}

#[test]
fn coordinator_accepts_independently_supplied_copy_and_archive_modules() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use syncthing_backup_tool::api::{
        Archiver, CopyRequest, PackRequest, Synchronizer, TestRequest,
    };
    struct CopyModule(Arc<AtomicUsize>);
    impl Synchronizer for CopyModule {
        fn copy(&self, request: CopyRequest<'_>) -> anyhow::Result<()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            syncthing_backup_tool::source::Rsync.copy(request)
        }
    }
    struct ArchiveModule(Arc<AtomicUsize>);
    impl Archiver for ArchiveModule {
        fn pack(&self, request: PackRequest<'_>) -> anyhow::Result<()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            syncthing_backup_tool::archive::InfoZip.pack(request)
        }
        fn test(&self, request: TestRequest<'_>) -> anyhow::Result<()> {
            self.0.fetch_add(1, Ordering::Relaxed);
            syncthing_backup_tool::archive::InfoZip.test(request)
        }
    }
    let f = Fixture::new();
    fs::write(f.source().join("data"), b"backend contract").unwrap();
    let instance = f.instance();
    let job = f.job(&instance);
    let copied = Arc::new(AtomicUsize::new(0));
    let packed = Arc::new(AtomicUsize::new(0));
    let mut modules =
        syncthing_backup_tool::backends::Modules::from_choices(&f.config.backends).unwrap();
    modules.synchronizer = Arc::new(CopyModule(Arc::clone(&copied)));
    modules.archiver = Arc::new(ArchiveModule(Arc::clone(&packed)));
    syncthing_backup_tool::snapshot::create_with(
        &job,
        instance.state.as_ref(),
        &f.config.state_dir,
        &AtomicBool::new(false),
        &modules,
    )
    .unwrap();
    assert_eq!(copied.load(Ordering::Relaxed), 1);
    assert_eq!(packed.load(Ordering::Relaxed), 2);
    assert_eq!(f.zips().len(), 1);
    assert!(
        fs::read_dir(f.destination().join(".partial"))
            .unwrap()
            .next()
            .is_none()
    );
}

#[test]
fn interrupted_deletion_rechecks_survivors_before_removing_a_good_copy() {
    let mut f = Fixture::new();
    f.config.targets[0].retention.min_snapshots = 2;
    f.config.targets[0].retention.max_snapshots = 2;
    let instance = f.instance();
    let oldest = f.backup(&instance);
    let _ = f.backup(&instance);
    let newest = f.backup(&instance);
    instance.state.mark_deleting(&oldest.job_id).unwrap();
    fs::write(
        f.destination().join(&newest.filename),
        b"damage after deletion intention",
    )
    .unwrap();
    retention::sweep(instance.state.as_ref(), &f.config, &AtomicBool::new(false)).unwrap();
    assert!(f.destination().join(oldest.filename).exists());
    assert_eq!(f.zips().len(), 3);
}

#[test]
fn source_changes_during_tool_copy_fail_without_publishing() {
    use std::sync::{Arc, atomic::Ordering};
    let f = Fixture::new();
    let path = f.source().join("changing");
    fs::File::create(&path)
        .unwrap()
        .set_len(32 * 1024 * 1024)
        .unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let flag = Arc::clone(&stop);
    let writer = std::thread::spawn(move || {
        use std::os::unix::fs::FileExt;
        let file = fs::OpenOptions::new().write(true).open(path).unwrap();
        let mut value = 0u8;
        while !flag.load(Ordering::Relaxed) {
            file.write_at(&[value], 0).unwrap();
            value = value.wrapping_add(1);
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });
    let instance = f.instance();
    let job = f.job(&instance);
    let result = syncthing_backup_tool::snapshot::create(
        &job,
        instance.state.as_ref(),
        &f.config.state_dir,
        &AtomicBool::new(false),
    );
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    assert!(result.is_err());
    assert!(f.zips().is_empty());
}

fn hook(
    path: &Path,
    name: &str,
    body: &str,
    on_error: &str,
) -> syncthing_backup_tool::config::Hook {
    use std::os::unix::fs::PermissionsExt;
    fs::write(path, format!("#!/bin/sh\nset -eu\n{body}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
    serde_json::from_value(
        serde_json::json!({"name":name,"command":[path],"timeout_seconds":2,"on_error":on_error}),
    )
    .unwrap()
}
#[test]
fn hooks_gate_copy_and_always_cleanup_a_failed_save() {
    let mut f = Fixture::new();
    let prepare = f._temp.path().join("prepare.sh");
    let resume = f._temp.path().join("resume.sh");
    f.config.targets[0]
        .hooks
        .before_backup
        .push(hook(&prepare, "save", "exit 7", "skip_backup"));
    f.config.targets[0].hooks.finally.push(hook(
        &resume,
        "resume",
        "touch \"$BACKUP_SOURCE/resumed\"",
        "fail_job",
    ));
    let instance = f.instance();
    let job = f.job(&instance);
    let error = syncthing_backup_tool::snapshot::create(
        &job,
        instance.state.as_ref(),
        &f.config.state_dir,
        &AtomicBool::new(false),
    )
    .unwrap_err();
    assert!(error.is::<syncthing_backup_tool::domain::Skipped>());
    assert!(f.source().join("resumed").exists());
    assert!(f.zips().is_empty());
    assert_eq!(
        instance.state.job_status(&job.id).unwrap().unwrap().status,
        "skipped"
    );
    assert!(!instance.state.cleanup_pending("test").unwrap());
}
#[test]
fn successful_prepare_precedes_inventory_and_after_hook_error_preserves_archive() {
    let mut f = Fixture::new();
    let prepare = f._temp.path().join("prepare.sh");
    let after = f._temp.path().join("after.sh");
    f.config.targets[0].hooks.before_backup.push(hook(
        &prepare,
        "save",
        "echo saved > \"$BACKUP_SOURCE/data\"",
        "fail_job",
    ));
    f.config.targets[0]
        .hooks
        .after_backup
        .push(hook(&after, "notify", "exit 4", "fail_job"));
    let instance = f.instance();
    let job = f.job(&instance);
    assert!(
        syncthing_backup_tool::snapshot::create(
            &job,
            instance.state.as_ref(),
            &f.config.state_dir,
            &AtomicBool::new(false)
        )
        .is_err()
    );
    assert_eq!(f.zips().len(), 1);
    assert_eq!(
        instance.state.job_status(&job.id).unwrap().unwrap().status,
        "completed_with_hook_failure"
    );
    let mut zip = zip::ZipArchive::new(fs::File::open(&f.zips()[0]).unwrap()).unwrap();
    let mut data = String::new();
    zip.by_name("data/data")
        .unwrap()
        .read_to_string(&mut data)
        .unwrap();
    assert_eq!(data, "saved\n");
}
#[test]
fn failed_cleanup_is_durable_blocks_target_and_can_be_recovered() {
    let mut f = Fixture::new();
    let prepare = f._temp.path().join("prepare.sh");
    let resume = f._temp.path().join("resume.sh");
    f.config.targets[0]
        .hooks
        .before_backup
        .push(hook(&prepare, "save", "exit 1", "skip_backup"));
    f.config.targets[0]
        .hooks
        .finally
        .push(hook(&resume, "resume", "exit 1", "fail_job"));
    let instance = f.instance();
    let job = f.job(&instance);
    assert!(
        syncthing_backup_tool::snapshot::create(
            &job,
            instance.state.as_ref(),
            &f.config.state_dir,
            &AtomicBool::new(false)
        )
        .is_err()
    );
    assert!(instance.state.cleanup_pending("test").unwrap());
    assert_eq!(
        instance.state.job_status(&job.id).unwrap().unwrap().status,
        "cleanup_failed"
    );
    fs::write(&resume, "#!/bin/sh\nexit 0\n").unwrap();
    syncthing_backup_tool::hooks::recover(instance.state.as_ref()).unwrap();
    assert!(!instance.state.cleanup_pending("test").unwrap());
}
#[test]
fn hook_timeout_skips_backup_without_leaking_a_running_process() {
    let mut f = Fixture::new();
    let prepare = f._temp.path().join("prepare.sh");
    let mut h = hook(&prepare, "save", "sleep 30", "skip_backup");
    h.timeout_seconds = 1;
    f.config.targets[0].hooks.before_backup.push(h);
    let instance = f.instance();
    let job = f.job(&instance);
    let start = std::time::Instant::now();
    assert!(
        syncthing_backup_tool::snapshot::create(
            &job,
            instance.state.as_ref(),
            &f.config.state_dir,
            &AtomicBool::new(false)
        )
        .unwrap_err()
        .is::<syncthing_backup_tool::domain::Skipped>()
    );
    assert!(start.elapsed() < std::time::Duration::from_secs(5));
    assert!(f.zips().is_empty());
}
#[test]
fn preserve_symlinks_without_reading_their_targets() {
    let mut f = Fixture::new();
    f.config.targets[0].symlink_policy = "preserve".into();
    std::os::unix::fs::symlink("/a/missing/external/file", f.source().join("link")).unwrap();
    let instance = f.instance();
    let snapshot = f.backup(&instance);
    let manifest = archive::verify(
        fs::File::open(f.destination().join(snapshot.filename)).unwrap(),
        &f.config.resources,
        None,
        None,
        &AtomicBool::new(false),
    )
    .unwrap();
    let link = manifest.entries.iter().find(|e| e.path == "link").unwrap();
    assert_eq!(link.kind, "symlink");
    assert_eq!(
        link.symlink_target.as_deref(),
        Some("/a/missing/external/file")
    );
}
#[test]
fn restart_does_not_run_cleanup_for_an_active_job() {
    let mut f = Fixture::new();
    let resume = f._temp.path().join("resume.sh");
    f.config.targets[0].hooks.finally.push(hook(
        &resume,
        "resume",
        "touch \"$BACKUP_SOURCE/resumed\"",
        "fail_job",
    ));
    let instance = f.instance();
    let job = f.job(&instance);
    instance.state.register_cleanup(&job).unwrap();
    syncthing_backup_tool::hooks::recover_except(
        instance.state.as_ref(),
        &std::collections::HashSet::from([job.id.clone()]),
    )
    .unwrap();
    assert!(!f.source().join("resumed").exists());
    assert!(instance.state.cleanup_pending("test").unwrap());
    syncthing_backup_tool::hooks::recover(instance.state.as_ref()).unwrap();
    assert!(f.source().join("resumed").exists());
}

#[test]
fn component_open_fallback_rejects_parent_escape_and_links() {
    let f = Fixture::new();
    fs::create_dir(f.source().join("directory")).unwrap();
    fs::write(f.source().join("directory/file"), b"safe").unwrap();
    let root = fs::File::open(f.source()).unwrap();
    let file = syncthing_backup_tool::storage::open_components(
        &root,
        Path::new("directory/file"),
        rustix::fs::OFlags::RDONLY,
    )
    .unwrap();
    assert_eq!(file.metadata().unwrap().len(), 4);
    assert!(
        syncthing_backup_tool::storage::open_components(
            &root,
            Path::new("../outside"),
            rustix::fs::OFlags::RDONLY
        )
        .is_err()
    );
    std::os::unix::fs::symlink("directory", f.source().join("alias")).unwrap();
    assert!(
        syncthing_backup_tool::storage::open_components(
            &root,
            Path::new("alias/file"),
            rustix::fs::OFlags::RDONLY
        )
        .is_err()
    );
}
