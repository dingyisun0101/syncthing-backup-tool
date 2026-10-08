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
    daemon::recover(&instance, &f.config).unwrap();
    assert!(
        !f.destination()
            .join(format!(".partial/{}.zip.part", job.id))
            .exists()
    );
    assert!(f.destination().join(".partial/unknown.zip.part").exists());
    assert_eq!(instance.state.pending_count().unwrap(), 1);
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
