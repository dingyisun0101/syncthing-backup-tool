use fs2::FileExt;
use serde_json::{Value, json};
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use syncthing_backup_tool::daemon;

struct Service {
    child: Child,
}
impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn config(root: &Path) -> Value {
    json!({"config_version":1,"state_dir":root.join("state"),"shutdown_grace_seconds":1,
    "retention":{"sweep_interval_seconds":3600},"targets":[{
        "id":"test","source_dir":root.join("source"),"destination_dir":root.join("backups"),
        "run_on_startup":false,"backup_interval_seconds":3600,"storage":{"min_free_bytes":0}
    }]})
}
fn start(root: &Path) -> Service {
    let log = fs::File::create(root.join("service.log")).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_syncthing-backup-tool"))
        .arg("--config")
        .arg(root.join("config.json"))
        .arg("--control-socket")
        .arg(root.join("control.sock"))
        .arg("run")
        .stdout(Stdio::null())
        .stderr(log)
        .spawn()
        .unwrap();
    let service = Service { child };
    until(|| daemon::request(&root.join("control.sock"), "status").is_ok());
    service
}
fn until(mut predicate: impl FnMut() -> bool) {
    let start = Instant::now();
    while !predicate() {
        assert!(
            start.elapsed() < Duration::from_secs(10),
            "timed out waiting for service"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn file_edits_have_no_effect_until_explicit_reload_and_invalid_reload_is_atomic() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("source")).unwrap();
    let mut settings = config(root);
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    let _service = start(root);
    let socket = root.join("control.sock");
    settings["targets"][0]["enabled"] = false.into();
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    assert_eq!(
        daemon::request(&socket, "status").unwrap()["targets"][0]["enabled"],
        true
    );
    let original_due =
        daemon::request(&socket, "status").unwrap()["targets"][0]["next_due_ms"].clone();
    let unusable_log = root.join("audit-directory");
    fs::create_dir(&unusable_log).unwrap();
    settings["logging"] = json!({"audit_file":unusable_log});
    settings["targets"][0]["backup_interval_seconds"] = 7200.into();
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    assert!(daemon::request(&socket, "reload").is_err());
    let unchanged = daemon::request(&socket, "status").unwrap();
    assert_eq!(unchanged["targets"][0]["enabled"], true);
    assert_eq!(unchanged["targets"][0]["next_due_ms"], original_due);
    settings.as_object_mut().unwrap().remove("logging");
    fs::write(root.join("config.json"), b"invalid JSON").unwrap();
    assert!(daemon::request(&socket, "reload").is_err());
    assert_eq!(
        daemon::request(&socket, "status").unwrap()["targets"][0]["enabled"],
        true
    );
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    daemon::request(&socket, "reload").unwrap();
    assert_eq!(
        daemon::request(&socket, "status").unwrap()["targets"][0]["enabled"],
        false
    );
}

#[test]
fn control_requests_can_repair_an_unwritable_audit_destination() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("source")).unwrap();
    let mut settings = config(root);
    settings["logging"] = json!({"audit_file":"/dev/full"});
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    let _service = start(root);
    let socket = root.join("control.sock");
    assert_eq!(
        daemon::request(&socket, "status").unwrap()["targets"][0]["enabled"],
        true
    );
    let audit = root.join("operations.jsonl");
    settings["logging"] = json!({"audit_file":audit});
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    daemon::request(&socket, "reload").unwrap();
    assert_eq!(
        daemon::request(&socket, "status").unwrap()["config"]["logging"]["audit_file"],
        settings["logging"]["audit_file"]
    );
    assert!(
        fs::read_to_string(root.join("service.log"))
            .unwrap()
            .contains("audit.write")
    );
    assert!(fs::read_to_string(audit).unwrap().lines()
        .filter_map(|line|serde_json::from_str::<Value>(line).ok())
        .any(|event|event["operation"]=="control.request" && event["outcome"]=="succeeded"));
}

#[test]
fn busy_target_skips_requests_and_shutdown_drains_cleanly() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("source")).unwrap();
    fs::write(root.join("source/file"), b"source").unwrap();
    fs::create_dir(root.join("state")).unwrap();
    fs::create_dir(root.join("backups")).unwrap();
    let device = fs::metadata(root.join("backups")).unwrap().dev();
    let lock = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join(format!("state/filesystem-{device}.lock")))
        .unwrap();
    lock.lock_exclusive().unwrap();
    let mut settings = config(root);
    settings["targets"][0]["run_on_startup"] = true.into();
    settings["targets"][0]["backup_interval_seconds"] = 1.into();
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    let mut service = start(root);
    let socket = root.join("control.sock");
    until(|| {
        fs::read_to_string(root.join("service.log"))
            .unwrap()
            .contains("scheduled trigger skipped")
    });
    let status = daemon::request(&socket, "status").unwrap();
    assert_eq!(status["targets"][0]["outstanding_jobs"], 1);
    assert!(
        daemon::request(
            &socket,
            &json!({"command":"trigger","target":"test"}).to_string()
        )
        .is_err()
    );
    assert!(
        fs::read_to_string(root.join("service.log"))
            .unwrap()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .any(|event| event["operation"] == "control.request"
                && event["outcome"] == "failed"
                && event["timestamp"].as_str().is_some())
    );
    FileExt::unlock(&lock).unwrap();
    until(|| {
        daemon::request(&socket, "status").unwrap()["targets"][0]["healthy_snapshots"]
            .as_u64()
            .unwrap()
            > 0
    });
    Command::new("kill")
        .arg("-TERM")
        .arg(service.child.id().to_string())
        .status()
        .unwrap();
    until(|| service.child.try_wait().unwrap().is_some());
    assert!(service.child.wait().unwrap().success());
    assert!(!socket.exists());
    assert_eq!(fs::read(root.join("source/file")).unwrap(), b"source");
}

#[test]
fn failed_jobs_use_bounded_retries_and_release_the_queue_slot() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("source")).unwrap();
    let mut settings = config(root);
    settings["targets"][0]["run_on_startup"] = true.into();
    settings["targets"][0]["source_dir"] = root.join("not-yet-mounted").to_str().unwrap().into();
    settings["queue"] = json!({"max_attempts":2,"retry_initial_seconds":1,"retry_max_seconds":1});
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    let _service = start(root);
    let socket = root.join("control.sock");
    until(|| {
        fs::read_to_string(root.join("service.log"))
            .unwrap()
            .contains("backup failed")
    });
    until(|| daemon::request(&socket, "status").unwrap()["targets"][0]["outstanding_jobs"] == 0);
    let log = fs::read_to_string(root.join("service.log")).unwrap();
    assert_eq!(log.matches("backup failed").count(), 2);
    assert_eq!(
        daemon::request(&socket, "status").unwrap()["targets"][0]["healthy_snapshots"],
        0
    );
}

#[test]
fn immediate_request_runs_with_service_and_keeps_the_regular_schedule() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("source")).unwrap();
    fs::write(root.join("source/file"), b"immediate").unwrap();
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&config(root)).unwrap(),
    )
    .unwrap();
    let _service = start(root);
    let socket = root.join("control.sock");
    let due = daemon::request(&socket, "status").unwrap()["targets"][0]["next_due_ms"].clone();
    let output = Command::new(env!("CARGO_BIN_EXE_syncthing-backup-tool"))
        .arg("--control-socket")
        .arg(&socket)
        .args([
            "trigger",
            "--target",
            "test",
            "--wait",
            "--timeout-seconds",
            "10",
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let status = daemon::request(&socket, "status").unwrap();
    assert_eq!(status["targets"][0]["healthy_snapshots"], 1);
    assert_eq!(status["targets"][0]["next_due_ms"], due);
}

#[test]
fn queued_work_obeys_restart_cooldown_before_workers_or_prepare_hooks() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("source")).unwrap();
    fs::write(root.join("source/data"), b"backup").unwrap();
    let mut settings = config(root);
    settings["io_cooldown_seconds"] = 2.into();
    settings["targets"][0]["manual_only"] = true.into();
    fs::create_dir(root.join("second-source")).unwrap();
    fs::write(root.join("second-source/data"), b"backup").unwrap();
    let mut second = settings["targets"][0].clone();
    second["id"] = "second".into();
    second["source_dir"] = json!(root.join("second-source"));
    second["destination_dir"] = json!(root.join("second-backups"));
    second["hooks"] = json!({"before_backup":[{"name":"prepare","command":["/usr/bin/touch",root.join("prepared")]}]});
    settings["targets"].as_array_mut().unwrap().push(second);
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    let service = start(root);
    let socket = root.join("control.sock");
    let first = daemon::request(
        &socket,
        &json!({"command":"trigger","target":"test"}).to_string(),
    )
    .unwrap()["jobs"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    until(|| {
        daemon::request(&socket, &json!({"command":"job","id":first}).to_string()).unwrap()["status"]
            == "succeeded"
    });
    let second = daemon::request(
        &socket,
        &json!({"command":"trigger","target":"second"}).to_string(),
    )
    .unwrap()["jobs"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    thread::sleep(Duration::from_millis(100));
    assert!(!root.join("prepared").exists());
    let queued =
        daemon::request(&socket, &json!({"command":"job","id":second}).to_string()).unwrap();
    assert_eq!(queued["status"], "queued");
    assert_eq!(queued["attempts"], 0);
    assert!(
        daemon::request(&socket, "status").unwrap()["io"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["delay_reason"] == "filesystem cooldown")
    );
    drop(service);
    let _service = start(root);
    assert!(!root.join("prepared").exists());
    until(|| {
        daemon::request(&socket, &json!({"command":"job","id":second}).to_string()).unwrap()["status"]
            == "succeeded"
    });
    assert!(root.join("prepared").exists());
    assert!(daemon::request(&socket, "status").unwrap()["targets"][0]["next_due_ms"].is_null());
}

#[test]
fn independent_scrub_detects_corruption_without_scheduling_another_capture() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("source")).unwrap();
    fs::write(root.join("source/data"), b"original").unwrap();
    let mut settings = config(root);
    settings["targets"][0]["manual_only"] = true.into();
    settings["scrub"] = json!({"interval_seconds":1});
    fs::write(
        root.join("config.json"),
        serde_json::to_vec(&settings).unwrap(),
    )
    .unwrap();
    let _service = start(root);
    let socket = root.join("control.sock");
    let id = daemon::request(
        &socket,
        &json!({"command":"trigger","target":"test"}).to_string(),
    )
    .unwrap()["jobs"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    until(|| {
        daemon::request(&socket, &json!({"command":"job","id":id}).to_string()).unwrap()["status"]
            == "succeeded"
    });
    let status = daemon::request(&socket, &json!({"command":"job","id":id}).to_string()).unwrap();
    let archive = root
        .join("backups")
        .join(status["snapshot"]["filename"].as_str().unwrap());
    let mut bytes = fs::read(&archive).unwrap();
    bytes[20] ^= 1;
    fs::write(&archive, &bytes).unwrap();
    until(|| daemon::request(&socket, "status").unwrap()["last_scrub"]["healthy"] == false);
    let status = daemon::request(&socket, "status").unwrap();
    assert_eq!(status["targets"][0]["healthy_snapshots"], 1);
    assert_eq!(status["targets"][0]["health"], "degraded");
    assert_eq!(fs::read(&archive).unwrap(), bytes);
    assert!(
        !status["recent_integrity_incidents"]
            .as_array()
            .unwrap()
            .is_empty()
    );
}
