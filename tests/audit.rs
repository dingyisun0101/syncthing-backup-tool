#[test]
fn audit_file_records_timestamps_and_rotates_without_filtering_operations() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("operations.jsonl");
    let config = syncthing_backup_tool::config::Logging {
        audit_file: Some(path.clone()),
        max_file_bytes: 1024 * 1024,
        max_files: 2,
        level: "error".into(),
        format: "json".into(),
    };
    syncthing_backup_tool::telemetry::configure(&config).unwrap();
    let large = "x".repeat(32768);
    for number in 0..100 {
        syncthing_backup_tool::telemetry::audit(
            "file.verify",
            "succeeded",
            serde_json::json!({"number":number,"details":large}),
        )
        .unwrap();
    }
    assert!(temp.path().join("operations.jsonl.1").exists());
    let text = std::fs::read_to_string(&path).unwrap();
    let value: serde_json::Value = serde_json::from_str(text.lines().next().unwrap()).unwrap();
    assert_eq!(value["operation"], "file.verify");
    assert!(chrono::DateTime::parse_from_rfc3339(value["timestamp"].as_str().unwrap()).is_ok());
    assert!(path.metadata().unwrap().len() <= 1024 * 1024);
}
