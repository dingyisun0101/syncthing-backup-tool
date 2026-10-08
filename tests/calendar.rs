use chrono::TimeZone;
#[test]
fn pacific_daily_schedule_keeps_wall_time_across_dst() {
    let schedule = serde_json::from_value(
        serde_json::json!({"frequency":"daily","time":"04:00","timezone":"America/Los_Angeles"}),
    )
    .unwrap();
    let tz = chrono_tz::America::Los_Angeles;
    let saturday = tz.with_ymd_and_hms(2026, 3, 7, 4, 0, 0).unwrap();
    let next =
        syncthing_backup_tool::scheduler::calendar_next(&schedule, saturday.timestamp_millis())
            .unwrap();
    assert_eq!(
        next,
        tz.with_ymd_and_hms(2026, 3, 8, 4, 0, 0)
            .unwrap()
            .timestamp_millis()
    );
    assert_eq!(next - saturday.timestamp_millis(), 23 * 3600 * 1000);
}
#[test]
fn weekly_calendar_chooses_next_monday_and_rejects_invalid_settings() {
    let mut schedule:syncthing_backup_tool::config::CalendarSchedule=serde_json::from_value(serde_json::json!({"frequency":"weekly","weekday":"mon","time":"03:00","timezone":"America/Los_Angeles"})).unwrap();
    let tz = chrono_tz::America::Los_Angeles;
    let now = tz.with_ymd_and_hms(2026, 10, 7, 12, 0, 0).unwrap();
    assert_eq!(
        syncthing_backup_tool::scheduler::calendar_next(&schedule, now.timestamp_millis()).unwrap(),
        tz.with_ymd_and_hms(2026, 10, 12, 3, 0, 0)
            .unwrap()
            .timestamp_millis()
    );
    schedule.timezone = "invalid".into();
    assert!(
        syncthing_backup_tool::scheduler::calendar_next(&schedule, now.timestamp_millis()).is_err()
    );
}
