use crate::config::Logging;
use serde_json::{Value, json};
use std::sync::{OnceLock, RwLock};

static SETTINGS: OnceLock<RwLock<Logging>> = OnceLock::new();
pub fn configure(settings: &Logging) {
    let lock = SETTINGS.get_or_init(|| RwLock::new(settings.clone()));
    if let Ok(mut config) = lock.write() {
        *config = settings.clone();
    }
}
fn priority(level: &str) -> u8 {
    match level {
        "debug" => 0,
        "info" => 1,
        "warn" => 2,
        _ => 3,
    }
}
pub fn event(level: &str, message: &str, details: Value) {
    let settings = SETTINGS.get().and_then(|l| l.read().ok());
    if settings
        .as_ref()
        .is_some_and(|s| priority(level) < priority(&s.level))
    {
        return;
    }
    let at = chrono::Utc::now().to_rfc3339();
    if settings.as_ref().is_some_and(|s| s.format == "text") {
        eprintln!("{at} {level} {message} {details}");
    } else {
        eprintln!(
            "{}",
            json!({"time":at,"level":level,"message":message,"details":details})
        );
    }
}
