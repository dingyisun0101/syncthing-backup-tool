/// Advance to the next future deadline, coalescing all missed intervals.
pub fn next_due(previous: i64, now: i64, interval_seconds: u64) -> i64 {
    let interval = interval_seconds as i64 * 1000;
    previous.saturating_add(
        ((now.saturating_sub(previous)).max(0) / interval + 1).saturating_mul(interval),
    )
}

pub struct Interval;
impl crate::api::SchedulingPolicy for Interval {
    fn next_due(&self, previous: i64, now: i64, interval_seconds: u64) -> i64 {
        next_due(previous, now, interval_seconds)
    }
}

pub fn next_for(target: &crate::config::Target, previous: i64, now: i64) -> anyhow::Result<i64> {
    if target.manual_only {
        return Ok(i64::MAX);
    }
    if let Some(schedule) = &target.schedule {
        calendar_next(schedule, now)
    } else if let Some(anchor) = &target.interval_anchor {
        let anchor = chrono::DateTime::parse_from_rfc3339(anchor)?.timestamp_millis();
        let interval = target.backup_interval_seconds as i64 * 1000;
        if now < anchor {
            Ok(anchor)
        } else {
            Ok(anchor.saturating_add(
                (now.saturating_sub(anchor) / interval + 1).saturating_mul(interval),
            ))
        }
    } else {
        Ok(next_due(previous, now, target.backup_interval_seconds))
    }
}
pub fn calendar_next(schedule: &crate::config::CalendarSchedule, now: i64) -> anyhow::Result<i64> {
    use anyhow::{Context, ensure};
    use chrono::{Datelike, LocalResult, TimeZone};
    ensure!(
        ["daily", "weekly"].contains(&schedule.frequency.as_str()),
        "schedule.frequency must be daily or weekly"
    );
    let timezone: chrono_tz::Tz = schedule
        .timezone
        .parse()
        .context("invalid schedule timezone")?;
    ensure!(
        schedule.time.is_empty() != schedule.times.is_empty(),
        "provide exactly one of schedule.time or schedule.times"
    );
    let slots: Vec<_> = if schedule.times.is_empty() {
        vec![&schedule.time]
    } else {
        schedule.times.iter().collect()
    };
    ensure!(slots.len() <= 96, "at most 96 calendar slots allowed");
    let mut times = Vec::new();
    for slot in slots {
        let time = chrono::NaiveTime::parse_from_str(slot, "%H:%M")
            .context("calendar slots must be HH:MM")?;
        ensure!(
            time.format("%H:%M").to_string() == *slot && !times.contains(&time),
            "invalid or duplicate calendar slot"
        );
        times.push(time);
    }
    times.sort();
    let weekday = if schedule.frequency == "weekly" {
        Some(match schedule.weekday.as_deref() {
            Some("mon") => 0,
            Some("tue") => 1,
            Some("wed") => 2,
            Some("thu") => 3,
            Some("fri") => 4,
            Some("sat") => 5,
            Some("sun") => 6,
            _ => anyhow::bail!("weekly schedule needs weekday mon..sun"),
        })
    } else {
        None
    };
    let date = chrono::Utc
        .timestamp_millis_opt(now)
        .single()
        .context("invalid schedule timestamp")?
        .with_timezone(&timezone)
        .date_naive();
    for offset in 0..15 {
        let day = date
            .checked_add_days(chrono::Days::new(offset))
            .context("schedule date overflow")?;
        if weekday.is_some_and(|w| day.weekday().num_days_from_monday() != w) {
            continue;
        }
        for time in &times {
            let value = match timezone.from_local_datetime(&day.and_time(*time)) {
                LocalResult::Single(v) => Some(v),
                LocalResult::Ambiguous(a, b) => Some(a.min(b)),
                LocalResult::None => None,
            };
            if let Some(value) = value
                && value.timestamp_millis() > now
            {
                return Ok(value.timestamp_millis());
            }
        }
    }
    anyhow::bail!("no future calendar deadline")
}

#[cfg(test)]
mod tests {
    #[test]
    fn skip_missed_intervals_and_clock_rollback() {
        assert_eq!(super::next_due(0, 25_000, 10), 30_000);
        assert_eq!(super::next_due(0, 10_000, 10), 20_000);
        assert_eq!(super::next_due(10_000, 0, 10), 20_000);
    }
}
