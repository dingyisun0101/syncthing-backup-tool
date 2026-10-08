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

#[cfg(test)]
mod tests {
    #[test]
    fn skip_missed_intervals_and_clock_rollback() {
        assert_eq!(super::next_due(0, 25_000, 10), 30_000);
        assert_eq!(super::next_due(0, 10_000, 10), 20_000);
        assert_eq!(super::next_due(10_000, 0, 10), 20_000);
    }
}
