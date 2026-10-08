use crate::{config::QueueConfig, domain::Permanent};

pub fn retry_at(
    error: &anyhow::Error,
    attempts: u32,
    settings: &QueueConfig,
    now: i64,
) -> Option<i64> {
    if error.is::<Permanent>() || attempts >= settings.max_attempts {
        return None;
    }
    let delay = settings
        .retry_initial_seconds
        .saturating_mul(
            1u64.checked_shl(attempts.saturating_sub(1).min(63))
                .unwrap_or(u64::MAX),
        )
        .min(settings.retry_max_seconds);
    Some(now.saturating_add(delay as i64 * 1000))
}

pub struct BoundedFifo;
impl crate::api::QueuePolicy for BoundedFifo {
    fn admit(&self, outstanding: bool, pending: usize, maximum: usize) -> bool {
        !outstanding && pending < maximum
    }
    fn retry_at(
        &self,
        error: &anyhow::Error,
        attempts: u32,
        settings: &QueueConfig,
        now: i64,
    ) -> Option<i64> {
        retry_at(error, attempts, settings, now)
    }
}
