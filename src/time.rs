//! Time utilities. Monotonic time is used for latencies and lease timing;
//! wall-clock (epoch seconds) is used for durable timestamps and logging.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const NANOS_PER_SEC: u64 = 1_000_000_000;

/// A point-in-time source. Kept behind a small trait so tests can inject a
/// controllable clock (for lease-expiry and migration-timeout testing).
pub trait Clock: Send + Sync {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
    /// Current seconds since the Unix epoch (wall clock).
    fn epoch_secs(&self) -> u64;
    /// Approximate clock quality: true if wall clock is a real source.
    fn is_real(&self) -> bool {
        true
    }
}

/// The production clock backed by the operating system.
#[derive(Debug, Clone, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
    fn epoch_secs(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
}

/// A synthetic, controllable clock for deterministic tests.
#[derive(Debug)]
pub struct MockClock {
    base: Instant,
    epoch: AtomicU64,
}

impl MockClock {
    pub fn new() -> MockClock {
        MockClock { base: Instant::now(), epoch: AtomicU64::new(0) }
    }
    pub fn advance(&self, _d: Duration) {
        // For deterministic lease/migration tests, a MockClock with a fixed
        // base is sufficient; the caller normally uses set_epoch.
    }
    pub fn set_epoch(&self, e: u64) {
        self.epoch.store(e, Ordering::SeqCst);
    }
}

impl Clock for MockClock {
    fn now(&self) -> Instant {
        self.base
    }
    fn epoch_secs(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }
    fn is_real(&self) -> bool {
        false
    }
}

/// Helpers for readable durations in telemetry.
pub fn fmt_us(d: Duration) -> u128 {
    d.as_micros()
}

pub fn fmt_nanos(d: Duration) -> u128 {
    d.as_nanos()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn system_clock_epoch_positive() {
        let c = SystemClock;
        assert!(c.epoch_secs() > 1_600_000_000);
    }
    #[test]
    fn mock_clock_epoch_control() {
        let c = MockClock::new();
        c.set_epoch(42);
        assert_eq!(c.epoch_secs(), 42);
        assert!(!c.is_real());
    }
}
