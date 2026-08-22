//! Runtime telemetry counters. Accessible programmatically and reported by the
//! CLI. Counters are atomics so they are safe under the concurrency exercised
//! by node and coordinator threads.

use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
pub struct Telemetry {
    hits: AtomicU64,
    exact_hits: AtomicU64,
    longest_prefix_hits: AtomicU64,
    misses: AtomicU64,
    tokens_reused: AtomicU64,
    bytes_reused: AtomicU64,
    estimated_compute_avoided: AtomicU64,
    objects_admitted: AtomicU64,
    objects_rejected: AtomicU64,
    promotions: AtomicU64,
    demotions: AtomicU64,
    storage_restores: AtomicU64,
    recomputations: AtomicU64,
    migrations: AtomicU64,
    replica_fetches: AtomicU64,
    bytes_transferred: AtomicU64,
    integrity_failures: AtomicU64,
    stale_authority_rejections: AtomicU64,
    lease_conflicts: AtomicU64,
    reclaim_events: AtomicU64,
    quota_violations: AtomicU64,
}

impl Telemetry {
    pub fn new() -> Telemetry {
        Telemetry::default()
    }
    fn add(&self, c: &AtomicU64, by: u64) {
        c.fetch_add(by, Ordering::Relaxed);
    }
    pub fn record_hit(&self) {
        self.add(&self.hits, 1);
    }
    pub fn record_exact_hit(&self) {
        self.add(&self.exact_hits, 1);
    }
    pub fn record_longest_prefix_hit(&self) {
        self.add(&self.longest_prefix_hits, 1);
    }
    pub fn record_miss(&self) {
        self.add(&self.misses, 1);
    }
    pub fn record_tokens_reused(&self, by: u64) {
        self.add(&self.tokens_reused, by);
    }
    pub fn record_bytes_reused(&self, by: u64) {
        self.add(&self.bytes_reused, by);
    }
    pub fn record_compute_avoided(&self, by: u64) {
        self.add(&self.estimated_compute_avoided, by);
    }
    pub fn record_admitted(&self, by: u64) {
        self.add(&self.objects_admitted, by);
    }
    pub fn record_rejected(&self, by: u64) {
        self.add(&self.objects_rejected, by);
    }
    pub fn record_promotion(&self) {
        self.add(&self.promotions, 1);
    }
    pub fn record_demotion(&self) {
        self.add(&self.demotions, 1);
    }
    pub fn record_restore(&self) {
        self.add(&self.storage_restores, 1);
    }
    pub fn record_recomputation(&self) {
        self.add(&self.recomputations, 1);
    }
    pub fn record_migration(&self) {
        self.add(&self.migrations, 1);
    }
    pub fn record_replica_fetch(&self) {
        self.add(&self.replica_fetches, 1);
    }
    pub fn record_bytes_transferred(&self, by: u64) {
        self.add(&self.bytes_transferred, by);
    }
    pub fn record_integrity_failure(&self) {
        self.add(&self.integrity_failures, 1);
    }
    pub fn record_stale_authority_rejection(&self) {
        self.add(&self.stale_authority_rejections, 1);
    }
    pub fn record_lease_conflict(&self) {
        self.add(&self.lease_conflicts, 1);
    }
    pub fn record_reclaim(&self) {
        self.add(&self.reclaim_events, 1);
    }
    pub fn record_quota_violation(&self) {
        self.add(&self.quota_violations, 1);
    }

    pub fn snapshot(&self) -> TelemetrySnapshot {
        TelemetrySnapshot {
            hits: self.hits.load(Ordering::Relaxed),
            exact_hits: self.exact_hits.load(Ordering::Relaxed),
            longest_prefix_hits: self.longest_prefix_hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            tokens_reused: self.tokens_reused.load(Ordering::Relaxed),
            bytes_reused: self.bytes_reused.load(Ordering::Relaxed),
            estimated_compute_avoided: self.estimated_compute_avoided.load(Ordering::Relaxed),
            objects_admitted: self.objects_admitted.load(Ordering::Relaxed),
            objects_rejected: self.objects_rejected.load(Ordering::Relaxed),
            promotions: self.promotions.load(Ordering::Relaxed),
            demotions: self.demotions.load(Ordering::Relaxed),
            storage_restores: self.storage_restores.load(Ordering::Relaxed),
            recomputations: self.recomputations.load(Ordering::Relaxed),
            migrations: self.migrations.load(Ordering::Relaxed),
            replica_fetches: self.replica_fetches.load(Ordering::Relaxed),
            bytes_transferred: self.bytes_transferred.load(Ordering::Relaxed),
            integrity_failures: self.integrity_failures.load(Ordering::Relaxed),
            stale_authority_rejections: self.stale_authority_rejections.load(Ordering::Relaxed),
            lease_conflicts: self.lease_conflicts.load(Ordering::Relaxed),
            reclaim_events: self.reclaim_events.load(Ordering::Relaxed),
            quota_violations: self.quota_violations.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetrySnapshot {
    pub hits: u64,
    pub exact_hits: u64,
    pub longest_prefix_hits: u64,
    pub misses: u64,
    pub tokens_reused: u64,
    pub bytes_reused: u64,
    pub estimated_compute_avoided: u64,
    pub objects_admitted: u64,
    pub objects_rejected: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub storage_restores: u64,
    pub recomputations: u64,
    pub migrations: u64,
    pub replica_fetches: u64,
    pub bytes_transferred: u64,
    pub integrity_failures: u64,
    pub stale_authority_rejections: u64,
    pub lease_conflicts: u64,
    pub reclaim_events: u64,
    pub quota_violations: u64,
}

impl TelemetrySnapshot {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn counters_increment_and_snapshot() {
        let t = Telemetry::new();
        t.record_hit();
        t.record_hit();
        t.record_miss();
        t.record_tokens_reused(128);
        let s = t.snapshot();
        assert_eq!(s.hits, 2);
        assert_eq!(s.misses, 1);
        assert_eq!(s.tokens_reused, 128);
        assert!((s.hit_rate() - 2.0 / 3.0).abs() < 1e-9);
    }
}
