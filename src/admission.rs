//! Bounded admission control.
//!
//! KV Fabric never blindly admits every cache object. Admission considers
//! capacity, expected reuse, size, reconstruction cost, priority, namespace
//! quotas and current pressure, and returns an explicit (deterministic) mode.

use crate::id::ObjectId;

/// Version of the admission policy.
pub const ADMISSION_VERSION: u32 = 1;

/// The modes admission may produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdmitMode {
    /// Admit as hot (accelerator-favoured).
    Hot,
    /// Admit as cold (host-favoured).
    Cold,
    /// Persist only; do not keep resident.
    PersistOnly,
    /// Use transiently; do not cache.
    Transient,
    /// Decline caching entirely.
    Decline,
    /// Admit by replacing a lower-value resident object.
    Replace,
}

impl AdmitMode {
    pub fn as_str(self) -> &'static str {
        match self {
            AdmitMode::Hot => "hot",
            AdmitMode::Cold => "cold",
            AdmitMode::PersistOnly => "persist_only",
            AdmitMode::Transient => "transient",
            AdmitMode::Decline => "decline",
            AdmitMode::Replace => "replace",
        }
    }
}

/// Inputs to the admission decision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdmissionInput {
    pub object_id: ObjectId,
    pub namespace: &'static str,
    pub byte_size: u64,
    pub block_count: u64,
    pub recompute_cost: u64,
    pub reuse_value: u64,
    pub expected_reuse: f64,
    pub priority: i32,
    pub namespace_used_bytes: u64,
    pub namespace_used_objects: u64,
    pub namespace_max_bytes: u64,
    pub namespace_max_objects: u64,
    pub host_used_bytes: u64,
    pub host_capacity_bytes: u64,
    pub accelerator_used_bytes: u64,
    pub accelerator_capacity_bytes: u64,
    pub pressure: f64,
    pub policy_enabled: bool,
    pub min_recompute_cost: u64,
    pub min_predicted_reuse: f64,
}

/// The admission result.
#[derive(Debug, Clone, PartialEq)]
pub struct AdmitDecision {
    pub mode: AdmitMode,
    pub reason: String,
}

/// Decide whether to admit an object. Pure and deterministic.
pub fn decide(input: &AdmissionInput) -> AdmitDecision {
    let _ = input.object_id;
    let recompute_cost = input.recompute_cost;
    if !input.policy_enabled {
        return AdmitDecision { mode: AdmitMode::Hot, reason: "admission disabled; pass-through".into() };
    }
    // Too cheap to reconstruct -> never worthwhile to cache.
    if recompute_cost < input.min_recompute_cost {
        return AdmitDecision { mode: AdmitMode::Transient, reason: "recompute cheaper than caching".into() };
    }
    // Predicted reuse too low -> not worth caching.
    if input.expected_reuse < input.min_predicted_reuse {
        return AdmitDecision { mode: AdmitMode::Transient, reason: "predicted reuse too low".into() };
    }

    // Namespace quota checks.
    let bytes_fit = input.byte_size <= input.namespace_max_bytes.saturating_sub(input.namespace_used_bytes);
    let objects_fit = input.namespace_used_objects < input.namespace_max_objects;
    if !bytes_fit {
        return AdmitDecision { mode: AdmitMode::Replace, reason: "namespace byte quota exceeded; replace candidate".into() };
    }
    if !objects_fit {
        return AdmitDecision { mode: AdmitMode::Replace, reason: "namespace object quota exceeded; replace candidate".into() };
    }

    // Overall capacity check.
    let host_fit = input.byte_size <= input.host_capacity_bytes.saturating_sub(input.host_used_bytes);
    let accel_total_possible = if input.accelerator_capacity_bytes == 0 { u64::MAX } else { input.accelerator_capacity_bytes };
    let accel_fit = input.byte_size <= accel_total_possible.saturating_sub(input.accelerator_used_bytes);
    if !host_fit && !accel_fit {
        return AdmitDecision { mode: AdmitMode::Replace, reason: "no tier capacity; replace candidate".into() };
    }

    // Value score: how much value per unit of space.
    let value_per_byte = if input.byte_size == 0 {
        input.reuse_value as f64
    } else {
        input.reuse_value as f64 / input.byte_size as f64
    };
    let priority = input.priority;

    let hot_threshold = 1.0;
    let cold_threshold = 0.05;

    if value_per_byte >= hot_threshold || priority >= 10 {
        AdmitDecision { mode: AdmitMode::Hot, reason: "high value; hot admission".into() }
    } else if value_per_byte >= cold_threshold {
        AdmitDecision { mode: AdmitMode::Cold, reason: "moderate value; cold admission".into() }
    } else if input.recompute_cost > input.reuse_value * 4 {
        AdmitDecision { mode: AdmitMode::PersistOnly, reason: "expensive to rebuild; persist but not resident".into() }
    } else {
        AdmitDecision { mode: AdmitMode::Transient, reason: "low value; transient use".into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> AdmissionInput {
        AdmissionInput {
            object_id: ObjectId::from_u128(1),
            namespace: "ns",
            byte_size: 4096,
            block_count: 4,
            recompute_cost: 100,
            reuse_value: 50,
            expected_reuse: 3.0,
            priority: 0,
            namespace_used_bytes: 0,
            namespace_used_objects: 0,
            namespace_max_bytes: 1 << 30,
            namespace_max_objects: 1000,
            host_used_bytes: 0,
            host_capacity_bytes: 1 << 30,
            accelerator_used_bytes: 0,
            accelerator_capacity_bytes: 0,
            pressure: 0.0,
            policy_enabled: true,
            min_recompute_cost: 1,
            min_predicted_reuse: 0.0,
        }
    }

    #[test]
    fn high_value_is_hot() {
        let mut i = base();
        i.reuse_value = 50000;
        let d = decide(&i);
        assert_eq!(d.mode, AdmitMode::Hot);
    }

    #[test]
    fn cheap_recompute_is_transient() {
        let mut i = base();
        i.recompute_cost = 0;
        i.min_recompute_cost = 1;
        let d = decide(&i);
        assert_eq!(d.mode, AdmitMode::Transient);
    }

    #[test]
    fn quota_exceeded_is_replace() {
        let mut i = base();
        i.namespace_max_bytes = 100;
        i.namespace_used_bytes = 90;
        let d = decide(&i);
        assert_eq!(d.mode, AdmitMode::Replace);
    }

    #[test]
    fn object_quota_exceeded_is_replace() {
        let mut i = base();
        i.reuse_value = 50000; // would be admitted hot if the quota were ignored
        i.namespace_max_objects = 5;
        i.namespace_used_objects = 5;
        let d = decide(&i);
        assert_eq!(d.mode, AdmitMode::Replace);
        // One object below the quota is still admitted.
        i.namespace_used_objects = 4;
        assert_eq!(decide(&i).mode, AdmitMode::Hot);
    }

    #[test]
    fn no_capacity_is_replace() {
        let mut i = base();
        i.host_capacity_bytes = 1000;
        i.host_used_bytes = 900;
        i.accelerator_capacity_bytes = 1000;
        i.accelerator_used_bytes = 900;
        let d = decide(&i);
        assert_eq!(d.mode, AdmitMode::Replace);
    }

    #[test]
    fn deterministic() {
        assert_eq!(decide(&base()), decide(&base()));
    }
}
