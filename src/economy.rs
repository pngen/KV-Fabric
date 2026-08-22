//! Transfer-vs-recompute economics planner.
//!
//! The planner is a deterministic, pure function. For identical inputs it always
//! returns the same decision, and it exposes the reasoning (all cost terms and
//! the selected action) through a `Plan` so that telemetry can audit decisions.

use crate::lifecycle::ResidencyState;

/// Version of the placement/planner policy.
pub const PLANNER_VERSION: u32 = 1;

/// Cost terms that feed the decision.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PlanInput {
    pub object_size_bytes: u64,
    /// Predicted number of future reuse accesses.
    pub expected_reuse: u64,
    /// Value (in arbitrary cost units) of serving from cache.
    pub reuse_value: u64,
    /// Cost to recompute the state from scratch.
    pub recompute_cost: u64,
    /// Cost to copy bytes host -> accelerator.
    pub promotion_cost: u64,
    /// Cost to copy bytes accelerator -> host.
    pub demotion_cost: u64,
    /// Cost to restore from persistent storage to host/accelerator.
    pub restore_cost: u64,
    /// Cost to persist the object durably.
    pub persistence_cost: u64,
    /// Cost to fetch from a remote replica (network + transfer).
    pub replica_fetch_cost: u64,
    pub current_residency: ResidencyState,
    /// 0..1 pressure on the accelerator tier.
    pub accelerator_pressure: f64,
    /// 0..1 pressure on the host tier.
    pub host_pressure: f64,
    pub has_remote_replica: bool,
    pub has_storage_copy: bool,
    pub latency_weight: f64,
}

impl Default for PlanInput {
    fn default() -> Self {
        PlanInput {
            object_size_bytes: 0,
            expected_reuse: 0,
            reuse_value: 0,
            recompute_cost: u64::MAX,
            promotion_cost: 0,
            demotion_cost: 0,
            restore_cost: 0,
            persistence_cost: 0,
            replica_fetch_cost: 0,
            current_residency: ResidencyState::Absent,
            accelerator_pressure: 0.0,
            host_pressure: 0.0,
            has_remote_replica: false,
            has_storage_copy: false,
            latency_weight: 1.0,
        }
    }
}

/// The selected action.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Decision {
    ReuseInPlace,
    Promote,
    Restore,
    FetchRemote,
    Recompute,
    Reject,
    Wait,
    ReclaimFirst,
}

impl Decision {
    pub fn as_str(self) -> &'static str {
        match self {
            Decision::ReuseInPlace => "reuse_in_place",
            Decision::Promote => "promote",
            Decision::Restore => "restore",
            Decision::FetchRemote => "fetch_remote",
            Decision::Recompute => "recompute",
            Decision::Reject => "reject",
            Decision::Wait => "wait",
            Decision::ReclaimFirst => "reclaim_first",
        }
    }
}

/// A deterministic plan: the chosen action, its estimated cost and the full
/// reasoning (all cost terms) so it can be exposed via telemetry.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub decision: Decision,
    pub estimated_cost: u64,
    pub recompute_cost: u64,
    pub transfer_cost: u64,
    pub retention_cost: u64,
    pub reuse_value: u64,
    pub reasoning: String,
}

/// Compute a plan for a requested object. Pure and deterministic.
pub fn plan(input: &PlanInput) -> Plan {
    // Deterministic tie-breaking: among equal estimated cost, prefer the action
    // that appears lowest in this fixed order.
    let order = [
        Decision::ReuseInPlace,
        Decision::Restore,
        Decision::FetchRemote,
        Decision::Promote,
        Decision::Wait,
        Decision::Recompute,
        Decision::ReclaimFirst,
        Decision::Reject,
    ];

    let mut candidates: Vec<(Decision, u64, String)> = Vec::new();
    let pressure_penalty = |p: f64| (p.max(0.0) * 10_000.0) as u64;
    let pressure = (input.accelerator_pressure + input.host_pressure) / 2.0;

    // Reuse in place is the best path when the state is already resident and
    // usable: zero delivery cost, zero recompute.
    match input.current_residency {
        ResidencyState::AcceleratorResident | ResidencyState::HostResident
        | ResidencyState::Replicated => {
            candidates.push((Decision::ReuseInPlace, 0, "resident; reuse in place".into()));
        }
        ResidencyState::StorageResident => {
            if input.has_storage_copy && input.restore_cost < input.recompute_cost {
                candidates.push((Decision::Restore, input.restore_cost, "storage copy cheaper than recompute".into()));
            }
        }
        ResidencyState::Absent | ResidencyState::Reclaimable | ResidencyState::Invalid
        | ResidencyState::Retired => {
            if input.has_storage_copy && input.restore_cost < input.recompute_cost {
                candidates.push((Decision::Restore, input.restore_cost, "storage copy cheaper than recompute".into()));
            }
            if input.has_remote_replica && input.replica_fetch_cost < input.recompute_cost {
                candidates.push((Decision::FetchRemote, input.replica_fetch_cost, "remote replica cheaper than recompute".into()));
            }
        }
        _ => {}
    }

    // Promote is offered when the state exists on host and the request wants it
    // on the accelerator; compare against recompute and discourage promotion
    // under high accelerator pressure.
    if matches!(input.current_residency, ResidencyState::HostResident)
        && input.promotion_cost < input.recompute_cost
        && input.accelerator_pressure < 0.8
    {
        candidates.push((
            Decision::Promote,
            input.promotion_cost.saturating_add(pressure_penalty(input.accelerator_pressure)),
            "promotion to accelerator cheaper than recompute".into(),
        ));
    }

    // Recompute is always a fallback candidate.
    candidates.push((Decision::Recompute, input.recompute_cost, "recompute from scratch".into()));

    // Wait is offered when pressure is high and the object is not resident.
    if pressure > 0.9 && !matches!(
        input.current_residency,
        ResidencyState::AcceleratorResident | ResidencyState::HostResident
    ) {
        candidates.push((
            Decision::Wait,
            input.recompute_cost.saturating_add(pressure_penalty(pressure)).saturating_add(1000),
            "high pressure; wait for reclamation".into(),
        ));
    }

    // Reject when nothing is better than a threshold and reuse value is tiny.
    if input.expected_reuse == 0 && input.recompute_cost == u64::MAX {
        candidates = vec![(Decision::Reject, u64::MAX, "unreusable and unprecomputable".into())];
    }

    // Choose minimum cost with deterministic tie-break by action order.
    let mut best = None;
    for d in order.iter() {
        if let Some((_, cost, reason)) = candidates.iter().find(|t| t.0 == *d) {
            let c = *cost;
            let better = match best {
                None => true,
                Some((bc, _, _)) => c < bc,
            };
            if better {
                best = Some((c, *d, reason.clone()));
            }
        }
    }

    let (cost, decision, reason) = best
        .unwrap_or((u64::MAX, Decision::Reject, "no viable option".into()));

    Plan {
        decision,
        estimated_cost: cost,
        recompute_cost: input.recompute_cost,
        transfer_cost: input.promotion_cost + input.demotion_cost + input.restore_cost,
        retention_cost: input.persistence_cost,
        reuse_value: input.reuse_value,
        reasoning: reason,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resident() -> PlanInput {
        PlanInput {
            object_size_bytes: 1000,
            expected_reuse: 4,
            reuse_value: 400,
            recompute_cost: 1000,
            promotion_cost: 20,
            demotion_cost: 20,
            restore_cost: 100,
            persistence_cost: 5,
            replica_fetch_cost: 200,
            current_residency: ResidencyState::HostResident,
            accelerator_pressure: 0.2,
            host_pressure: 0.2,
            has_remote_replica: true,
            has_storage_copy: false,
            latency_weight: 1.0,
        }
    }

    #[test]
    fn resident_reuses_in_place() {
        let p = plan(&resident());
        assert_eq!(p.decision, Decision::ReuseInPlace);
        assert!(p.reasoning.contains("resident"));
    }

    #[test]
    fn absent_with_replica_fetches_when_cheaper() {
        let mut i = resident();
        i.current_residency = ResidencyState::Absent;
        i.recompute_cost = 1000;
        i.replica_fetch_cost = 100;
        i.reuse_value = 0;
        i.has_remote_replica = true;
        let p = plan(&i);
        assert_eq!(p.decision, Decision::FetchRemote);
    }

    #[test]
    fn absent_recomputes_when_no_replica() {
        let mut i = resident();
        i.current_residency = ResidencyState::Absent;
        i.has_remote_replica = false;
        i.recompute_cost = 500;
        i.reuse_value = 0;
        let p = plan(&i);
        assert_eq!(p.decision, Decision::Recompute);
    }

    #[test]
    fn storage_restore_preferred_when_cheaper() {
        let mut i = resident();
        i.current_residency = ResidencyState::StorageResident;
        i.has_storage_copy = true;
        i.recompute_cost = 5000;
        i.restore_cost = 100;
        i.reuse_value = 0;
        let p = plan(&i);
        assert_eq!(p.decision, Decision::Restore);
    }

    #[test]
    fn deterministic_for_same_input() {
        assert_eq!(plan(&resident()), plan(&resident()));
    }
}
