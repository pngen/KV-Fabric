//! Reclamation policy.
//!
//! Under pressure, KV Fabric must choose which state to demote, persist, mark
//! recomputable or reclaim. The policy is deterministic and never reclaims
//! state that is actively leased or required by protected descendants.

use crate::id::ObjectId;

/// Version of the eviction/reclamation policy.
pub const RECLAIM_VERSION: u32 = 1;

/// The action taken on a selected object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReclaimAction {
    /// Demote one tier down (e.g. accelerator -> host).
    Demote,
    /// Persist durably, then reclaim resident state.
    Persist,
    /// Mark recomputable (drop resident bytes, keep metadata).
    MarkRecomputable,
    /// Reclaim (free memory, keep durable/lineage if applicable).
    Reclaim,
    /// Retire (delete durable too) when no longer worth keeping.
    Retire,
}

impl ReclaimAction {
    pub fn as_str(self) -> &'static str {
        match self {
            ReclaimAction::Demote => "demote",
            ReclaimAction::Persist => "persist",
            ReclaimAction::MarkRecomputable => "mark_recomputable",
            ReclaimAction::Reclaim => "reclaim",
            ReclaimAction::Retire => "retire",
        }
    }
}

/// A candidate object considered for reclamation.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub object_id: ObjectId,
    pub byte_size: u64,
    /// Reuse value (higher is better to keep).
    pub value: u64,
    pub last_access_epoch: u64,
    pub recompute_cost: u64,
    /// Reference / lease count; 0 means no active users (reclaimable).
    pub ref_count: u64,
    /// True if protected (e.g. required by a protected descendant).
    pub protected: bool,
    /// True if already durable (persisted), so reclaim does not lose data.
    pub durable: bool,
    pub is_leased: bool,
}

/// A reclamation decision for one object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReclaimDecision {
    pub object_id: ObjectId,
    pub action: ReclaimAction,
    pub bytes_freed: u64,
    pub reason: String,
}

/// Select the best victims to free at least target_bytes. Deterministic.
///
/// Selection order: skip protected/leased/active; then prefer low value, old
/// last_access, high recompute cost (cheap to recompute -> reclaim). Within a
/// tier, actions are chosen to best preserve durable value.
pub fn select(candidates: &[Candidate], target_bytes: u64) -> Vec<ReclaimDecision> {
    let mut eligible: Vec<&Candidate> = candidates
        .iter()
        .filter(|c| !c.protected && !c.is_leased && c.ref_count == 0)
        .collect();

    // Deterministic sort: value ascending, then last_access ascending, then id.
    eligible.sort_by(|a, b| {
        a.value
            .cmp(&b.value)
            .then(a.last_access_epoch.cmp(&b.last_access_epoch))
            .then(a.object_id.cmp(&b.object_id))
    });

    let mut result = Vec::new();
    let mut freed = 0u64;
    for c in eligible {
        if freed >= target_bytes {
            break;
        }
        let action = if c.durable {
            ReclaimAction::Reclaim
        } else if c.recompute_cost < c.byte_size {
            ReclaimAction::MarkRecomputable
        } else {
            ReclaimAction::Demote
        };
        freed += c.byte_size;
        result.push(ReclaimDecision {
            object_id: c.object_id,
            action,
            bytes_freed: c.byte_size,
            reason: format!("selected under pressure (value={} last={})", c.value, c.last_access_epoch),
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cand(id: u128, value: u64, last: u64, refcount: u64, protected: bool, leased: bool, durable: bool, size: u64) -> Candidate {
        Candidate {
            object_id: ObjectId::from_u128(id),
            byte_size: size,
            value,
            last_access_epoch: last,
            recompute_cost: 100,
            ref_count: refcount,
            protected,
            durable,
            is_leased: leased,
        }
    }

    #[test]
    fn selects_lowest_value_first() {
        let cs = vec![
            cand(1, 50, 100, 0, false, false, true, 10),
            cand(2, 10, 50, 0, false, false, true, 10),
            cand(3, 30, 500, 1, false, false, true, 10), // active ref
        ];
        let picked = select(&cs, 10);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].object_id, ObjectId::from_u128(2)); // lowest value
    }

    #[test]
    fn never_reclaims_leased_or_protected() {
        let cs = vec![
            cand(1, 0, 0, 0, true, false, true, 10),  // protected
            cand(2, 0, 0, 0, false, true, true, 10),  // leased
            cand(3, 0, 0, 0, false, false, true, 10), // reclaimable
        ];
        let picked = select(&cs, 5);
        assert_eq!(picked.len(), 1);
        assert_eq!(picked[0].object_id, ObjectId::from_u128(3));
    }

    #[test]
    fn accumulative_bytes_target() {
        let cs = vec![
            cand(1, 0, 0, 0, false, false, true, 40),
            cand(2, 0, 0, 0, false, false, true, 40),
        ];
        let picked = select(&cs, 50);
        assert_eq!(picked.len(), 2);
        let total: u64 = picked.iter().map(|p| p.bytes_freed).sum();
        assert!(total >= 50);
    }

    #[test]
    fn deterministic_order() {
        let cs = vec![
            cand(1, 5, 10, 0, false, false, true, 10),
            cand(2, 5, 10, 0, false, false, true, 10),
        ];
        let a = select(&cs, 1000);
        let b = select(&cs, 1000);
        assert_eq!(a, b);
    }
}
