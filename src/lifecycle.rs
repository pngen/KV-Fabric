//! Object lifecycle and residency state machines.
//!
//! Legal transitions are enforced explicitly. An illegal transition is an
//! error, never silently allowed. The state sets and transition tables are the
//! single source of truth for both the runtime and the recovery logic.

use crate::error::{KvError, KvResult};

/// Physical residency tier (where bytes may live).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    Absent,
    Accelerator,
    Host,
    Storage,
}

impl Tier {
    pub fn as_str(self) -> &'static str {
        match self {
            Tier::Absent => "absent",
            Tier::Accelerator => "accelerator",
            Tier::Host => "host",
            Tier::Storage => "storage",
        }
    }
    pub fn from_str(s: &str) -> Option<Tier> {
        match s {
            "absent" => Some(Tier::Absent),
            "accelerator" => Some(Tier::Accelerator),
            "host" => Some(Tier::Host),
            "storage" => Some(Tier::Storage),
            _ => None,
        }
    }
    pub fn code(self) -> u8 {
        match self {
            Tier::Absent => 0,
            Tier::Accelerator => 1,
            Tier::Host => 2,
            Tier::Storage => 3,
        }
    }
    pub fn from_code(c: u8) -> Option<Tier> {
        match c {
            0 => Some(Tier::Absent),
            1 => Some(Tier::Accelerator),
            2 => Some(Tier::Host),
            3 => Some(Tier::Storage),
            _ => None,
        }
    }
}

/// Residency state machine. Composite (in-flight) states are explicit so that
/// a crash mid-transition is recoverable and never looks like a durable state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResidencyState {
    Absent,
    AcceleratorResident,
    HostResident,
    StorageResident,
    Migrating,
    Promoting,
    Demoting,
    Persisting,
    Restoring,
    Replicated,
    Invalid,
    Reclaimable,
    Retired,
}

impl ResidencyState {
    pub fn as_str(self) -> &'static str {
        match self {
            ResidencyState::Absent => "absent",
            ResidencyState::AcceleratorResident => "accelerator_resident",
            ResidencyState::HostResident => "host_resident",
            ResidencyState::StorageResident => "storage_resident",
            ResidencyState::Migrating => "migrating",
            ResidencyState::Promoting => "promoting",
            ResidencyState::Demoting => "demoting",
            ResidencyState::Persisting => "persisting",
            ResidencyState::Restoring => "restoring",
            ResidencyState::Replicated => "replicated",
            ResidencyState::Invalid => "invalid",
            ResidencyState::Reclaimable => "reclaimable",
            ResidencyState::Retired => "retired",
        }
    }
    pub fn from_str(s: &str) -> Option<ResidencyState> {
        Some(match s {
            "absent" => ResidencyState::Absent,
            "accelerator_resident" => ResidencyState::AcceleratorResident,
            "host_resident" => ResidencyState::HostResident,
            "storage_resident" => ResidencyState::StorageResident,
            "migrating" => ResidencyState::Migrating,
            "promoting" => ResidencyState::Promoting,
            "demoting" => ResidencyState::Demoting,
            "persisting" => ResidencyState::Persisting,
            "restoring" => ResidencyState::Restoring,
            "replicated" => ResidencyState::Replicated,
            "invalid" => ResidencyState::Invalid,
            "reclaimable" => ResidencyState::Reclaimable,
            "retired" => ResidencyState::Retired,
            _ => return Option::None,
        })
    }
    pub fn code(self) -> u8 {
        match self {
            ResidencyState::Absent => 0,
            ResidencyState::AcceleratorResident => 1,
            ResidencyState::HostResident => 2,
            ResidencyState::StorageResident => 3,
            ResidencyState::Migrating => 4,
            ResidencyState::Promoting => 5,
            ResidencyState::Demoting => 6,
            ResidencyState::Persisting => 7,
            ResidencyState::Restoring => 8,
            ResidencyState::Replicated => 9,
            ResidencyState::Invalid => 10,
            ResidencyState::Reclaimable => 11,
            ResidencyState::Retired => 12,
        }
    }
    pub fn from_code(c: u8) -> Option<ResidencyState> {
        Some(match c {
            0 => ResidencyState::Absent,
            1 => ResidencyState::AcceleratorResident,
            2 => ResidencyState::HostResident,
            3 => ResidencyState::StorageResident,
            4 => ResidencyState::Migrating,
            5 => ResidencyState::Promoting,
            6 => ResidencyState::Demoting,
            7 => ResidencyState::Persisting,
            8 => ResidencyState::Restoring,
            9 => ResidencyState::Replicated,
            10 => ResidencyState::Invalid,
            11 => ResidencyState::Reclaimable,
            12 => ResidencyState::Retired,
            _ => return Option::None,
        })
    }
    /// The simple tier a resolved state corresponds to, if any.
    pub fn resolved_tier(self) -> Tier {
        match self {
            ResidencyState::AcceleratorResident => Tier::Accelerator,
            ResidencyState::HostResident | ResidencyState::Replicated => Tier::Host,
            ResidencyState::StorageResident => Tier::Storage,
            _ => Tier::Absent,
        }
    }
    pub fn is_resident(self) -> bool {
        matches!(
            self,
            ResidencyState::AcceleratorResident
                | ResidencyState::HostResident
                | ResidencyState::StorageResident
                | ResidencyState::Replicated
        )
    }
    pub fn is_terminal(self) -> bool {
        matches!(self, ResidencyState::Retired)
    }
}

/// Lifecycle state machine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleState {
    Constructing,
    Committed,
    Resident,
    Demoted,
    Persisted,
    Restoring,
    Recomputed,
    Recomputable,
    Reclaimable,
    Retired,
    Quarantined,
    Invalid,
}

impl LifecycleState {
    pub fn as_str(self) -> &'static str {
        match self {
            LifecycleState::Constructing => "constructing",
            LifecycleState::Committed => "committed",
            LifecycleState::Resident => "resident",
            LifecycleState::Demoted => "demoted",
            LifecycleState::Persisted => "persisted",
            LifecycleState::Restoring => "restoring",
            LifecycleState::Recomputed => "recomputed",
            LifecycleState::Recomputable => "recomputable",
            LifecycleState::Reclaimable => "reclaimable",
            LifecycleState::Retired => "retired",
            LifecycleState::Quarantined => "quarantined",
            LifecycleState::Invalid => "invalid",
        }
    }
    pub fn code(self) -> u8 {
        match self {
            LifecycleState::Constructing => 0,
            LifecycleState::Committed => 1,
            LifecycleState::Resident => 2,
            LifecycleState::Demoted => 3,
            LifecycleState::Persisted => 4,
            LifecycleState::Restoring => 5,
            LifecycleState::Recomputed => 6,
            LifecycleState::Recomputable => 7,
            LifecycleState::Reclaimable => 8,
            LifecycleState::Retired => 9,
            LifecycleState::Quarantined => 10,
            LifecycleState::Invalid => 11,
        }
    }
    pub fn from_code(c: u8) -> Option<LifecycleState> {
        Some(match c {
            0 => LifecycleState::Constructing,
            1 => LifecycleState::Committed,
            2 => LifecycleState::Resident,
            3 => LifecycleState::Demoted,
            4 => LifecycleState::Persisted,
            5 => LifecycleState::Restoring,
            6 => LifecycleState::Recomputed,
            7 => LifecycleState::Recomputable,
            8 => LifecycleState::Reclaimable,
            9 => LifecycleState::Retired,
            10 => LifecycleState::Quarantined,
            11 => LifecycleState::Invalid,
            _ => return Option::None,
        })
    }

    /// Validate a transition. Returns Ok(()) if legal, else an error.
    pub fn check_transition(&self, to: LifecycleState) -> KvResult<()> {
        let legal = matches!(
            (self, to),
            (LifecycleState::Constructing, LifecycleState::Committed)
                | (LifecycleState::Committed, LifecycleState::Resident)
                | (LifecycleState::Resident, LifecycleState::Demoted)
                | (LifecycleState::Resident, LifecycleState::Persisted)
                | (LifecycleState::Resident, LifecycleState::Restoring)
                | (LifecycleState::Resident, LifecycleState::Committed)
                | (LifecycleState::Resident, LifecycleState::Recomputable)
                | (LifecycleState::Resident, LifecycleState::Reclaimable)
                | (LifecycleState::Resident, LifecycleState::Retired)
                | (LifecycleState::Resident, LifecycleState::Quarantined)
                | (LifecycleState::Resident, LifecycleState::Invalid)
                | (LifecycleState::Demoted, LifecycleState::Persisted)
                | (LifecycleState::Demoted, LifecycleState::Restoring)
                | (LifecycleState::Demoted, LifecycleState::Reclaimable)
                | (LifecycleState::Demoted, LifecycleState::Recomputable)
                | (LifecycleState::Demoted, LifecycleState::Retired)
                | (LifecycleState::Demoted, LifecycleState::Quarantined)
                | (LifecycleState::Demoted, LifecycleState::Invalid)
                | (LifecycleState::Persisted, LifecycleState::Restoring)
                | (LifecycleState::Persisted, LifecycleState::Recomputable)
                | (LifecycleState::Persisted, LifecycleState::Reclaimable)
                | (LifecycleState::Persisted, LifecycleState::Retired)
                | (LifecycleState::Persisted, LifecycleState::Quarantined)
                | (LifecycleState::Persisted, LifecycleState::Invalid)
                | (LifecycleState::Restoring, LifecycleState::Resident)
                | (LifecycleState::Restoring, LifecycleState::Invalid)
                | (LifecycleState::Restoring, LifecycleState::Quarantined)
                | (LifecycleState::Restoring, LifecycleState::Retired)
                | (LifecycleState::Recomputed, LifecycleState::Resident)
                | (LifecycleState::Recomputed, LifecycleState::Recomputable)
                | (LifecycleState::Recomputed, LifecycleState::Reclaimable)
                | (LifecycleState::Recomputed, LifecycleState::Retired)
                | (LifecycleState::Recomputable, LifecycleState::Resident)
                | (LifecycleState::Recomputable, LifecycleState::Recomputable)
                | (LifecycleState::Recomputable, LifecycleState::Reclaimable)
                | (LifecycleState::Recomputable, LifecycleState::Retired)
                | (LifecycleState::Recomputable, LifecycleState::Persisted)
                | (LifecycleState::Reclaimable, LifecycleState::Retired)
                | (LifecycleState::Reclaimable, LifecycleState::Resident)
                | (LifecycleState::Reclaimable, LifecycleState::Recomputable)
                | (LifecycleState::Reclaimable, LifecycleState::Persisted)
                | (LifecycleState::Quarantined, LifecycleState::Recomputable)
                | (LifecycleState::Quarantined, LifecycleState::Retired)
                | (LifecycleState::Invalid, LifecycleState::Recomputable)
                | (LifecycleState::Invalid, LifecycleState::Retired)
        );
        if legal {
            Ok(())
        } else {
            Err(KvError::conflict(format!(
                "illegal lifecycle transition: {} to {}",
                self.as_str(),
                to.as_str()
            )))
        }
    }
    pub fn is_terminal(self) -> bool {
        matches!(self, LifecycleState::Retired)
    }
    /// Whether this state may be served to an inference engine (reusable).
    pub fn is_reusable(self) -> bool {
        matches!(self, LifecycleState::Resident | LifecycleState::Committed)
    }
}

/// Persistence durability state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PersistenceState {
    NonDurable,
    DurablePending,
    Durable,
    Failed,
}

impl PersistenceState {
    pub fn as_str(self) -> &'static str {
        match self {
            PersistenceState::NonDurable => "non_durable",
            PersistenceState::DurablePending => "durable_pending",
            PersistenceState::Durable => "durable",
            PersistenceState::Failed => "failed",
        }
    }
    pub fn code(self) -> u8 {
        match self {
            PersistenceState::NonDurable => 0,
            PersistenceState::DurablePending => 1,
            PersistenceState::Durable => 2,
            PersistenceState::Failed => 3,
        }
    }
    pub fn from_code(c: u8) -> Option<PersistenceState> {
        Some(match c {
            0 => PersistenceState::NonDurable,
            1 => PersistenceState::DurablePending,
            2 => PersistenceState::Durable,
            3 => PersistenceState::Failed,
            _ => return Option::None,
        })
    }
}

/// Integrity state of an object/blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IntegrityState {
    Unverified,
    Verified,
    Corrupt,
    Quarantined,
}

impl IntegrityState {
    pub fn as_str(self) -> &'static str {
        match self {
            IntegrityState::Unverified => "unverified",
            IntegrityState::Verified => "verified",
            IntegrityState::Corrupt => "corrupt",
            IntegrityState::Quarantined => "quarantined",
        }
    }
    pub fn code(self) -> u8 {
        match self {
            IntegrityState::Unverified => 0,
            IntegrityState::Verified => 1,
            IntegrityState::Corrupt => 2,
            IntegrityState::Quarantined => 3,
        }
    }
    pub fn from_code(c: u8) -> Option<IntegrityState> {
        Some(match c {
            0 => IntegrityState::Unverified,
            1 => IntegrityState::Verified,
            2 => IntegrityState::Corrupt,
            3 => IntegrityState::Quarantined,
            _ => return Option::None,
        })
    }
}

/// Replica validity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ReplicaState {
    Primary,
    Valid,
    Stale,
    Superseded,
    Undergoing,
}

impl ReplicaState {
    pub fn as_str(self) -> &'static str {
        match self {
            ReplicaState::Primary => "primary",
            ReplicaState::Valid => "valid",
            ReplicaState::Stale => "stale",
            ReplicaState::Superseded => "superseded",
            ReplicaState::Undergoing => "undergoing",
        }
    }
    pub fn code(self) -> u8 {
        match self {
            ReplicaState::Primary => 0,
            ReplicaState::Valid => 1,
            ReplicaState::Stale => 2,
            ReplicaState::Superseded => 3,
            ReplicaState::Undergoing => 4,
        }
    }
    pub fn from_code(c: u8) -> Option<ReplicaState> {
        Some(match c {
            0 => ReplicaState::Primary,
            1 => ReplicaState::Valid,
            2 => ReplicaState::Stale,
            3 => ReplicaState::Superseded,
            4 => ReplicaState::Undergoing,
            _ => return Option::None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn lifecycle_legal_transitions() {
        assert!(LifecycleState::Constructing
            .check_transition(LifecycleState::Committed)
            .is_ok());
        assert!(LifecycleState::Committed
            .check_transition(LifecycleState::Resident)
            .is_ok());
        assert!(LifecycleState::Resident
            .check_transition(LifecycleState::Demoted)
            .is_ok());
        assert!(LifecycleState::Persisted
            .check_transition(LifecycleState::Restoring)
            .is_ok());
        assert!(LifecycleState::Restoring
            .check_transition(LifecycleState::Resident)
            .is_ok());
    }

    #[test]
    fn lifecycle_illegal_is_error() {
        let e = LifecycleState::Constructing
            .check_transition(LifecycleState::Retired)
            .unwrap_err();
        assert_eq!(e.kind(), ErrorKind::Conflict);
        assert!(LifecycleState::Retired
            .check_transition(LifecycleState::Committed)
            .is_err());
        assert!(LifecycleState::Demoted
            .check_transition(LifecycleState::Resident)
            .is_err());
    }

    #[test]
    fn residency_codes_roundtrip() {
        for s in [
            ResidencyState::Absent,
            ResidencyState::AcceleratorResident,
            ResidencyState::HostResident,
            ResidencyState::StorageResident,
            ResidencyState::Migrating,
            ResidencyState::Promoting,
            ResidencyState::Demoting,
            ResidencyState::Persisting,
            ResidencyState::Restoring,
            ResidencyState::Replicated,
            ResidencyState::Invalid,
            ResidencyState::Reclaimable,
            ResidencyState::Retired,
        ] {
            assert_eq!(ResidencyState::from_code(s.code()), Some(s));
        }
    }
}
