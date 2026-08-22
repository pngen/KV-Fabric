//! Leases and mutation authority.
//!
//! Distributed state requires explicit authority. Read leases and write
//! (mutation) authority expire and must be renewed. A stale lease (from a
//! previous node boot or a previous authority epoch) is rejected. Reusable
//! committed prefix state is immutable; mutable construction state is not
//! globally reusable until committed.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::error::{KvError, KvResult};
use crate::id::{LeaseId, NodeId, ObjectId};
use crate::time::Clock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LeaseKind {
    Read,
    Write,
}

impl LeaseKind {
    pub fn as_str(self) -> &'static str {
        match self {
            LeaseKind::Read => "read",
            LeaseKind::Write => "write",
        }
    }
}

/// A single lease grant.
#[derive(Debug, Clone, PartialEq)]
pub struct Lease {
    pub id: LeaseId,
    pub holder: NodeId,
    pub holder_boot: u128,
    pub object_id: ObjectId,
    pub kind: LeaseKind,
    pub authority_epoch: u64,
    pub expires_epoch: u64,
}

impl Lease {
    pub fn is_expired(&self, now_epoch: u64) -> bool {
        now_epoch >= self.expires_epoch
    }
    pub fn belongs_to(&self, node: NodeId, boot: u128) -> bool {
        self.holder == node && self.holder_boot == boot
    }
}

/// A registry of leases for live objects.
#[derive(Default)]
pub struct LeaseRegistry {
    leases: RwLock<HashMap<ObjectId, Vec<Lease>>>,
}

impl LeaseRegistry {
    pub fn new() -> LeaseRegistry {
        LeaseRegistry::default()
    }

    fn prune<C: Clock + ?Sized>(map: &mut HashMap<ObjectId, Vec<Lease>>, object: ObjectId, clock: &C) {
        if let Some(v) = map.get_mut(&object) {
            v.retain(|l| !l.is_expired(clock.epoch_secs()));
            if v.is_empty() {
                map.remove(&object);
            }
        }
    }

    pub fn acquire<C: Clock + ?Sized>(
        &self,
        object: ObjectId,
        holder: NodeId,
        holder_boot: u128,
        kind: LeaseKind,
        epoch: u64,
        ttl: u64,
        clock: &C,
    ) -> KvResult<Lease> {
        let mut map = self.leases.write().map_err(|_| KvError::internal("lease lock poisoned"))?;
        Self::prune(&mut map, object, clock);
        if kind == LeaseKind::Write {
            if let Some(v) = map.get(&object) {
                if let Some(existing) = v.iter().find(|l| l.kind == LeaseKind::Write) {
                    if !existing.belongs_to(holder, holder_boot) {
                        return Err(KvError::unauthorized(format!(
                            "object {} already has a write lease held by node {}",
                            object, existing.holder
                        )));
                    }
                }
            }
        }
        let id = crate::id::random_lease_id(&mut crate::rng::os_rng());
        let expires = clock.epoch_secs().saturating_add(ttl);
        let lease = Lease { id, holder, holder_boot, object_id: object, kind, authority_epoch: epoch, expires_epoch: expires };
        map.entry(object).or_default().push(lease.clone());
        Ok(lease)
    }

    pub fn renew<C: Clock + ?Sized>(&self, object: ObjectId, lease_id: LeaseId, ttl: u64, clock: &C) -> KvResult<()> {
        let mut map = self.leases.write().map_err(|_| KvError::internal("lease lock poisoned"))?;
        Self::prune(&mut map, object, clock);
        let v = map.get_mut(&object).ok_or_else(|| KvError::stale_lease("no leases for object"))?;
        let lease = v.iter_mut().find(|l| l.id == lease_id).ok_or_else(|| KvError::stale_lease("lease not found"))?;
        if lease.is_expired(clock.epoch_secs()) {
            return Err(KvError::stale_lease("lease already expired"));
        }
        lease.expires_epoch = clock.epoch_secs().saturating_add(ttl);
        Ok(())
    }

    pub fn release(&self, object: ObjectId, lease_id: LeaseId, holder: NodeId, boot: u128) -> KvResult<()> {
        let mut map = self.leases.write().map_err(|_| KvError::internal("lease lock poisoned"))?;
        if let Some(v) = map.get_mut(&object) {
            let before = v.len();
            v.retain(|l| !(l.id == lease_id && l.holder == holder && l.holder_boot == boot));
            if v.len() == before {
                // Nothing matched this holder/boot pair: reject the stale authority.
                return Err(KvError::stale_lease("no matching live lease for this holder"));
            }
            if v.is_empty() {
                map.remove(&object);
            }
            Ok(())
        } else {
            Err(KvError::not_found("object has no leases"))
        }
    }

    pub fn has_live<C: Clock + ?Sized>(&self, object: ObjectId, holder: NodeId, boot: u128, kind: LeaseKind, clock: &C) -> bool {
        let Ok(mut map) = self.leases.write() else {
            return false;
        };
        Self::prune(&mut map, object, clock);
        map.get(&object)
            .map(|v| v.iter().any(|l| l.kind == kind && l.belongs_to(holder, boot) && !l.is_expired(clock.epoch_secs())))
            .unwrap_or(false)
    }

    pub fn is_leased<C: Clock + ?Sized>(&self, object: ObjectId, clock: &C) -> bool {
        let Ok(mut map) = self.leases.write() else {
            return false;
        };
        Self::prune(&mut map, object, clock);
        map.get(&object).map(|v| !v.is_empty()).unwrap_or(false)
    }

    pub fn live_lease_count<C: Clock + ?Sized>(&self, object: ObjectId, clock: &C) -> usize {
        let Ok(mut map) = self.leases.write() else {
            return 0;
        };
        Self::prune(&mut map, object, clock);
        map.get(&object).map(|v| v.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::MockClock;

    fn obj() -> ObjectId {
        ObjectId::from_u128(0x555)
    }

    #[test]
    fn acquire_and_release_roundtrip() {
        let reg = LeaseRegistry::new();
        let c = MockClock::new();
        let l = reg.acquire(obj(), NodeId(1), 0xaa, LeaseKind::Read, 1, 10, &c).unwrap();
        assert!(reg.has_live(obj(), NodeId(1), 0xaa, LeaseKind::Read, &c));
        reg.release(obj(), l.id, NodeId(1), 0xaa).unwrap();
        assert!(!reg.has_live(obj(), NodeId(1), 0xaa, LeaseKind::Read, &c));
    }

    #[test]
    fn stale_boot_is_rejected() {
        let reg = LeaseRegistry::new();
        let c = MockClock::new();
        let l = reg.acquire(obj(), NodeId(1), 0xaa, LeaseKind::Read, 1, 10, &c).unwrap();
        assert!(!reg.has_live(obj(), NodeId(1), 0xbb, LeaseKind::Read, &c));
        assert!(reg.release(obj(), l.id, NodeId(1), 0xbb).is_err());
    }

    #[test]
    fn write_conflict_rejected() {
        let reg = LeaseRegistry::new();
        let c = MockClock::new();
        reg.acquire(obj(), NodeId(1), 0xaa, LeaseKind::Write, 1, 10, &c).unwrap();
        let e = reg.acquire(obj(), NodeId(2), 0xbb, LeaseKind::Write, 1, 10, &c).unwrap_err();
        assert_eq!(e.kind(), crate::error::ErrorKind::Unauthorized);
        reg.acquire(obj(), NodeId(1), 0xaa, LeaseKind::Write, 1, 10, &c).unwrap();
    }

    #[test]
    fn expiry_invalidates() {
        let reg = LeaseRegistry::new();
        let c = MockClock::new();
        reg.acquire(obj(), NodeId(1), 0xaa, LeaseKind::Read, 1, 5, &c).unwrap();
        assert!(reg.has_live(obj(), NodeId(1), 0xaa, LeaseKind::Read, &c));
        c.set_epoch(100);
        assert!(!reg.has_live(obj(), NodeId(1), 0xaa, LeaseKind::Read, &c));
    }
}