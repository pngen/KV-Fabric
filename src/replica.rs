//! Replica management.
//!
//! Read replicas let a node serve reusable state without the canonical owner.
//! A replica is only valid if its generation, authority epoch, boot identity
//! and integrity match; a stale replica is never served as current.

use std::collections::HashMap;

use crate::id::{Generation, NodeId, ObjectId};
use crate::lifecycle::ReplicaState;

/// A record of a replica for an object.
#[derive(Debug, Clone, PartialEq)]
pub struct Replica {
    pub node: NodeId,
    pub boot_id: u128,
    pub generation: Generation,
    pub state: ReplicaState,
    pub verified_at_epoch: u64,
}

/// The replica registry for live objects.
#[derive(Default)]
pub struct ReplicaRegistry {
    replicas: HashMap<ObjectId, Vec<Replica>>,
}

impl ReplicaRegistry {
    pub fn new() -> ReplicaRegistry {
        ReplicaRegistry::default()
    }

    pub fn register(&mut self, object_id: ObjectId, rep: Replica) {
        let v = self.replicas.entry(object_id).or_default();
        v.retain(|r| !(r.node == rep.node && r.boot_id == rep.boot_id));
        v.push(rep);
    }

    /// Invalidate a replica for an object after mutation / generation change.
    pub fn invalidate(&mut self, object_id: ObjectId, node: NodeId) {
        if let Some(v) = self.replicas.get_mut(&object_id) {
            for r in v.iter_mut() {
                if r.node == node {
                    r.state = ReplicaState::Superseded;
                }
            }
        }
    }

    pub fn invalidate_all(&mut self, object_id: ObjectId) {
        if let Some(v) = self.replicas.get_mut(&object_id) {
            for r in v.iter_mut() {
                r.state = ReplicaState::Superseded;
            }
        }
    }

    /// Valid replicas for an object at a given generation. Never a stale gen.
    pub fn valid_replicas(&self, object_id: ObjectId, generation: Generation) -> Vec<Replica> {
        self.replicas.get(&object_id)
            .map(|v| v.iter().filter(|r| r.generation == generation && r.state == ReplicaState::Valid).cloned().collect())
            .unwrap_or_default()
    }

    pub fn for_object(&self, object_id: ObjectId) -> Vec<Replica> {
        self.replicas.get(&object_id).cloned().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn register_and_validity_by_generation() {
        let mut reg = ReplicaRegistry::new();
        let oid = ObjectId::from_u128(9);
        reg.register(oid, Replica { node: NodeId(1), boot_id: 0xa, generation: Generation::new(2), state: ReplicaState::Valid, verified_at_epoch: 0 });
        reg.register(oid, Replica { node: NodeId(2), boot_id: 0xb, generation: Generation::new(2), state: ReplicaState::Valid, verified_at_epoch: 0 });
        // A generation-1 request must not see the generation-2 replicas.
        assert!(reg.valid_replicas(oid, Generation::new(1)).is_empty());
        assert_eq!(reg.valid_replicas(oid, Generation::new(2)).len(), 2);
        // Mutating invalidates all replicas.
        reg.invalidate_all(oid);
        assert!(reg.valid_replicas(oid, Generation::new(2)).is_empty());
    }
    #[test]
    fn stale_generation_replica_never_served() {
        let mut reg = ReplicaRegistry::new();
        let oid = ObjectId::from_u128(3);
        reg.register(oid, Replica { node: NodeId(1), boot_id: 0xa, generation: Generation::new(1), state: ReplicaState::Valid, verified_at_epoch: 0 });
        // Object is now at generation 3; the gen-1 replica is stale.
        assert!(reg.valid_replicas(oid, Generation::new(3)).is_empty());
    }
}