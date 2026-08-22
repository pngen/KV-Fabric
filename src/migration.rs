//! Migration between nodes.
//!
//! Migration follows a disciplined sequence: plan - reserve - transfer - verify
//! - commit - retire old authority. Destination state is never exposed as
//! authoritative before verification and commit. Failure leaves the system
//! recoverable, and retry is only attempted where idempotency is proven.

use crate::coordinator::Coordinator;
use crate::id::{Generation, NodeId, ObjectId};
use crate::node::NodeRuntime;

/// The result of a migration.
#[derive(Debug, Clone, PartialEq)]
pub struct MigrationResult {
    pub object_id: ObjectId,
    pub new_owner: NodeId,
    pub new_generation: Generation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MigrationError {
    pub stage: &'static str,
    pub error: String,
}

impl MigrationError {
    fn new(stage: &'static str, error: String) -> MigrationError {
        MigrationError { stage, error }
    }
}

/// Perform a migration of an object from the source node to the destination
/// node, coordinated through the authority of the coordinator.
pub fn migrate(
    coord: &Coordinator,
    caller_node: NodeId,
    caller_boot: u128,
    source: &NodeRuntime,
    destination: &NodeRuntime,
    object_id: ObjectId,
) -> Result<MigrationResult, MigrationError> {
    // 1. PLAN + RESERVE: confirm the caller is the current owner in the current
    //    epoch, and the destination is not the source.
    let (owner, owner_boot, epoch) = coord.owner_of(object_id)
        .ok_or_else(|| MigrationError::new("plan", "no authoritative owner".into()))?;
    if owner != caller_node || owner_boot != caller_boot {
        return Err(MigrationError::new("plan", "caller is not the owner".into()));
    }
    if source.id() == destination.id() {
        return Err(MigrationError::new("plan", "source and destination are the same node".into()));
    }
    let meta = coord.object_metadata(object_id)
        .ok_or_else(|| MigrationError::new("plan", "object metadata missing".into()))?;

    // 2. TRANSFER: export blocks from the source and import (reserve + stage at
    //    destination). Destination is staged, not yet authoritative.
    let blocks = source.export_blocks(object_id)
        .map_err(|e| MigrationError::new("transfer", e.message.clone()))?;
    if blocks.is_empty() && meta.block_count > 0 {
        return Err(MigrationError::new("transfer", "source has no resident blocks".into()));
    }
    destination.import_object(&meta, blocks)
        .map_err(|e| MigrationError::new("transfer", e.message.clone()))?;

    // 3. VERIFY: recompute the content digest at the destination.
    destination.verify(object_id)
        .map_err(|e| MigrationError::new("verify", e.message.clone()))?;

    // 4. COMMIT: transfer canonical ownership to the destination with a bumped
    //    generation, through the coordinator authority.
    let old_gen = meta.generation;
    let new_gen = old_gen.next();
    coord.migrate_commit(object_id, destination.id(), destination.boot_id, new_gen, caller_node, caller_boot)
        .map_err(|e| MigrationError::new("commit", e.message.clone()))?;

    // 5. RETIRE old authority: the source may now drop the authoritative copy
    //    (it becomes a replica at best).
    let _ = source.reclaim(object_id);
    let _ = epoch;
    Ok(MigrationResult { object_id, new_owner: destination.id(), new_generation: new_gen })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::time::MockClock;
    use std::sync::Arc;

    fn make_node(id: u64, dir: &std::path::Path) -> NodeRuntime {
        let mut c = Config::default();
        c.host_capacity_bytes = 1 << 20;
        c.persistence_enabled = true;
        NodeRuntime::new(Arc::new(c), NodeId(id), id as u128, dir.to_path_buf()).unwrap()
    }

    #[test]
    fn migrate_transfers_ownership() {
        let dir = std::env::temp_dir().join(format!("kvfabric-mig-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = Arc::new(Config::default());
        let coord = Coordinator::new(cfg, Arc::new(MockClock::new()), None).unwrap();
        let a = make_node(1, &dir.join("a"));
        let b = make_node(2, &dir.join("b"));
        let _ = coord.register(a.id(), a.boot_id, "127.0.0.1:7001".parse().unwrap(), vec![]).unwrap();
        let _ = coord.register(b.id(), b.boot_id, "127.0.0.1:7002".parse().unwrap(), vec![]).unwrap();
        // Create + commit an object on A and register it with the coordinator.
        let mut meta = crate::metadata::KvMetadata::new(crate::id::random_object_id(&mut crate::rng::deterministic_rng(3)), Generation::one(), "ns");
        meta.compat_key = [1u8; 32];
        let id = meta.object_id;
        a.create_object("ns", meta).unwrap();
        a.append_block(id, 0, &[3u8; 64]).unwrap();
        a.commit_object("ns", &[1u8; 32], &vec![1, 2], id).unwrap();
        let am = a.object_metadata(id).unwrap();
        coord.register_object(&am, a.id(), a.boot_id).unwrap();
        // Migrate A -> B.
        let res = migrate(&coord, a.id(), a.boot_id, &a, &b, id).unwrap();
        assert_eq!(res.new_owner, b.id());
        assert_eq!(coord.owner_of(id).map(|(o, _, _)| o), Some(b.id()));
        // B now has the object and it verifies.
        b.verify(id).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}