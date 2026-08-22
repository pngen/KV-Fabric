//! Public API facade.
//!
//! `KvFabric` ties a coordinator and one or more node runtimes together into a
//! single coherent, in-process runtime that exercises the full object lifecycle
//! with real authority, durability, integrity and residency policy. It is what
//! the examples and the integration tests use; the CLI drives the same
//! machinery in separate coordinator/node processes.

use std::collections::HashMap;
use std::sync::Arc;

use crate::compat::CompatibilitySpec;
use crate::config::Config;
use crate::coordinator::Coordinator;
use crate::error::{KvError, KvResult};
use crate::id::{Generation, NodeId, ObjectId, Token};
use crate::metadata::KvMetadata;
use crate::node::NodeRuntime;
use crate::time::{Clock, SystemClock};

/// How the runtime was configured.
#[derive(Debug, Clone)]
pub struct KvFabricOptions {
    pub node_count: usize,
    pub data_dir: String,
    pub accelerator_capacity_bytes: Option<u64>,
    pub host_capacity_bytes: Option<u64>,
    pub cuda_enabled: bool,
}

impl Default for KvFabricOptions {
    fn default() -> Self {
        KvFabricOptions { node_count: 1, data_dir: "./kvfabric-data".into(), accelerator_capacity_bytes: None, host_capacity_bytes: None, cuda_enabled: false }
    }
}

/// The runtime facade.
pub struct KvFabric {
    coord: Arc<Coordinator>,
    nodes: Vec<Arc<NodeRuntime>>,
    opts: KvFabricOptions,
}

impl KvFabric {
    /// Construct a single-node runtime with an in-process coordinator.
    pub fn new(opts: KvFabricOptions) -> KvResult<KvFabric> {
        Self::with_options(opts)
    }

    pub fn with_options(opts: KvFabricOptions) -> KvResult<KvFabric> {
        let mut cfg = Config::default();
        cfg.accelerator_capacity_bytes = opts.accelerator_capacity_bytes;
        if let Some(h) = opts.host_capacity_bytes {
            cfg.host_capacity_bytes = h;
        }
        cfg.cuda_enabled = opts.cuda_enabled;
        cfg.data_dir = std::path::PathBuf::from(&opts.data_dir);
        cfg.persistence_enabled = true;
        let clock: Arc<dyn Clock> = Arc::new(SystemClock);
        let coord = Arc::new(Coordinator::new(Arc::new(cfg.clone()), clock.clone(), None)?);
        let mut nodes = Vec::new();
        for i in 0..opts.node_count.max(1) {
            let id = NodeId::from_u64(1 + i as u64);
            let boot = crate::rng::os_rng().next_u64() as u128;
            let dir = std::path::PathBuf::from(&opts.data_dir).join(format!("node-{}", i));
            let node = Arc::new(NodeRuntime::new(Arc::new(cfg.clone()), id, boot, dir)?);
            let addr = format!("127.0.0.1:{}", 7000 + i as u16).parse().unwrap();
            let _ = coord.register(id, boot, addr, vec![format!("node{}", i)]);
            nodes.push(node);
        }
        Ok(KvFabric { coord, nodes, opts })
    }

    pub fn coordinator(&self) -> &Coordinator { &self.coord }
    pub fn options(&self) -> &KvFabricOptions { &self.opts }
    pub fn node_count(&self) -> usize { self.nodes.len() }
    pub fn node(&self, i: usize) -> Option<&NodeRuntime> { self.nodes.get(i).map(|n| n.as_ref()) }
    pub fn primary(&self) -> &NodeRuntime { self.nodes[0].as_ref() }

    pub fn new_object_metadata(&self, namespace: &str, spec: &CompatibilitySpec) -> KvMetadata {
        let mut meta = KvMetadata::new(self.coord.next_object_id(), Generation::one(), namespace);
        meta.set_compat(spec);
        meta
    }

    /// Begin construction of an object on the primary node.
    pub fn create_object(&self, namespace: &str, meta: KvMetadata) -> KvResult<ObjectId> {
        self.primary().create_object(namespace, meta)
    }

    pub fn append_block(&self, object_id: ObjectId, index: u32, payload: &[u8]) -> KvResult<()> {
        self.primary().append_block(object_id, index, payload)
    }

    /// Commit a constructed object and register it with the coordinator.
    pub fn commit(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token], object_id: ObjectId) -> KvResult<KvMetadata> {
        self.primary().commit_object(namespace, compat, tokens, object_id)?;
        let meta = self.primary().object_metadata(object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        let _ = self.coord.register_object(&meta, self.primary().id(), self.primary().boot_id)?;
        Ok(meta)
    }

    /// Compute and return the longest reusable prefix across all nodes. A
    /// namespace+compat mismatch is never a hit.
    pub fn lookup(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token]) -> KvResult<LookupOutcome> {
        let mut best: Option<crate::prefix::PrefixMatch> = None;
        for node in &self.nodes {
            if let Some(m) = node.lookup(namespace, compat, tokens) {
                if best.as_ref().map(|b| m.token_len > b.token_len).unwrap_or(true) {
                    best = Some(m);
                }
            }
        }
        match best {
            Some(m) => {
                self.primary().telemetry().record_hit();
                self.primary().telemetry().record_tokens_reused(m.token_len as u64);
                let objects = m.candidates.iter().map(|c| c.object_id).collect();
                Ok(LookupOutcome { prefix_id: Some(m.prefix_id), token_len: m.token_len, objects })
            }
            None => {
                self.primary().telemetry().record_miss();
                Ok(LookupOutcome { prefix_id: None, token_len: 0, objects: Vec::new() })
            }
        }
    }

    pub fn index_prefix(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token], object_id: ObjectId) -> KvResult<()> {
        for node in &self.nodes {
            node.index_prefix(namespace, compat, tokens, object_id)?;
        }
        Ok(())
    }

    /// Re-associate a token sequence onto every durable object that matches the
    /// compatibility key, so a recovered object re-enters prefix service.
    pub fn reassociate(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token]) -> KvResult<()> {
        for node in &self.nodes {
            for id in node.object_ids() {
                let m = match node.object_metadata(id) { Some(m) => m, None => continue };
                if &m.compat_key == compat {
                    let _ = node.index_prefix(namespace, compat, tokens, id);
                }
            }
        }
        Ok(())
    }

    pub fn promote(&self, object_id: ObjectId) -> KvResult<()> { self.primary().promote_to_accelerator(object_id) }
    pub fn demote(&self, object_id: ObjectId) -> KvResult<()> { self.primary().demote_to_host(object_id) }
    pub fn persist(&self, object_id: ObjectId) -> KvResult<()> { self.primary().persist(object_id) }
    pub fn restore(&self, object_id: ObjectId) -> KvResult<()> { self.primary().restore(object_id) }
    pub fn reclaim(&self, object_id: ObjectId) -> KvResult<()> { self.primary().reclaim(object_id) }
    pub fn retire(&self, object_id: ObjectId) -> KvResult<()> { self.primary().retire(object_id) }
    pub fn verify(&self, object_id: ObjectId) -> KvResult<()> { self.primary().verify(object_id) }

    /// Micro-replica: copy a persisted object to a secondary node.
    pub fn replicate_to(&self, object_id: ObjectId, to_node: usize) -> KvResult<()> {
        if to_node >= self.nodes.len() {
            return Err(KvError::invalid_arg("target node out of range"));
        }
        let payloads = self.primary().export_blocks(object_id)?;
        let mut blocks = HashMap::new();
        for (idx, p) in &payloads {
            blocks.insert(*idx, p.clone());
        }
        let meta = self.primary().object_metadata(object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        self.nodes[to_node].import_object(&meta, blocks)?;
        let _ = self.primary().telemetry().record_replica_fetch();
        Ok(())
    }

    /// Migrate an object from node `from` to node `to`.
    pub fn migrate(&self, object_id: ObjectId, from: usize, to: usize) -> KvResult<()> {
        if from >= self.nodes.len() || to >= self.nodes.len() {
            return Err(KvError::invalid_arg("node out of range"));
        }
        let caller = self.nodes[from].id();
        let caller_boot = self.nodes[from].boot_id;
        let res = crate::migration::migrate(&self.coord, caller, caller_boot, &self.nodes[from], &self.nodes[to], object_id)
            .map_err(|e| KvError::new(crate::error::ErrorKind::Conflict, format!("migration {}: {}", e.stage, e.error)))?;
        self.primary().telemetry().record_migration();
        let _ = res;
        Ok(())
    }

    pub fn stats(&self) -> RuntimeStats {
        let t = self.primary().snapshot();
        RuntimeStats {
            object_count: self.primary().object_count(),
            coordinator_epoch: self.coord.epoch(),
            host_used: self.primary().host_used(),
            device_used: self.primary().device_used(),
            storage_used: self.primary().storage_used(),
            hits: t.hits,
            misses: t.misses,
            exact_hits: t.exact_hits,
            promotions: t.promotions,
            demotions: t.demotions,
            storage_restores: t.storage_restores,
            migrations: t.migrations,
            recomputations: t.recomputations,
            integrity_failures: t.integrity_failures,
            objects_admitted: t.objects_admitted,
            bytes_transferred: t.bytes_transferred,
        }
    }
}

/// Result of a prefix lookup.
#[derive(Debug, Clone, PartialEq)]
pub struct LookupOutcome {
    pub prefix_id: Option<crate::id::PrefixId>,
    pub token_len: usize,
    pub objects: Vec<ObjectId>,
}

/// A snapshot of runtime statistics.
#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeStats {
    pub object_count: usize,
    pub coordinator_epoch: u64,
    pub host_used: u64,
    pub device_used: u64,
    pub storage_used: u64,
    pub hits: u64,
    pub misses: u64,
    pub exact_hits: u64,
    pub promotions: u64,
    pub demotions: u64,
    pub storage_restores: u64,
    pub migrations: u64,
    pub recomputations: u64,
    pub integrity_failures: u64,
    pub objects_admitted: u64,
    pub bytes_transferred: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lifecycle_end_to_end() {
        let mut opts = KvFabricOptions::default();
        opts.data_dir = std::env::temp_dir().join(format!("kvfabric-fab-{}", std::process::id())).to_string_lossy().into_owned();
        let _ = std::fs::remove_dir_all(&opts.data_dir);
        let f = KvFabric::new(opts).unwrap();
        let spec = CompatibilitySpec { model_identity: "llama".into(), model_revision: "8b".into(), ..Default::default() };
        let meta = f.new_object_metadata("ns", &spec);
        let id = meta.object_id;
        f.create_object("ns", meta).unwrap();
        f.append_block(id, 0, &[4u8; 64]).unwrap();
        f.append_block(id, 1, &[5u8; 64]).unwrap();
        let compat = spec.key();
        let tokens = vec![10, 20, 30, 40];
        let m = f.commit("ns", &compat, &tokens, id).unwrap();
        assert_eq!(m.block_count, 2);
        let out = f.lookup("ns", &compat, &tokens).unwrap();
        assert_eq!(out.token_len, 4);
        assert_eq!(out.objects, vec![id]);
        f.promote(id).unwrap();
        f.demote(id).unwrap();
        f.persist(id).unwrap();
        f.reclaim(id).unwrap();
        f.restore(id).unwrap();
        f.verify(id).unwrap();
        let s = f.stats();
        assert!(s.hits >= 1);
        assert_eq!(s.object_count, 1);
        // Incompatible spec must not hit.
        let spec2 = CompatibilitySpec { model_identity: "gemma".into(), ..Default::default() };
        let no = f.lookup("ns", &spec2.key(), &tokens).unwrap();
        assert_eq!(no.token_len, 0);
        let _ = std::fs::remove_dir_all(&f.opts.data_dir);
    }
}