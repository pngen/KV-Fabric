//! Node runtime: local management of reusable inference state.
//!
//! A node reports capacity, owns a local object registry, manages residency
//! across the host / accelerator / storage tiers, enforces leases, performs
//! transfers and integrity verification, and can serve remote peers over the
//! framed TCP control plane. A node restart recovers durable local metadata
//! from the durable store but does not imply continuing prior authority.

use std::collections::HashMap;
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};

use crate::accelerator::DeviceStore;
use crate::config::Config;
use crate::error::{KvError, KvResult};
use crate::id::{BlockId, NodeId, ObjectId, Token};
use crate::lifecycle::{IntegrityState, LifecycleState, PersistenceState, ResidencyState, Tier};
use crate::metadata::KvMetadata;
use crate::prefix::{PrefixMatch, PrefixStore};
use crate::storage::TierStore;
use crate::telemetry::{Telemetry, TelemetrySnapshot};
use crate::time::{Clock, SystemClock};

/// A locally-held object.
#[derive(Clone)]
pub struct LocalObject {
    pub meta: KvMetadata,
    /// Resident block payloads, indexed by block index.
    pub blocks: HashMap<u32, Vec<u8>>,
    pub ref_count: u64,
}

/// A node runtime.
pub struct NodeRuntime {
    config: Arc<Config>,
    clock: Arc<dyn Clock>,
    pub node_id: NodeId,
    pub boot_id: u128,
    host: crate::storage::HostStore,
    device: Arc<dyn DeviceStore>,
    durable: crate::persistence::DurableStore,
    objects: RwLock<HashMap<ObjectId, LocalObject>>,
    prefixes: PrefixStore,
    telemetry: Telemetry,
}

impl NodeRuntime {
    pub fn new(
        config: Arc<Config>,
        node_id: NodeId,
        boot_id: u128,
        durable_root: std::path::PathBuf,
    ) -> KvResult<NodeRuntime> {
        let host = crate::storage::HostStore::new(Tier::Host, config.host_capacity_bytes);
        let device = crate::accelerator::default_device_store(config.accelerator_capacity_bytes, config.cuda_enabled);
        let durable = crate::persistence::DurableStore::new(&durable_root)?;
        let prefix_store = PrefixStore::new();
        let node = NodeRuntime {
            config: config.clone(),
            clock: Arc::new(SystemClock),
            node_id,
            boot_id,
            host,
            device,
            durable,
            objects: RwLock::new(HashMap::new()),
            prefixes: prefix_store,
            telemetry: Telemetry::new(),
        };
        // Recover durable objects into the local registry but do *not* grant
        // execution authority: they are re-verified and re-admitted lazily.
        if config.persistence_enabled {
            let recovered = node.durable.recover()?;
            for (meta, payloads) in recovered {
                let mut blocks = HashMap::new();
                for (bid, payload) in payloads {
                    if let Some(idx) = meta.block_ids.iter().position(|b| *b == bid) {
                        blocks.insert(idx as u32, payload);
                    }
                }
                let mut meta = meta;
                meta.integrity = IntegrityState::Verified;
                meta.persistence = PersistenceState::Durable;
                node.objects.write().ok().map(|mut m| {
                    m.insert(meta.object_id, LocalObject { meta, blocks, ref_count: 0 });
                });
            }
        }
        Ok(node)
    }

    pub fn id(&self) -> NodeId { self.node_id }
    pub fn telemetry(&self) -> &Telemetry { &self.telemetry }
    pub fn snapshot(&self) -> TelemetrySnapshot { self.telemetry.snapshot() }

    pub fn host_used(&self) -> u64 { self.host.used() }
    pub fn device_used(&self) -> u64 { self.device.used() }
    pub fn storage_used(&self) -> u64 { self.durable.used() }
    pub fn host_capacity(&self) -> u64 { self.config.host_capacity_bytes }
    pub fn device_capacity(&self) -> u64 { self.config.accelerator_capacity_bytes.unwrap_or(0) }
    pub fn storage_capacity(&self) -> u64 { self.config.storage_capacity_bytes }

    pub fn object_count(&self) -> usize {
        self.objects.read().map(|m| m.len()).unwrap_or(0)
    }

    pub fn object_ids(&self) -> Vec<ObjectId> {
        self.objects.read().map(|m| m.keys().copied().collect()).unwrap_or_default()
    }

    fn resident_bytes(&self) -> u64 {
        self.objects.read()
            .map(|m| m.values().map(|o| o.blocks.values().map(|b| b.len()).sum::<usize>() as u64).sum())
            .unwrap_or(0)
    }

    pub fn object_metadata(&self, id: ObjectId) -> Option<KvMetadata> {
        self.objects.read().ok().and_then(|m| m.get(&id).map(|o| o.meta.clone()))
    }

    /// Begin construction of a new object (reserves identity + metadata).
    pub fn create_object(&self, namespace: &str, meta: KvMetadata) -> KvResult<ObjectId> {
        if meta.object_id.is_zero() {
            return Err(KvError::invalid_arg("object id must be set"));
        }
        let mut m = meta;
        m.namespace = namespace.to_string();
        m.lifecycle = LifecycleState::Constructing;
        m.residency = ResidencyState::Absent;
        m.created_epoch = self.clock.epoch_secs();
        m.owner = self.node_id;
        m.owner_boot_id = self.boot_id;
        m.validate()?;
        let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        if objects.contains_key(&m.object_id) {
            return Err(KvError::already_exists("object already exists"));
        }
        let object_id = m.object_id;
        objects.insert(object_id, LocalObject { meta: m, blocks: HashMap::new(), ref_count: 0 });
        Ok(object_id)
    }

    /// Append a block payload to a constructing object.
    pub fn append_block(&self, object_id: ObjectId, index: u32, payload: &[u8]) -> KvResult<()> {
        // Check the global resident budget BEFORE taking the write lock, so we
        // never re-enter the same RwLock (read inside write) and deadlock.
        let existing = self.resident_bytes();
        let new_bytes = existing.saturating_add(payload.len() as u64);
        if new_bytes > self.config.host_capacity_bytes {
            return Err(KvError::capacity("host capacity exceeded"));
        }
        let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let o = objects.get_mut(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        if o.meta.lifecycle != LifecycleState::Constructing {
            return Err(KvError::rejected("object is not under construction"));
        }
        o.blocks.insert(index, payload.to_vec());
        Ok(())
    }

    /// Commit a constructed object: finalize metadata, verify integrity, index
    /// its reusable prefix and make it (potentially) reusable.
    pub fn commit_object(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token], object_id: ObjectId) -> KvResult<()> {
        let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let o = objects.get_mut(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        if o.meta.lifecycle != LifecycleState::Constructing {
            return Err(KvError::rejected("object not under construction"));
        }
        // Finalize block metadata + integrity.
        let mut block_ids = Vec::new();
        let mut shas = Vec::new();
        let mut total = 0u64;
        let mut indices: Vec<u32> = o.blocks.keys().copied().collect();
        indices.sort();
        for idx in indices {
            let payload = o.blocks.get(&idx).cloned().ok_or_else(|| KvError::corrupt("missing block"))?;
            let bid = BlockId::from_u64_pair(self.node_id.to_u64(), idx as u64);
            block_ids.push(bid);
            shas.push(crate::hash::sha256(&payload));
            total += payload.len() as u64;
            self.host.write(&key_of(bid), &payload)?;
        }
        o.meta.block_ids = block_ids;
        o.meta.block_count = o.meta.block_ids.len() as u32;
        o.meta.byte_size = total;
        o.meta.content_digest = crate::integrity::content_digest(&shas);
        o.meta.integrity = IntegrityState::Verified;
        o.meta.lifecycle = LifecycleState::Committed;
        o.meta.residency = ResidencyState::HostResident;
        o.meta.compat_key = *compat;
        o.meta.token_count = tokens.len() as u32;
        o.meta.token_end = o.meta.token_start.saturating_add(o.meta.token_count);
        let _ = o.meta.policy_versions;
        // Index the reusable prefix locally.
        let _ = self.prefixes.commit(namespace, compat, tokens, object_id, o.meta.generation, self.clock.epoch_secs());
        Ok(())
    }

    /// Longest reusable-prefix lookup in the local registry.
    pub fn lookup(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token]) -> Option<PrefixMatch> {
        self.prefixes.longest_prefix(namespace, compat, tokens)
    }

    pub fn exact_lookup(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token]) -> Option<PrefixMatch> {
        self.prefixes.exact(namespace, compat, tokens)
    }

    /// Re-associate a token sequence with an existing object so it can be found
    /// by prefix lookup (required after recovery/migration, since metadata does
    /// not store the token list).
    pub fn index_prefix(&self, namespace: &str, compat: &[u8; 32], tokens: &[Token], object_id: ObjectId) -> KvResult<()> {
        // Tolerant: a node that does not currently hold the object simply skips,
        // so re-association can be broadcast across a cluster safely.
        let generation = {
            let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
            match objects.get(&object_id) {
                Some(o) => o.meta.generation,
                None => return Ok(()),
            }
        };
        {
            let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
            if let Some(o2) = objects.get_mut(&object_id) {
                o2.meta.compat_key = *compat;
                o2.meta.token_count = tokens.len() as u32;
                o2.meta.token_end = o2.meta.token_start.saturating_add(tokens.len() as u32);
            }
        }
        self.prefixes.commit(namespace, compat, tokens, object_id, generation, self.clock.epoch_secs()).map(|_| ())
    }

    /// Export the resident block payloads of an object (for migration/replica).
    pub fn export_blocks(&self, object_id: ObjectId) -> KvResult<HashMap<u32, Vec<u8>>> {
        let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let o = objects.get(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        if o.ref_count > 0 {
            return Err(KvError::busy("object is actively leased"));
        }
        Ok(o.blocks.clone())
    }

    /// Import a complete object (metadata + blocks) into this node.
    pub fn import_object(&self, meta: &KvMetadata, blocks: HashMap<u32, Vec<u8>>) -> KvResult<()> {
        for (idx, payload) in &blocks {
            let bid = BlockId::from_u64_pair(self.node_id.to_u64(), *idx as u64);
            self.host.write(&key_of(bid), payload)?;
        }
        let mut meta = meta.clone();
        meta.owner = self.node_id;
        meta.owner_boot_id = self.boot_id;
        meta.residency = ResidencyState::HostResident;
        meta.lifecycle = LifecycleState::Resident;
        meta.integrity = IntegrityState::Verified;
        let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        objects.insert(meta.object_id, LocalObject { meta, blocks, ref_count: 0 });
        Ok(())
    }

    /// Promote an object to the accelerator tier (copy bytes host -> device).
    pub fn promote_to_accelerator(&self, object_id: ObjectId) -> KvResult<()> {
        // Snapshot under the read lock, then release it before acquiring the
        // write lock (re-entering the same RwLock would self-deadlock).
        let (lifecycle, blocks) = {
            let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
            let o = objects.get(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
            (o.meta.lifecycle, o.blocks.clone())
        };
        if lifecycle != LifecycleState::Resident && lifecycle != LifecycleState::Committed {
            return Err(KvError::rejected("object not resident"));
        }
        for (idx, payload) in &blocks {
            let bid = BlockId::from_u64_pair(self.node_id.to_u64(), *idx as u64);
            self.device.store(&key_of(bid), payload)?;
        }
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        if let Some(o2) = owned.get_mut(&object_id) {
            o2.meta.residency = ResidencyState::AcceleratorResident;
            o2.meta.lifecycle = LifecycleState::Resident;
        }
        self.telemetry.record_promotion();
        Ok(())
    }

    /// Demote an object to the host tier (copy bytes device -> host).
    pub fn demote_to_host(&self, object_id: ObjectId) -> KvResult<()> {
        // Snapshot under the read lock, then release it before the write lock.
        let blocks = {
            let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
            let o = objects.get(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
            o.blocks.clone()
        };
        for (idx, _) in &blocks {
            let bid = BlockId::from_u64_pair(self.node_id.to_u64(), *idx as u64);
            let payload = self.device.load(&key_of(bid))?;
            self.host.write(&key_of(bid), &payload)?;
            let _ = self.device.remove(&key_of(bid));
        }
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        if let Some(o2) = owned.get_mut(&object_id) {
            o2.meta.residency = ResidencyState::HostResident;
            o2.meta.lifecycle = LifecycleState::Demoted;
        }
        self.telemetry.record_demotion();
        Ok(())
    }

    /// Persist an object durably (atomic block + metadata + manifest commits).
    pub fn persist(&self, object_id: ObjectId) -> KvResult<()> {
        // Snapshot under the read lock, then release it before the write lock.
        let (mut meta, blocks) = {
            let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
            let o = objects.get(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
            (o.meta.clone(), o.blocks.clone())
        };
        meta.persistence = PersistenceState::DurablePending;
        let ids: Vec<BlockId> = blocks.keys().map(|i| BlockId::from_u64_pair(self.node_id.to_u64(), *i as u64)).collect();
        let refs: Vec<(&BlockId, &[u8])> = ids.iter().zip(blocks.values().map(|p| p.as_slice())).collect();
        self.durable.store_object(&meta, &refs)?;
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        if let Some(o2) = owned.get_mut(&object_id) {
            o2.meta.persistence = PersistenceState::Durable;
            o2.meta.lifecycle = LifecycleState::Persisted;
        }
        self.telemetry.record_admitted(1);
        Ok(())
    }

    /// Restore a persisted object into host residency.
    pub fn restore(&self, object_id: ObjectId) -> KvResult<()> {
        let (meta, payloads) = self.durable.load_object(object_id)?;
        let mut blocks = HashMap::new();
        for (bid, payload) in payloads {
            if let Some(idx) = meta.block_ids.iter().position(|b| *b == bid) {
                blocks.insert(idx as u32, payload);
            }
        }
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        if !owned.contains_key(&object_id) {
            owned.insert(object_id, LocalObject { meta: meta.clone(), blocks, ref_count: 0 });
        } else {
            owned.get_mut(&object_id).map(|o| { o.blocks = blocks; });
        }
        if let Some(o) = owned.get_mut(&object_id) {
            o.meta.lifecycle = LifecycleState::Restoring;
            o.meta.residency = ResidencyState::HostResident;
            o.meta.lifecycle = LifecycleState::Resident;
        }
        drop(owned);
        self.telemetry.record_restore();
        Ok(())
    }

    /// Reclaim resident bytes but keep metadata (mark recomputable/reclaimable).
    pub fn reclaim(&self, object_id: ObjectId) -> KvResult<()> {
        // Free resident bytes from the host/device tiers, then mark reclaimable.
        let indices: Vec<u32> = {
            let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
            let o = objects.get(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
            if o.ref_count > 0 {
                return Err(KvError::busy("object is actively leased"));
            }
            o.blocks.keys().copied().collect()
        };
        for idx in &indices {
            let bid = BlockId::from_u64_pair(self.node_id.to_u64(), *idx as u64);
            let _ = self.host.remove(&key_of(bid));
            let _ = self.device.remove(&key_of(bid));
        }
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        if let Some(o) = owned.get_mut(&object_id) {
            o.blocks.clear();
            o.meta.lifecycle = LifecycleState::Reclaimable;
            o.meta.residency = ResidencyState::Reclaimable;
        }
        self.telemetry.record_reclaim();
        Ok(())
    }

    /// Retire an object permanently (drop from registry + durable store).
    pub fn retire(&self, object_id: ObjectId) -> KvResult<()> {
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        {
            let o = owned.get_mut(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
            if o.ref_count > 0 {
                return Err(KvError::busy("object is actively leased"));
            }
            o.meta.lifecycle = LifecycleState::Retired;
        }
        let _ = self.durable.remove_object(object_id);
        owned.remove(&object_id);
        Ok(())
    }

    /// Verify object integrity by re-reading resident/durable content.
    pub fn verify(&self, object_id: ObjectId) -> KvResult<()> {
        let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let o = objects.get(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        let meta = o.meta.clone();
        // Hash blocks in the same deterministic (sorted-index) order used at commit.
        let mut keys: Vec<u32> = o.blocks.keys().copied().collect();
        keys.sort();
        let mut shas = Vec::with_capacity(keys.len());
        for k in keys {
            let p = o.blocks.get(&k).ok_or_else(|| KvError::corrupt("missing block"))?;
            shas.push(crate::hash::sha256(p));
        }
        let digest = crate::integrity::content_digest(&shas);
        if digest != meta.content_digest {
            self.telemetry.record_integrity_failure();
            return Err(KvError::integrity("object content digest mismatch"));
        }
        Ok(())
    }

    /// Serve the control-plane on a listener. This blocks; run it in a thread.
    pub fn serve(&self, listener: TcpListener) -> KvResult<()> {
        for stream in listener.incoming() {
            let stream = match stream {
                Ok(s) => s,
                Err(_) => continue,
            };
            let _ = self.serve_conn(stream);
        }
        Ok(())
    }

    pub fn serve_conn(&self, mut stream: TcpStream) -> KvResult<()> {
        use crate::protocol::*;
                loop {
            let frame = match read_frame(&mut stream) {
                Ok(f) => f,
                Err(_) => return Ok(()),
            };
            match frame.op {
                Op::Status => {
                    let mut b = crate::bytes::Buffer::new();
                    let s = self.snapshot();
                    b.len_str(&format!("node={} objects={} host_used={} device_used={} storage_used={} hits={}",
                        self.node_id, self.object_count(), self.host_used(), self.device_used(), self.storage_used(), s.hits));
                    let _ = write_frame(&mut stream, frame.request_id, Op::Reply, &b.into_vec());
                }
                Op::Lookup => {
                    match decode_lookup(&frame.payload) {
                        Ok(msg) => {
                            let r = self.lookup(&msg.namespace, &msg.compat_key, &msg.tokens);
                            let res = LookupResult {
                                prefix_id: r.as_ref().map(|m| m.prefix_id.to_u64()),
                                token_len: r.as_ref().map(|m| m.token_len).unwrap_or(0),
                                object_ids: r.map(|m| m.candidates.iter().map(|c| c.object_id).collect()).unwrap_or_default(),
                            };
                            let _ = write_frame(&mut stream, frame.request_id, Op::Reply, &encode_lookup_result(&res));
                        }
                        Err(_) => { let _ = write_err(&mut stream, frame.request_id, &KvError::protocol("bad lookup")); }
                    }
                }
                Op::FetchBlocks => {
                    match decode_fetch_blocks(&frame.payload) {
                        Ok(msg) => {
                            let objects = self.objects.read().ok();
                            let payloads = objects.and_then(|m| m.get(&msg.object_id).cloned());
                            match payloads {
                                Some(o) => {
                                    let mut b = crate::bytes::Buffer::new();
                                    b.u32(o.blocks.len() as u32);
                                    for (idx, p) in o.blocks.iter() {
                                        b.u32(*idx);
                                        b.len_bytes(p);
                                    }
                                    let _ = write_frame(&mut stream, frame.request_id, Op::Reply, &b.into_vec());
                                }
                                None => { let _ = write_err(&mut stream, frame.request_id, &KvError::not_found("object unknown")); }
                            }
                        }
                        Err(_) => { let _ = write_err(&mut stream, frame.request_id, &KvError::protocol("bad fetch")); }
                    }
                }
                _ => { let _ = write_err(&mut stream, frame.request_id, &KvError::protocol("unhandled op")); }
            }
        }
    }
}

fn key_of(bid: BlockId) -> Vec<u8> {
    bid.to_u128().to_be_bytes().to_vec()
}

fn write_err<W: std::io::Write>(w: &mut W, request_id: u64, e: &KvError) -> KvResult<()> {
    use crate::protocol::*;
    let mut b = crate::bytes::Buffer::new();
    b.u32(e.kind() as u32);
    b.len_str(&e.message);
    write_frame(w, request_id, Op::ErrCode, &b.into_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Generation;

    fn node() -> NodeRuntime {
        let dir = std::env::temp_dir().join(format!("kvfabric-node-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = Config::default();
        cfg.host_capacity_bytes = 1 << 20;
        cfg.persistence_enabled = true;
        NodeRuntime::new(Arc::new(cfg), NodeId(1), 0xaa, dir).unwrap()
    }

    #[test]
    fn create_commit_lookup() {
        let n = node();
        let meta = KvMetadata::new(crate::id::random_object_id(&mut crate::rng::deterministic_rng(1)), Generation::one(), "ns");
        let id = meta.object_id;
        n.create_object("ns", meta).unwrap();
        n.append_block(id, 0, &[0u8; 64]).unwrap();
        n.append_block(id, 1, &[1u8; 64]).unwrap();
        let compat = [7u8; 32];
        let tokens = vec![1, 2, 3, 4];
        n.commit_object("ns", &compat, &tokens, id).unwrap();
        let m = n.lookup("ns", &compat, &tokens).unwrap();
        assert_eq!(m.token_len, 4);
        assert!(m.candidates.iter().any(|c| c.object_id == id));
        // Confirm integrity verifies.
        n.verify(id).unwrap();
    }

    #[test]
    fn persist_restore_cycles() {
        let n = node();
        let meta = KvMetadata::new(crate::id::random_object_id(&mut crate::rng::deterministic_rng(2)), Generation::one(), "ns");
        let id = meta.object_id;
        n.create_object("ns", meta).unwrap();
        n.append_block(id, 0, &[5u8; 128]).unwrap();
        let compat = [9u8; 32];
        n.commit_object("ns", &compat, &vec![1, 2], id).unwrap();
        n.persist(id).unwrap();
        n.reclaim(id).unwrap();
        n.restore(id).unwrap();
        n.verify(id).unwrap();
        n.retire(id).unwrap();
        assert_eq!(n.object_count(), 0);
    }
}