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
use crate::error::{ErrorKind, KvError, KvResult};
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
            let mut objects = node.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
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
                // Recovered bytes are host resident only while the host tier
                // can hold them. An object that does not fit stays durable and
                // restorable instead of overcommitting the tier.
                match node.admit_to_host(&objects, meta.object_id, &blocks) {
                    Ok(()) => meta.residency = ResidencyState::HostResident,
                    Err(e) if e.kind() == ErrorKind::Capacity => {
                        blocks.clear();
                        meta.lifecycle = LifecycleState::Reclaimable;
                        meta.residency = ResidencyState::Reclaimable;
                    }
                    Err(e) => return Err(e),
                }
                objects.insert(meta.object_id, LocalObject { meta, blocks, ref_count: 0 });
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

    /// Write `blocks` of `object_id` to the host tier. Entries created by a
    /// call that is refused part-way are removed again.
    fn write_host(&self, object_id: ObjectId, blocks: &HashMap<u32, Vec<u8>>) -> KvResult<()> {
        let mut created: Vec<Vec<u8>> = Vec::new();
        for (idx, payload) in blocks {
            let key = tier_key(object_id, *idx);
            let fresh = !self.host.exists(&key);
            if let Err(e) = self.host.write(&key, payload) {
                for k in &created {
                    let _ = self.host.remove(k);
                }
                return Err(e);
            }
            if fresh {
                created.push(key);
            }
        }
        Ok(())
    }

    /// Make `blocks` the host-resident copy of `object_id` while the registry
    /// is held: they must fit the resident budget next to everything else that
    /// is resident (the copy being replaced excluded), and are then charged to
    /// the host tier.
    fn admit_to_host(&self, objects: &HashMap<ObjectId, LocalObject>, object_id: ObjectId, blocks: &HashMap<u32, Vec<u8>>) -> KvResult<()> {
        let incoming: u64 = blocks.values().map(|b| b.len() as u64).sum();
        let replaced: u64 = objects.get(&object_id).map(|o| o.blocks.values().map(|b| b.len() as u64).sum()).unwrap_or(0);
        let others = resident_bytes(objects).saturating_sub(replaced);
        if others.saturating_add(incoming) > self.config.host_capacity_bytes {
            return Err(KvError::capacity("host capacity exceeded"));
        }
        self.write_host(object_id, blocks)
    }

    /// Drop the tier entries of a resident copy that `kept` replaces as the
    /// host-resident copy: its accelerator entries, and any host entry the new
    /// copy does not overwrite.
    fn release_replaced(&self, object_id: ObjectId, previous: &LocalObject, kept: &HashMap<u32, Vec<u8>>) {
        for idx in previous.blocks.keys() {
            let key = tier_key(object_id, *idx);
            let _ = self.device.remove(&key);
            if !kept.contains_key(idx) {
                let _ = self.host.remove(&key);
            }
        }
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
        // The global resident budget is checked under the same write guard
        // that performs the insert: a check made under a separate read lock
        // lets concurrent appends all pass it and overshoot the capacity.
        let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let existing = resident_bytes(&objects);
        let new_bytes = existing.saturating_add(payload.len() as u64);
        if new_bytes > self.config.host_capacity_bytes {
            return Err(KvError::capacity("host capacity exceeded"));
        }
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
        // The prefix is indexed under the namespace the object was created in.
        if o.meta.namespace != namespace {
            return Err(KvError::rejected("namespace does not match the object"));
        }
        // Finalize block metadata + integrity.
        let mut block_ids = Vec::new();
        let mut shas = Vec::new();
        let mut total = 0u64;
        let mut indices: Vec<u32> = o.blocks.keys().copied().collect();
        indices.sort();
        // A committed block is addressed by its position in the block list
        // (the order the durable store returns blocks in), so the resident
        // copy is numbered the same way.
        let mut ordered = HashMap::with_capacity(indices.len());
        for (pos, idx) in indices.iter().enumerate() {
            let payload = o.blocks.get(idx).cloned().ok_or_else(|| KvError::corrupt("missing block"))?;
            block_ids.push(block_id_for(object_id, pos as u32));
            shas.push(crate::hash::sha256(&payload));
            total += payload.len() as u64;
            ordered.insert(pos as u32, payload);
        }
        self.write_host(object_id, &ordered)?;
        o.blocks = ordered;
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
                Some(o) => {
                    // Re-association restores the token index of a committed
                    // object. It never makes construction state reusable and
                    // never moves an object into another namespace or
                    // compatibility identity.
                    if o.meta.lifecycle == LifecycleState::Constructing {
                        return Err(KvError::rejected("object is not committed"));
                    }
                    if o.meta.namespace != namespace || o.meta.compat_key != *compat {
                        return Err(KvError::rejected("namespace or compatibility identity does not match the object"));
                    }
                    o.meta.generation
                }
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
        // A transferred copy is admitted only when it is complete and matches
        // the committed content digest; it is never marked verified on trust.
        let mut blocks = blocks;
        let mut indices: Vec<u32> = blocks.keys().copied().collect();
        indices.sort();
        let mut ordered = HashMap::with_capacity(indices.len());
        let mut shas = Vec::with_capacity(indices.len());
        for (pos, idx) in indices.iter().enumerate() {
            let payload = blocks.remove(idx).ok_or_else(|| KvError::corrupt("missing block"))?;
            shas.push(crate::hash::sha256(&payload));
            ordered.insert(pos as u32, payload);
        }
        if ordered.len() != meta.block_ids.len() || crate::integrity::content_digest(&shas) != meta.content_digest {
            self.telemetry.record_integrity_failure();
            return Err(KvError::integrity("imported object does not match its content digest"));
        }
        let mut meta = meta.clone();
        meta.owner = self.node_id;
        meta.owner_boot_id = self.boot_id;
        meta.residency = ResidencyState::HostResident;
        meta.lifecycle = LifecycleState::Resident;
        meta.integrity = IntegrityState::Verified;
        let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        self.admit_to_host(&objects, meta.object_id, &ordered)?;
        if let Some(previous) = objects.get(&meta.object_id) {
            self.release_replaced(meta.object_id, previous, &ordered);
        }
        objects.insert(meta.object_id, LocalObject { meta, blocks: ordered, ref_count: 0 });
        Ok(())
    }

    /// Promote an object to the accelerator tier (copy bytes host -> device).
    pub fn promote_to_accelerator(&self, object_id: ObjectId) -> KvResult<()> {
        // One registry guard covers the whole promotion, so a reclaim or
        // retire of the object cannot interleave with the tier writes.
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let o = owned.get_mut(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        if o.meta.lifecycle != LifecycleState::Resident && o.meta.lifecycle != LifecycleState::Committed {
            return Err(KvError::rejected("object not resident"));
        }
        let mut created: Vec<Vec<u8>> = Vec::new();
        for (idx, payload) in &o.blocks {
            let key = tier_key(object_id, *idx);
            let fresh = !self.device.exists(&key);
            if let Err(e) = self.device.store(&key, payload) {
                // A refused promotion leaves nothing behind on the accelerator.
                for k in &created {
                    let _ = self.device.remove(k);
                }
                return Err(e);
            }
            if fresh {
                created.push(key);
            }
        }
        o.meta.residency = ResidencyState::AcceleratorResident;
        o.meta.lifecycle = LifecycleState::Resident;
        self.telemetry.record_promotion();
        Ok(())
    }

    /// Demote an object to the host tier (copy bytes device -> host).
    pub fn demote_to_host(&self, object_id: ObjectId) -> KvResult<()> {
        // One registry guard covers the whole demotion (see promotion).
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let o = owned.get_mut(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        // Only bytes that are on the accelerator can be demoted; an object
        // that holds none there must not come out marked host resident.
        if o.meta.residency != ResidencyState::AcceleratorResident {
            return Err(KvError::rejected("object is not accelerator resident"));
        }
        // Read every block back before either tier is changed, so a failed
        // read leaves the object fully on the accelerator.
        let mut loaded = Vec::with_capacity(o.blocks.len());
        for idx in o.blocks.keys() {
            let key = tier_key(object_id, *idx);
            let payload = self.device.load(&key)?;
            loaded.push((key, payload));
        }
        for (key, payload) in &loaded {
            self.host.write(key, payload)?;
        }
        for (key, _) in &loaded {
            let _ = self.device.remove(key);
        }
        o.meta.residency = ResidencyState::HostResident;
        o.meta.lifecycle = LifecycleState::Demoted;
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
        if meta.lifecycle == LifecycleState::Constructing {
            return Err(KvError::rejected("object is not committed"));
        }
        if blocks.len() == meta.block_ids.len() {
            meta.persistence = PersistenceState::DurablePending;
            // Block `i` of the resident copy is stored under the i-th recorded
            // block id: that is the name the durable store reads it back by.
            let mut refs: Vec<(&BlockId, &[u8])> = Vec::with_capacity(blocks.len());
            for (pos, bid) in meta.block_ids.iter().enumerate() {
                let payload = blocks.get(&(pos as u32)).ok_or_else(|| KvError::corrupt("missing block"))?;
                refs.push((bid, payload.as_slice()));
            }
            self.durable.store_object(&meta, &refs)?;
        } else if !(blocks.is_empty() && self.durable.contains(object_id)) {
            // Without a complete resident copy there is nothing to make
            // durable; only an object that already is durable may pass.
            return Err(KvError::rejected("object has no complete resident copy to persist"));
        }
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        match owned.get_mut(&object_id) {
            Some(o2) => {
                o2.meta.persistence = PersistenceState::Durable;
                o2.meta.lifecycle = LifecycleState::Persisted;
            }
            None => {
                // Retired while its durable copy was being written: take the
                // copy out again so the object cannot return after a restart.
                let _ = self.durable.remove_object(object_id);
                return Err(KvError::not_found("object retired during persist"));
            }
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
        // Only what crossed the durable boundary (the manifest) is restored.
        // This also turns away a copy read just before a retire removed the
        // object, so a retired object is never brought back.
        if !self.durable.contains(object_id) {
            return Err(KvError::not_found("object is not in the durable store"));
        }
        // The restored copy becomes host resident: it is charged to the host
        // tier, and refused when the tier cannot hold it.
        self.admit_to_host(&owned, object_id, &blocks)?;
        if let Some(previous) = owned.get(&object_id) {
            self.release_replaced(object_id, previous, &blocks);
        }
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
        // Free resident bytes from the host/device tiers and mark reclaimable
        // under one registry guard, so no tier write can land in between.
        let mut owned = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let o = owned.get_mut(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        if o.ref_count > 0 {
            return Err(KvError::busy("object is actively leased"));
        }
        for idx in o.blocks.keys() {
            let key = tier_key(object_id, *idx);
            let _ = self.host.remove(&key);
            let _ = self.device.remove(&key);
        }
        o.blocks.clear();
        o.meta.lifecycle = LifecycleState::Reclaimable;
        o.meta.residency = ResidencyState::Reclaimable;
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
            // A retired object gives its host and accelerator bytes back.
            for idx in o.blocks.keys() {
                let key = tier_key(object_id, *idx);
                let _ = self.host.remove(&key);
                let _ = self.device.remove(&key);
            }
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
                                    // A reply that cannot be framed is reported, never dropped:
                                    // the peer would otherwise wait for an answer that never comes.
                                    if let Err(e) = write_frame(&mut stream, frame.request_id, Op::Reply, &b.into_vec()) {
                                        let _ = write_err(&mut stream, frame.request_id, &e);
                                    }
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

/// Identity of block `index` of `object_id`.
///
/// A block id names the block's durable file and its host / accelerator tier
/// entries, so it has to be unique to the object. Deriving it from the node
/// and the index alone gave every object on a node the same ids.
fn block_id_for(object_id: ObjectId, index: u32) -> BlockId {
    let mut seed = [0u8; 20];
    seed[..16].copy_from_slice(&object_id.to_u128().to_be_bytes());
    seed[16..].copy_from_slice(&index.to_be_bytes());
    let digest = crate::hash::sha256(&seed);
    let mut id = [0u8; 16];
    id.copy_from_slice(&digest[..16]);
    BlockId::from_u128(u128::from_be_bytes(id))
}

/// Host / accelerator tier key of block `index` of `object_id`.
fn tier_key(object_id: ObjectId, index: u32) -> Vec<u8> {
    key_of(block_id_for(object_id, index))
}

/// Bytes held resident by every object in the registry.
fn resident_bytes(objects: &HashMap<ObjectId, LocalObject>) -> u64 {
    objects.values().map(|o| o.blocks.values().map(|b| b.len()).sum::<usize>() as u64).sum()
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

    const COMPAT: [u8; 32] = [7u8; 32];

    /// A fresh directory per test: the tests of this module run concurrently
    /// in one process, so they must not share (and wipe) one another's store.
    fn dir_for(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("kvfabric-node-{}-{}", tag, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn open(dir: &std::path::Path, id: u64, host_capacity: u64, accelerator_capacity: Option<u64>) -> NodeRuntime {
        let mut cfg = Config::default();
        cfg.host_capacity_bytes = host_capacity;
        cfg.accelerator_capacity_bytes = accelerator_capacity;
        cfg.persistence_enabled = true;
        NodeRuntime::new(Arc::new(cfg), NodeId(id), 0xaa, dir.to_path_buf()).unwrap()
    }

    fn node(tag: &str) -> NodeRuntime {
        open(&dir_for(tag), 1, 1 << 20, None)
    }

    /// Create, fill and commit an object in namespace "ns" under `COMPAT`.
    fn build(n: &NodeRuntime, seed: u64, blocks: &[Vec<u8>]) -> ObjectId {
        let meta = KvMetadata::new(crate::id::random_object_id(&mut crate::rng::deterministic_rng(seed)), Generation::one(), "ns");
        let id = meta.object_id;
        n.create_object("ns", meta).unwrap();
        for (i, b) in blocks.iter().enumerate() {
            n.append_block(id, i as u32, b).unwrap();
        }
        n.commit_object("ns", &COMPAT, &[seed as u32, 1, 2], id).unwrap();
        id
    }

    fn resident(n: &NodeRuntime, id: ObjectId) -> u64 {
        n.export_blocks(id).unwrap().values().map(|p| p.len() as u64).sum()
    }

    #[test]
    fn persisted_objects_do_not_overwrite_each_other() {
        let dir = dir_for("two-durable");
        let (a, b);
        {
            let n = open(&dir, 1, 1 << 20, None);
            a = build(&n, 10, &[vec![1u8; 128], vec![2u8; 64]]);
            b = build(&n, 11, &[vec![3u8; 128], vec![4u8; 64]]);
            n.persist(a).unwrap();
            n.persist(b).unwrap();
            // Each durable copy restores to its own content.
            n.reclaim(a).unwrap();
            n.reclaim(b).unwrap();
            n.restore(a).unwrap();
            n.verify(a).unwrap();
            n.restore(b).unwrap();
            n.verify(b).unwrap();
            assert_eq!(n.export_blocks(a).unwrap()[&0], vec![1u8; 128]);
            assert_eq!(n.export_blocks(b).unwrap()[&0], vec![3u8; 128]);
        }
        // And both survive a restart.
        let n = open(&dir, 1, 1 << 20, None);
        assert_eq!(n.object_count(), 2);
        n.verify(a).unwrap();
        n.verify(b).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn objects_keep_separate_tier_entries() {
        let dir = dir_for("tiers");
        let n = open(&dir, 1, 1 << 20, Some(1 << 20));
        let a = build(&n, 20, &[vec![1u8; 100]]);
        let b = build(&n, 21, &[vec![2u8; 100]]);
        assert_eq!(n.host_used(), 200);
        n.promote_to_accelerator(a).unwrap();
        n.promote_to_accelerator(b).unwrap();
        assert_eq!(n.device_used(), 200);
        // Reclaiming one object releases only its own bytes.
        n.reclaim(a).unwrap();
        assert_eq!(n.host_used(), 100);
        assert_eq!(n.device_used(), 100);
        n.demote_to_host(b).unwrap();
        n.verify(b).unwrap();
        assert_eq!(n.device_used(), 0);
        assert_eq!(n.host_used(), 100);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn imported_object_is_durable_on_the_importing_node() {
        let dir = dir_for("import-durable");
        let src = open(&dir.join("src"), 1, 1 << 20, None);
        let id = build(&src, 30, &[vec![5u8; 96], vec![6u8; 96]]);
        let meta = src.object_metadata(id).unwrap();
        {
            let dst = open(&dir.join("dst"), 2, 1 << 20, None);
            dst.import_object(&meta, src.export_blocks(id).unwrap()).unwrap();
            dst.persist(id).unwrap();
            dst.reclaim(id).unwrap();
            dst.restore(id).unwrap();
            dst.verify(id).unwrap();
        }
        let dst = open(&dir.join("dst"), 2, 1 << 20, None);
        dst.verify(id).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn persist_needs_a_complete_committed_copy() {
        let dir = dir_for("persist-guard");
        let n = open(&dir, 1, 1 << 20, None);
        // Under construction: nothing is committed yet.
        let building = ObjectId::from_u128(0x51);
        n.create_object("ns", KvMetadata::new(building, Generation::one(), "ns")).unwrap();
        n.append_block(building, 0, &[1u8; 32]).unwrap();
        assert_eq!(n.persist(building).unwrap_err().kind(), ErrorKind::Rejected);
        // Reclaimed before it was ever persisted: the bytes are gone.
        let lost = build(&n, 40, &[vec![9u8; 64]]);
        n.reclaim(lost).unwrap();
        assert_eq!(n.persist(lost).unwrap_err().kind(), ErrorKind::Rejected);
        assert_ne!(n.object_metadata(lost).unwrap().persistence, PersistenceState::Durable);
        assert_eq!(n.storage_used(), 0, "nothing may be reported durable");
        // Already durable: persisting the reclaimed object again is harmless.
        let kept = build(&n, 41, &[vec![8u8; 64]]);
        n.persist(kept).unwrap();
        n.reclaim(kept).unwrap();
        n.persist(kept).unwrap();
        n.restore(kept).unwrap();
        n.verify(kept).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_is_bounded_by_and_charged_to_the_host_tier() {
        let dir = dir_for("restore-cap");
        let n = open(&dir, 1, 1024, None);
        let a = build(&n, 50, &[vec![1u8; 600]]);
        n.persist(a).unwrap();
        n.reclaim(a).unwrap();
        let b = build(&n, 51, &[vec![2u8; 600]]);
        assert_eq!(n.host_used(), 600);
        // The tier cannot hold both: the restore is refused, not overcommitted.
        assert_eq!(n.restore(a).unwrap_err().kind(), ErrorKind::Capacity);
        assert_eq!(n.host_used(), 600);
        assert_eq!(resident(&n, a), 0);
        // Once there is room, the restored bytes are accounted for.
        n.reclaim(b).unwrap();
        n.restore(a).unwrap();
        n.verify(a).unwrap();
        assert_eq!(n.host_used(), 600);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn recovery_keeps_resident_bytes_within_host_capacity() {
        let dir = dir_for("recover-cap");
        let (a, b);
        {
            let n = open(&dir, 1, 1024, None);
            a = build(&n, 60, &[vec![1u8; 600]]);
            n.persist(a).unwrap();
            n.reclaim(a).unwrap();
            b = build(&n, 61, &[vec![2u8; 600]]);
            n.persist(b).unwrap();
        }
        let n = open(&dir, 1, 1024, None);
        assert_eq!(n.object_count(), 2);
        assert!(resident(&n, a) + resident(&n, b) <= 1024, "recovery must not overcommit the host tier");
        assert_eq!(n.host_used(), resident(&n, a) + resident(&n, b));
        // The object that did not fit is still durable, and restorable once
        // there is room for it.
        let (held, waiting) = if resident(&n, a) > 0 { (a, b) } else { (b, a) };
        assert_eq!(resident(&n, held), 600);
        assert_eq!(n.object_metadata(waiting).unwrap().residency, ResidencyState::Reclaimable);
        assert_eq!(n.restore(waiting).unwrap_err().kind(), ErrorKind::Capacity);
        n.reclaim(held).unwrap();
        n.restore(waiting).unwrap();
        n.verify(waiting).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retire_gives_back_every_tier() {
        let dir = dir_for("retire");
        let n = open(&dir, 1, 1024, Some(1 << 20));
        let a = build(&n, 70, &[vec![1u8; 256], vec![2u8; 256], vec![3u8; 256], vec![4u8; 256]]);
        n.promote_to_accelerator(a).unwrap();
        n.persist(a).unwrap();
        n.retire(a).unwrap();
        assert_eq!(n.object_count(), 0);
        assert_eq!(n.host_used(), 0);
        assert_eq!(n.device_used(), 0);
        assert_eq!(std::fs::read_dir(dir.join("blocks")).unwrap().count(), 0, "block files of a retired object remain");
        // The freed host bytes are usable again.
        let b = build(&n, 71, &[vec![5u8; 512]]);
        n.verify(b).unwrap();
        assert_eq!(n.host_used(), 512);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn import_admits_only_a_complete_copy_matching_the_digest() {
        let dir = dir_for("import-verify");
        let src = open(&dir.join("src"), 1, 1 << 20, None);
        let dst = open(&dir.join("dst"), 2, 1 << 20, None);
        let id = build(&src, 80, &[vec![1u8; 64], vec![2u8; 64]]);
        let meta = src.object_metadata(id).unwrap();
        // A flipped byte.
        let mut tampered = src.export_blocks(id).unwrap();
        tampered.get_mut(&1).unwrap()[0] ^= 0xff;
        assert_eq!(dst.import_object(&meta, tampered).unwrap_err().kind(), ErrorKind::Integrity);
        // A missing block.
        let mut partial = src.export_blocks(id).unwrap();
        partial.remove(&1);
        assert_eq!(dst.import_object(&meta, partial).unwrap_err().kind(), ErrorKind::Integrity);
        // No blocks at all (the source copy was reclaimed).
        assert_eq!(dst.import_object(&meta, HashMap::new()).unwrap_err().kind(), ErrorKind::Integrity);
        assert_eq!(dst.object_count(), 0);
        assert_eq!(dst.host_used(), 0);
        // The genuine copy is admitted.
        dst.import_object(&meta, src.export_blocks(id).unwrap()).unwrap();
        dst.verify(id).unwrap();
        assert_eq!(dst.host_used(), 128);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn refused_promotion_leaves_nothing_on_the_accelerator() {
        let dir = dir_for("promote-fail");
        let n = open(&dir, 1, 1 << 20, Some(1000));
        let big = build(&n, 90, &[vec![1u8; 400], vec![2u8; 400], vec![3u8; 400]]);
        assert_eq!(n.promote_to_accelerator(big).unwrap_err().kind(), ErrorKind::Capacity);
        assert_eq!(n.device_used(), 0);
        assert_eq!(n.object_metadata(big).unwrap().residency, ResidencyState::HostResident);
        // The accelerator is still fully available to an object that fits.
        let small = build(&n, 91, &[vec![4u8; 400], vec![5u8; 400]]);
        n.promote_to_accelerator(small).unwrap();
        assert_eq!(n.device_used(), 800);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn demote_is_refused_unless_the_object_is_on_the_accelerator() {
        let dir = dir_for("demote-guard");
        let n = open(&dir, 1, 1 << 20, Some(1 << 20));
        let id = build(&n, 100, &[vec![1u8; 64]]);
        n.persist(id).unwrap();
        n.reclaim(id).unwrap();
        // A reclaimed object holds no bytes anywhere: it cannot be "demoted"
        // into host residency.
        assert_eq!(n.demote_to_host(id).unwrap_err().kind(), ErrorKind::Rejected);
        let meta = n.object_metadata(id).unwrap();
        assert_eq!(meta.residency, ResidencyState::Reclaimable);
        assert_eq!(meta.lifecycle, LifecycleState::Reclaimable);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reassociation_cannot_move_an_object_to_another_namespace_or_model() {
        let dir = dir_for("reassociate");
        let n = open(&dir, 1, 1 << 20, None);
        let id = build(&n, 110, &[vec![1u8; 64]]);
        let tokens = [9u32, 8, 7];
        let other_model = [8u8; 32];
        assert_eq!(n.index_prefix("ns", &other_model, &tokens, id).unwrap_err().kind(), ErrorKind::Rejected);
        assert!(n.lookup("ns", &other_model, &tokens).is_none(), "hit under a different compatibility key");
        assert_eq!(n.index_prefix("other-ns", &COMPAT, &tokens, id).unwrap_err().kind(), ErrorKind::Rejected);
        assert!(n.lookup("other-ns", &COMPAT, &tokens).is_none(), "hit in a different namespace");
        assert_eq!(n.object_metadata(id).unwrap().compat_key, COMPAT);
        // Re-association under the object's own identity still works.
        n.index_prefix("ns", &COMPAT, &tokens, id).unwrap();
        assert!(n.lookup("ns", &COMPAT, &tokens).is_some());
        // An object under construction is not reusable, and is committed in
        // the namespace it was created in.
        let building = ObjectId::from_u128(0x111);
        n.create_object("ns", KvMetadata::new(building, Generation::one(), "ns")).unwrap();
        assert_eq!(n.index_prefix("ns", &[0u8; 32], &tokens, building).unwrap_err().kind(), ErrorKind::Rejected);
        assert_eq!(n.commit_object("other-ns", &COMPAT, &tokens, building).unwrap_err().kind(), ErrorKind::Rejected);
        n.commit_object("ns", &COMPAT, &tokens, building).unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn concurrent_appends_never_exceed_host_capacity() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Barrier;
        const THREADS: usize = 8;
        const ROUNDS: usize = 500;
        let dir = dir_for("append-race");
        // Room for exactly one 600-byte block.
        let n = open(&dir, 1, 1000, None);
        // Every round each worker appends to an object of its own.
        let id_of = |round: usize, worker: usize| ObjectId::from_u128(0x1000 + (round * THREADS + worker) as u128);
        let start = Barrier::new(THREADS + 1);
        let done = Barrier::new(THREADS + 1);
        let admitted = AtomicUsize::new(0);
        // Nothing asserts while the workers are parked on a barrier; the worst
        // round is recorded and checked after they have all finished.
        let mut most_resident = 0u64;
        let mut most_admitted = 0usize;
        std::thread::scope(|s| {
            for worker in 0..THREADS {
                let (n, start, done, admitted) = (&n, &start, &done, &admitted);
                s.spawn(move || {
                    for round in 0..ROUNDS {
                        start.wait();
                        if n.append_block(id_of(round, worker), 0, &[7u8; 600]).is_ok() {
                            admitted.fetch_add(1, Ordering::SeqCst);
                        }
                        done.wait();
                    }
                });
            }
            for round in 0..ROUNDS {
                for worker in 0..THREADS {
                    let _ = n.create_object("ns", KvMetadata::new(id_of(round, worker), Generation::one(), "ns"));
                }
                admitted.store(0, Ordering::SeqCst);
                start.wait();
                done.wait();
                most_admitted = most_admitted.max(admitted.load(Ordering::SeqCst));
                most_resident = most_resident.max(n.objects.read().map(|m| resident_bytes(&m)).unwrap_or(u64::MAX));
                // Empty the tier again for the next round.
                for worker in 0..THREADS {
                    let _ = n.reclaim(id_of(round, worker));
                }
            }
        });
        assert!(most_resident <= 1000, "resident bytes reached {} with a capacity of 1000", most_resident);
        assert_eq!(most_admitted, 1, "exactly one 600-byte block fits");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tier_accounting_matches_the_registry_under_concurrent_operations() {
        const THREADS: u64 = 4;
        const OPS: usize = 1500;
        let dir = dir_for("tier-race");
        let n = open(&dir, 1, 1 << 20, Some(1 << 20));
        let ids = [
            build(&n, 120, &[vec![1u8; 200], vec![2u8; 200]]),
            build(&n, 121, &[vec![3u8; 300]]),
        ];
        for id in ids {
            n.persist(id).unwrap();
        }
        std::thread::scope(|s| {
            for t in 0..THREADS {
                let n = &n;
                s.spawn(move || {
                    let mut rng = crate::rng::deterministic_rng(1000 + t);
                    for _ in 0..OPS {
                        let id = ids[rng.below(2) as usize];
                        // Many of these are refused for the object's current
                        // state; only the accounting afterwards matters.
                        let _ = match rng.below(4) {
                            0 => n.promote_to_accelerator(id),
                            1 => n.demote_to_host(id),
                            2 => n.reclaim(id),
                            _ => n.restore(id),
                        };
                    }
                });
            }
        });
        // Every tier byte belongs to a resident block, and the reverse.
        let check = |when: &str| {
            let objects = n.objects.read().unwrap();
            let bytes = |o: &LocalObject| o.blocks.values().map(|b| b.len() as u64).sum::<u64>();
            let on_host: u64 = objects.values().map(bytes).sum();
            let on_device: u64 = objects.values().filter(|o| o.meta.residency == ResidencyState::AcceleratorResident).map(bytes).sum();
            assert_eq!(n.host_used(), on_host, "host tier bytes do not match the resident blocks ({})", when);
            assert_eq!(n.device_used(), on_device, "accelerator bytes do not match the promoted objects ({})", when);
        };
        check("after the concurrent phase");
        // Whatever state the race left them in, both objects can be made
        // resident again and are then fully accounted for.
        for id in ids {
            n.restore(id).unwrap();
        }
        check("with both objects restored");
        assert_eq!(n.host_used(), 700);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_needs_a_manifest_entry() {
        let dir = dir_for("restore-manifest");
        let n = open(&dir, 1, 1 << 20, None);
        let id = build(&n, 130, &[vec![1u8; 64]]);
        n.persist(id).unwrap();
        n.reclaim(id).unwrap();
        // Metadata and block files without a manifest entry are a commit that
        // never crossed the durable boundary, or what a retire left behind.
        std::fs::remove_file(dir.join("manifest")).unwrap();
        assert_eq!(n.restore(id).unwrap_err().kind(), ErrorKind::NotFound);
        assert_eq!(resident(&n, id), 0);
        assert_eq!(n.host_used(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_object_retired_during_persist_does_not_come_back() {
        const OBJECTS: u64 = 60;
        let dir = dir_for("persist-retire");
        {
            let n = open(&dir, 1, 1 << 20, None);
            let (tx, rx) = std::sync::mpsc::channel::<ObjectId>();
            std::thread::scope(|s| {
                // Owned by this closure, so the retiring thread always sees
                // the channel close and ends.
                let tx = tx;
                let n = &n;
                s.spawn(move || {
                    for id in rx {
                        let _ = n.retire(id);
                    }
                });
                for i in 0..OBJECTS {
                    let id = build(n, 200 + i, &[vec![i as u8; 64]]);
                    // Retire and persist the same object at the same time.
                    let _ = tx.send(id);
                    let _ = n.persist(id);
                }
            });
            assert_eq!(n.object_count(), 0);
        }
        let n = open(&dir, 1, 1 << 20, None);
        assert_eq!(n.object_count(), 0, "a retired object was recovered from the durable store");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_reply_too_large_to_frame_is_reported_to_the_peer() {
        use crate::protocol::{encode_fetch_blocks, read_frame, write_frame, FetchBlocksMsg, Op, MAX_FRAME_PAYLOAD};
        let dir = dir_for("fetch-oversize");
        let n = open(&dir, 1, 256 << 20, None);
        // One block as large as the biggest frame: with its length prefix the
        // reply no longer fits a frame.
        let id = ObjectId::from_u128(0x222);
        n.create_object("ns", KvMetadata::new(id, Generation::one(), "ns")).unwrap();
        n.append_block(id, 0, &vec![0u8; MAX_FRAME_PAYLOAD]).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let mut first = None;
        std::thread::scope(|s| {
            let (n, listener) = (&n, &listener);
            s.spawn(move || {
                if let Ok((conn, _)) = listener.accept() {
                    let _ = n.serve_conn(conn);
                }
            });
            // The fetch, then a request the node can always answer: whichever
            // reply arrives first shows whether the fetch was answered at all.
            if let Ok(mut peer) = TcpStream::connect(addr) {
                let fetch = encode_fetch_blocks(&FetchBlocksMsg { object_id: id, from_generation: 0 });
                if write_frame(&mut peer, 1, Op::FetchBlocks, &fetch).is_ok() && write_frame(&mut peer, 2, Op::Status, b"").is_ok() {
                    first = read_frame(&mut peer).ok().map(|f| (f.request_id, f.op));
                }
            }
        });
        assert_eq!(first, Some((1, Op::ErrCode)), "the fetch must be answered with a structured error");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_commit_lookup() {
        let n = node("create-commit-lookup");
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
        let n = node("persist-restore-cycles");
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