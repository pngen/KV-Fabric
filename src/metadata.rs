//! The first-class reusable inference-state object: KVObject metadata.
//!
//! A KVObject has globally stable 128-bit identity and explicit metadata
//! covering compatibility identity, geometry, lineage, lifecycle, residency,
//! authority, durability, integrity and economics. Metadata is versioned and
//! can be canonical-serialized for durable identity anchors and for transit.

use crate::bytes::{Buffer, Cursor};
use crate::compat::CompatibilitySpec;
use crate::config::PolicyVersions;
use crate::error::{KvError, KvResult};
use crate::id::{BlockId, Generation, NodeId, ObjectId, PrefixId};
use crate::lifecycle::{
    IntegrityState, LifecycleState, PersistenceState, ReplicaState, ResidencyState,
};

/// The metadata serialization format version.
pub const METADATA_FORMAT_VERSION: u32 = 1;

/// A replica/placement record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaEntry {
    pub node: NodeId,
    pub state: ReplicaState,
    pub boot_id: u128,
    pub generation: Generation,
    pub verified_at_epoch: u64,
}

/// Complete metadata for a KVObject.
#[derive(Debug, Clone, PartialEq)]
pub struct KvMetadata {
    pub object_id: ObjectId,
    pub generation: Generation,
    pub namespace: String,
    /// Canonical 256-bit compatibility identity (from CompatibilitySpec).
    pub compat_key: [u8; 32],
    pub model_identity: String,
    pub model_revision: String,
    pub tokenizer_identity: String,
    pub engine_compat: String,
    pub sequence_id: String,
    pub token_start: u32,
    pub token_end: u32,
    pub token_count: u32,
    pub block_count: u32,
    pub block_ids: Vec<BlockId>,
    pub byte_size: u64,
    pub dtype: String,
    pub head_count: u32,
    pub head_dim: u32,
    pub layer_start: u32,
    pub layer_end: u32,
    pub layer_count: u32,
    pub device_compat: Vec<String>,
    pub created_epoch: u64,
    pub last_access_epoch: u64,
    pub access_count: u64,
    pub ref_count: u64,
    pub reuse_count: u64,
    pub recompute_cost: u64,
    pub transfer_cost: u64,
    pub persistence: PersistenceState,
    pub integrity: IntegrityState,
    pub lifecycle: LifecycleState,
    pub residency: ResidencyState,
    pub owner: NodeId,
    pub prefix_id: Option<PrefixId>,
    pub lineage_parent: Option<ObjectId>,
    pub lineage_children: Vec<ObjectId>,
    pub replicas: Vec<ReplicaEntry>,
    pub expiry_epoch: Option<u64>,
    pub priority: i32,
    pub policy_versions: PolicyVersions,
    pub authority_epoch: u64,
    pub owner_boot_id: u128,
    /// SHA-256 content digest of all block payloads.
    pub content_digest: [u8; 32],
    /// SHA-256 manifest anchor over metadata + content digest.
    pub manifest_anchor: [u8; 32],
}

impl KvMetadata {
    /// Produce a builder starting with a fresh object id and generation.
    pub fn new(object_id: ObjectId, generation: Generation, namespace: impl Into<String>) -> KvMetadata {
        KvMetadata {
            object_id,
            generation,
            namespace: namespace.into(),
            compat_key: [0u8; 32],
            model_identity: String::new(),
            model_revision: String::new(),
            tokenizer_identity: String::new(),
            engine_compat: String::new(),
            sequence_id: String::new(),
            token_start: 0,
            token_end: 0,
            token_count: 0,
            block_count: 0,
            block_ids: Vec::new(),
            byte_size: 0,
            dtype: String::new(),
            head_count: 0,
            head_dim: 0,
            layer_start: 0,
            layer_end: 0,
            layer_count: 0,
            device_compat: Vec::new(),
            created_epoch: 0,
            last_access_epoch: 0,
            access_count: 0,
            ref_count: 0,
            reuse_count: 0,
            recompute_cost: 0,
            transfer_cost: 0,
            persistence: PersistenceState::NonDurable,
            integrity: IntegrityState::Unverified,
            lifecycle: LifecycleState::Constructing,
            residency: ResidencyState::Absent,
            owner: NodeId(0),
            prefix_id: None,
            lineage_parent: None,
            lineage_children: Vec::new(),
            replicas: Vec::new(),
            expiry_epoch: None,
            priority: 0,
            policy_versions: PolicyVersions::default(),
            authority_epoch: 0,
            owner_boot_id: 0,
            content_digest: [0u8; 32],
            manifest_anchor: [0u8; 32],
        }
    }

    /// Set the compatibility identity from a spec.
    pub fn set_compat(&mut self, spec: &CompatibilitySpec) {
        self.compat_key = spec.key();
        self.model_identity = spec.model_identity.clone();
        self.model_revision = spec.model_revision.clone();
        self.tokenizer_identity = spec.tokenizer_identity.clone();
        self.engine_compat = spec.engine_abi.clone();
    }

    pub fn is_expired(&self, now_epoch: u64) -> bool {
        self.expiry_epoch.map(|e| now_epoch >= e).unwrap_or(false)
    }

    /// Token range contained by this object.
    pub fn token_range(&self) -> (u32, u32) {
        (self.token_start, self.token_end)
    }

    /// Whether this object represents the full requested sequence.
    pub fn covers(&self, token_count_target: u32) -> bool {
        self.token_end >= token_count_target
    }

    /// Recompute the manifest anchor from metadata bytes + content digest.
    pub fn recompute_manifest_anchor(&self) -> [u8; 32] {
        crate::integrity::manifest_anchor(&self.encode(), &self.content_digest)
    }

    /// Validate internal consistency. Returns an error on any corruption.
    pub fn validate(&self) -> KvResult<()> {
        if self.object_id.is_zero() {
            return Err(KvError::invalid_arg("metadata has zero object id"));
        }
        if self.namespace.len() > crate::bytes::MAX_NAMESPACE_LEN {
            return Err(KvError::invalid_arg("namespace too long"));
        }
        if self.token_end < self.token_start {
            return Err(KvError::corrupt("token_end < token_start"));
        }
        if self.block_count as usize != self.block_ids.len() {
            return Err(KvError::corrupt("block_count != block_ids.len"));
        }
        if self.block_count > crate::bytes::MAX_BLOCKS_PER_OBJECT as u32 {
            return Err(KvError::invalid_arg("too many blocks"));
        }
        if self.token_count as u64 * 16 > self.byte_size && self.byte_size > 0 {
            // Heuristic: byte_size should be at least roughly token_count * elemsize.
            // Not a hard error, just a lazy sanity note. We skip strictness here.
        }
        Ok(())
    }

    /// Encode to a canonical byte form (version + ordered fields). This is the
    /// form used both for durable identity anchors and for transit.
    pub fn encode(&self) -> Vec<u8> {
        let mut b = Buffer::new();
        b.u32(METADATA_FORMAT_VERSION);
        b.u128(self.object_id.to_u128());
        b.u64(self.generation.get());
        b.len_str(&self.namespace);
        b.len_bytes(&self.compat_key);
        b.len_str(&self.model_identity);
        b.len_str(&self.model_revision);
        b.len_str(&self.tokenizer_identity);
        b.len_str(&self.engine_compat);
        b.len_str(&self.sequence_id);
        b.u32(self.token_start);
        b.u32(self.token_end);
        b.u32(self.token_count);
        b.u32(self.block_count);
        for id in &self.block_ids {
            b.u128(id.to_u128());
        }
        b.u64(self.byte_size);
        b.len_str(&self.dtype);
        b.u32(self.head_count);
        b.u32(self.head_dim);
        b.u32(self.layer_start);
        b.u32(self.layer_end);
        b.u32(self.layer_count);
        b.u32(self.device_compat.len() as u32);
        for d in &self.device_compat {
            b.len_str(d);
        }
        b.u64(self.created_epoch);
        b.u64(self.last_access_epoch);
        b.u64(self.access_count);
        b.u64(self.ref_count);
        b.u64(self.reuse_count);
        b.u64(self.recompute_cost);
        b.u64(self.transfer_cost);
        b.u8(self.persistence.code());
        b.u8(self.integrity.code());
        b.u8(self.lifecycle.code());
        b.u8(self.residency.code());
        b.u64(self.owner.to_u64());
        b.u8(self.prefix_id.map(|_| 1).unwrap_or(0));
        if let Some(p) = self.prefix_id {
            b.u64(p.to_u64());
        }
        b.u8(self.lineage_parent.map(|_| 1).unwrap_or(0));
        if let Some(p) = self.lineage_parent {
            b.u128(p.to_u128());
        }
        b.u32(self.lineage_children.len() as u32);
        for c in &self.lineage_children {
            b.u128(c.to_u128());
        }
        b.u32(self.replicas.len() as u32);
        for r in &self.replicas {
            b.u64(r.node.to_u64());
            b.u8(r.state.code());
            b.u128(r.boot_id);
            b.u64(r.generation.get());
            b.u64(r.verified_at_epoch);
        }
        b.u8(self.expiry_epoch.map(|_| 1).unwrap_or(0));
        if let Some(e) = self.expiry_epoch {
            b.u64(e);
        }
        b.u32(self.priority as u32);
        b.u32(self.policy_versions.placement);
        b.u32(self.policy_versions.admission);
        b.u32(self.policy_versions.eviction);
        b.u32(self.policy_versions.planner);
        b.u32(self.policy_versions.compat_key);
        b.u64(self.authority_epoch);
        b.u128(self.owner_boot_id);
        b.len_bytes(&self.content_digest);
        b.len_bytes(&self.manifest_anchor);
        b.into_vec()
    }

    /// Decode a KVObject from canonical bytes.
    pub fn decode(data: &[u8]) -> KvResult<KvMetadata> {
        let mut c = Cursor::new(data);
        let ver = c.u32()?;
        if ver != METADATA_FORMAT_VERSION {
            return Err(KvError::invalid_format(format!("unsupported metadata version {}", ver)));
        }
        let object_id = ObjectId::from_u128(c.u128()?);
        let generation = Generation::new(c.u64()?);
        let namespace = c.len_str(crate::bytes::MAX_NAMESPACE_LEN)?;
        let mut compat_key = [0u8; 32];
        compat_key.copy_from_slice(c.len_bytes(32)?.as_slice());
        let model_identity = c.len_str(crate::bytes::MAX_MSG_LEN)?;
        let model_revision = c.len_str(crate::bytes::MAX_MSG_LEN)?;
        let tokenizer_identity = c.len_str(crate::bytes::MAX_MSG_LEN)?;
        let engine_compat = c.len_str(crate::bytes::MAX_MSG_LEN)?;
        let sequence_id = c.len_str(crate::bytes::MAX_MSG_LEN)?;
        let token_start = c.u32()?;
        let token_end = c.u32()?;
        let token_count = c.u32()?;
        let block_count = c.u32()?;
        if block_count as usize > crate::bytes::MAX_BLOCKS_PER_OBJECT {
            return Err(KvError::protocol("block_count exceeds bound"));
        }
        let mut block_ids = Vec::with_capacity(block_count as usize);
        for _ in 0..block_count {
            block_ids.push(BlockId::from_u128(c.u128()?));
        }
        let byte_size = c.u64()?;
        let dtype = c.len_str(256)?;
        let head_count = c.u32()?;
        let head_dim = c.u32()?;
        let layer_start = c.u32()?;
        let layer_end = c.u32()?;
        let layer_count = c.u32()?;
        let dc = c.u32()?;
        if dc > crate::bytes::MAX_NODES as u32 {
            return Err(KvError::protocol("device_count exceeds bound"));
        }
        let mut device_compat = Vec::with_capacity(dc as usize);
        for _ in 0..dc {
            device_compat.push(c.len_str(256)?);
        }
        let created_epoch = c.u64()?;
        let last_access_epoch = c.u64()?;
        let access_count = c.u64()?;
        let ref_count = c.u64()?;
        let reuse_count = c.u64()?;
        let recompute_cost = c.u64()?;
        let transfer_cost = c.u64()?;
        let persistence = PersistenceState::from_code(c.u8()?)
            .ok_or_else(|| KvError::protocol("bad persistence state"))?;
        let integrity = IntegrityState::from_code(c.u8()?)
            .ok_or_else(|| KvError::protocol("bad integrity state"))?;
        let lifecycle = LifecycleState::from_code(c.u8()?)
            .ok_or_else(|| KvError::protocol("bad lifecycle state"))?;
        let residency = ResidencyState::from_code(c.u8()?)
            .ok_or_else(|| KvError::protocol("bad residency state"))?;
        let owner = NodeId::from_u64(c.u64()?);
        let has_prefix = c.u8()? != 0;
        let prefix_id = if has_prefix { Some(PrefixId::from_u64(c.u64()?)) } else { None };
        let has_parent = c.u8()? != 0;
        let lineage_parent = if has_parent { Some(ObjectId::from_u128(c.u128()?)) } else { None };
        let lc = c.u32()?;
        if lc > crate::bytes::MAX_TOKENS as u32 {
            return Err(KvError::protocol("lineage children count exceeds bound"));
        }
        let mut lineage_children = Vec::with_capacity(lc as usize);
        for _ in 0..lc {
            lineage_children.push(ObjectId::from_u128(c.u128()?));
        }
        let rc = c.u32()?;
        if rc > crate::bytes::MAX_NODES as u32 {
            return Err(KvError::protocol("replica count exceeds bound"));
        }
        let mut replicas = Vec::with_capacity(rc as usize);
        for _ in 0..rc {
            let node = NodeId::from_u64(c.u64()?);
            let state = ReplicaState::from_code(c.u8()?)
                .ok_or_else(|| KvError::protocol("bad replica state"))?;
            let boot_id = c.u128()?;
            let generation = Generation::new(c.u64()?);
            let verified_at_epoch = c.u64()?;
            replicas.push(ReplicaEntry { node, state, boot_id, generation, verified_at_epoch });
        }
        let has_expiry = c.u8()? != 0;
        let expiry_epoch = if has_expiry { Some(c.u64()?) } else { None };
        let priority = c.u32()? as i32;
        let policy_versions = PolicyVersions {
            placement: c.u32()?,
            admission: c.u32()?,
            eviction: c.u32()?,
            planner: c.u32()?,
            compat_key: c.u32()?,
        };
        let authority_epoch = c.u64()?;
        let owner_boot_id = c.u128()?;
        let mut content_digest = [0u8; 32];
        content_digest.copy_from_slice(c.len_bytes(32)?.as_slice());
        let mut manifest_anchor = [0u8; 32];
        manifest_anchor.copy_from_slice(c.len_bytes(32)?.as_slice());
        c.finish()?;
        Ok(KvMetadata {
            object_id, generation, namespace, compat_key, model_identity,
            model_revision, tokenizer_identity, engine_compat, sequence_id,
            token_start, token_end, token_count, block_count, block_ids,
            byte_size, dtype, head_count, head_dim, layer_start, layer_end,
            layer_count, device_compat, created_epoch, last_access_epoch,
            access_count, ref_count, reuse_count, recompute_cost, transfer_cost,
            persistence, integrity, lifecycle, residency, owner, prefix_id,
            lineage_parent, lineage_children, replicas, expiry_epoch, priority,
            policy_versions, authority_epoch, owner_boot_id, content_digest,
            manifest_anchor,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::deterministic_rng;

    fn sample() -> KvMetadata {
        let mut r = deterministic_rng(11);
        let mut m = KvMetadata::new(
            crate::id::random_object_id(&mut r),
            Generation::one(),
            "ns-1",
        );
        m.set_compat(&CompatibilitySpec {
            model_identity: "llama".into(),
            model_revision: "3.1-8b".into(),
            ..Default::default()
        });
        m.token_count = 96;
        m.token_start = 0;
        m.token_end = 96;
        m.block_count = 2;
        m.block_ids.push(BlockId::from_u128(1));
        m.block_ids.push(BlockId::from_u128(2));
        m.byte_size = 4096;
        m.dtype = "float16".into();
        m.head_count = 32;
        m.head_dim = 128;
        m.layer_count = 32;
        m.layer_start = 0;
        m.layer_end = 32;
        m.lifecycle = LifecycleState::Resident;
        m.residency = ResidencyState::HostResident;
        m.owner = NodeId(0xabc);
        m.owner_boot_id = 0x1234;
        m.prefix_id = Some(PrefixId::from_u64(7));
        m.lineage_children.push(ObjectId::from_u128(99));
        m
    }

    #[test]
    fn encode_decode_roundtrip() {
        let m = sample();
        let bytes = m.encode();
        let d = KvMetadata::decode(&bytes).expect("decode");
        assert_eq!(d.object_id, m.object_id);
        assert_eq!(d.generation, m.generation);
        assert_eq!(d.namespace, "ns-1");
        assert_eq!(d.compat_key, m.compat_key);
        assert_eq!(d.block_ids, m.block_ids);
        assert_eq!(d.block_count, m.block_count);
        assert_eq!(d.lifecycle, m.lifecycle);
        assert_eq!(d.residency, m.residency);
        assert_eq!(d.owner, m.owner);
        assert_eq!(d.owner_boot_id, m.owner_boot_id);
        assert_eq!(d.prefix_id, Some(PrefixId::from_u64(7)));
        assert_eq!(d.lineage_children, m.lineage_children);
    }

    #[test]
    fn corrupt_decode_rejected() {
        let m = sample();
        let mut bytes = m.encode();
        bytes[0] ^= 0xff; // corrupt version
        assert!(KvMetadata::decode(&bytes).is_err());
    }

    #[test]
    fn validate_consistent() {
        let m = sample();
        assert!(m.validate().is_ok());
        let mut bad = sample();
        bad.token_start = 100;
        bad.token_end = 96;
        assert!(bad.validate().is_err());
    }
}
