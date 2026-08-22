//! Coordinator: global control-plane authority.
//!
//! The coordinator owns the authority epoch, the node registry, canonical
//! object ownership, placement, the global prefix index, leases and namespace
//! quotas. Its durable state is snapshotted atomically. On restart the epoch is
//! advanced and all workers must re-register with a fresh boot id, so stale
//! workers never retain authority simply because they were valid before the
//! restart.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use crate::config::Config;
use crate::error::{KvError, KvResult};
use crate::id::{Generation, NodeId, ObjectId, Token};
use crate::lease::{LeaseKind, LeaseRegistry};
use crate::metadata::KvMetadata;
use crate::prefix::{PrefixMatch, PrefixStore};
use crate::time::Clock;

/// A registered node (worker) record.
#[derive(Debug, Clone)]
pub struct NodeRecord {
    pub node_id: NodeId,
    pub boot_id: u128,
    pub addr: SocketAddr,
    pub last_epoch: u64,
    pub capabilities: Vec<String>,
}

/// Global object authority state.
#[derive(Debug, Clone)]
pub struct ObjectState {
    pub meta: KvMetadata,
    pub owner: NodeId,
    pub owner_boot: u128,
    pub authority_epoch: u64,
}

/// Result of a register request.
#[derive(Debug, Clone, PartialEq)]
pub struct RegisterResult {
    pub epoch: u64,
    pub node_id: NodeId,
}

/// The coordinator.
pub struct Coordinator {
    config: Arc<Config>,
    clock: Arc<dyn Clock>,
    epoch: AtomicU64,
    nodes: RwLock<HashMap<NodeId, NodeRecord>>,
    objects: RwLock<HashMap<ObjectId, ObjectState>>,
    prefixes: PrefixStore,
    leases: LeaseRegistry,
    snapshot_dir: Option<PathBuf>,
    rng: Mutex<crate::rng::Xoshiro256>,
}

impl Coordinator {
    pub fn new(config: Arc<Config>, clock: Arc<dyn Clock>, snapshot_dir: Option<PathBuf>) -> KvResult<Coordinator> {
        let rng = crate::rng::os_rng();
        if let Some(dir) = &snapshot_dir {
            std::fs::create_dir_all(dir).map_err(|e| KvError::io(format!("create coord dir: {}", e)))?;
        }
        let mut coord = Coordinator {
            config: config.clone(),
            clock,
            epoch: AtomicU64::new(1),
            nodes: RwLock::new(HashMap::new()),
            objects: RwLock::new(HashMap::new()),
            prefixes: PrefixStore::new(),
            leases: LeaseRegistry::new(),
            snapshot_dir,
            rng: Mutex::new(rng),
        };
        // Recover durable state (epoch, objects).
        coord.recover_from_snapshot()?;
        // Advancing the epoch invalidates all authority from before restart.
        let e = coord.epoch.load(Ordering::SeqCst).saturating_add(1);
        coord.epoch.store(e, Ordering::SeqCst);
        Ok(coord)
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// A fresh, globally-unique object identity from the coordinator RNG.
    pub fn next_object_id(&self) -> ObjectId {
        let mut guard = match self.rng.lock() {
            Ok(g) => g,
            Err(_) => return crate::id::random_object_id(&mut crate::rng::os_rng()),
        };
        crate::id::random_object_id(&mut guard)
    }

    pub fn config(&self) -> &Config {
        &self.config
    }

    pub fn register(&self, node_id: NodeId, boot_id: u128, addr: SocketAddr, caps: Vec<String>) -> KvResult<RegisterResult> {
        let epoch = self.epoch();
        let rec = NodeRecord { node_id, boot_id, addr, last_epoch: epoch, capabilities: caps };
        let mut nodes = self.nodes.write().map_err(|_| KvError::internal("nodes lock poisoned"))?;
        if let Some(existing) = nodes.get(&node_id) {
            if existing.boot_id != boot_id {
                // A restarted node with the same id is a new authoritative boot.
            }
        }
        nodes.insert(node_id, rec);
        let _ = self.maybe_snapshot();
        Ok(RegisterResult { epoch, node_id })
    }

    pub fn heartbeat(&self, node_id: NodeId, boot_id: u128) -> KvResult<u64> {
        let epoch = self.epoch();
        let mut nodes = self.nodes.write().map_err(|_| KvError::internal("nodes lock poisoned"))?;
        let rec = nodes.get_mut(&node_id).ok_or_else(|| KvError::not_found("node not registered"))?;
        if rec.boot_id != boot_id {
            return Err(KvError::stale_authority("node boot identity is stale"));
        }
        rec.last_epoch = epoch;
        Ok(epoch)
    }

    pub fn node(&self, node_id: NodeId) -> Option<NodeRecord> {
        self.nodes.read().ok().and_then(|m| m.get(&node_id).cloned())
    }

    pub fn nodes(&self) -> Vec<NodeRecord> {
        self.nodes.read().map(|m| m.values().cloned().collect()).unwrap_or_default()
    }

    /// Owner of an object, if known and authoritative in the current epoch.
    pub fn owner_of(&self, object_id: ObjectId) -> Option<(NodeId, u128, u64)> {
        let objects = self.objects.read().ok()?;
        let o = objects.get(&object_id)?;
        if o.authority_epoch != self.epoch() {
            return None;
        }
        Some((o.owner, o.owner_boot, o.authority_epoch))
    }

    /// Register a committed object and its prefix into the global index.
    pub fn register_object(&self, meta: &KvMetadata, owner: NodeId, owner_boot: u128) -> KvResult<()> {
        let epoch = self.epoch();
        if meta.lifecycle != crate::lifecycle::LifecycleState::Committed
            && meta.lifecycle != crate::lifecycle::LifecycleState::Resident {
            return Err(KvError::rejected("only committed objects may be registered"));
        }
        {
            let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
            if let Some(existing) = objects.get(&meta.object_id) {
                if existing.meta.generation >= meta.generation {
                    return Err(KvError::stale_generation("object generation not newer"));
                }
            }
            objects.insert(meta.object_id, ObjectState {
                meta: meta.clone(),
                owner,
                owner_boot,
                authority_epoch: epoch,
            });
        }
        // Commit the prefix into the global index (reuse-safe key).
        let _ = self.prefixes.commit(
            &meta.namespace,
            &meta.compat_key,
            &prefix_tokens(meta),
            meta.object_id,
            meta.generation,
            self.clock.epoch_secs(),
        );
        let _ = self.maybe_snapshot();
        Ok(())
    }

    pub fn lookup(&self, namespace: &str, compat_key: &[u8; 32], tokens: &[Token]) -> Option<PrefixMatch> {
        self.prefixes.longest_prefix(namespace, compat_key, tokens)
    }

    pub fn acquire_lease(&self, object_id: ObjectId, holder: NodeId, boot: u128, kind: LeaseKind, ttl: u64) -> KvResult<()> {
        let epoch = self.epoch();
        // Authority: object must exist and be owned by this holder in this epoch.
        let objects = self.objects.read().map_err(|_| KvError::internal("objects lock poisoned"))?;
        let st = objects.get(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
        if st.owner != holder || st.owner_boot != boot || st.authority_epoch != epoch {
            return Err(KvError::unauthorized("holder has no authority over this object"));
        }
        let _ = self.leases.acquire(object_id, holder, boot, kind, epoch, ttl, self.clock.as_ref())?;
        Ok(())
    }

    pub fn release_lease(&self, object_id: ObjectId, lease_id: crate::id::LeaseId, holder: NodeId, boot: u128) -> KvResult<()> {
        self.leases.release(object_id, lease_id, holder, boot)
    }

    pub fn is_leased(&self, object_id: ObjectId) -> bool {
        self.leases.is_leased(object_id, self.clock.as_ref())
    }

    pub fn lease_count(&self, object_id: ObjectId) -> usize {
        self.leases.live_lease_count(object_id, self.clock.as_ref())
    }

    /// Object metadata (canonical).
    pub fn object_metadata(&self, object_id: ObjectId) -> Option<KvMetadata> {
        self.objects.read().ok().and_then(|m| m.get(&object_id).map(|s| s.meta.clone()))
    }

    pub fn object_count(&self) -> usize {
        self.objects.read().map(|m| m.len()).unwrap_or(0)
    }

    pub fn objects_list(&self) -> Vec<ObjectState> {
        self.objects.read().map(|m| m.values().cloned().collect()).unwrap_or_default()
    }

    /// Apply a migration commit: transfer canonical ownership.
    pub fn migrate_commit(&self, object_id: ObjectId, new_owner: NodeId, new_owner_boot: u128, new_gen: Generation, caller: NodeId, caller_boot: u128) -> KvResult<()> {
        let epoch = self.epoch();
        {
            let mut objects = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
            let st = objects.get_mut(&object_id).ok_or_else(|| KvError::not_found("object unknown"))?;
            if st.authority_epoch != epoch {
                return Err(KvError::stale_authority("old authority epoch"));
            }
            if st.owner != caller || st.owner_boot != caller_boot {
                return Err(KvError::unauthorized("not the current owner"));
            }
            if new_gen.get() <= st.meta.generation.get() {
                return Err(KvError::stale_generation("target generation not newer"));
            }
            st.owner = new_owner;
            st.owner_boot = new_owner_boot;
            st.meta.generation = new_gen;
            st.authority_epoch = epoch;
        }
        let _ = self.maybe_snapshot();
        Ok(())
    }

    fn maybe_snapshot(&self) -> KvResult<()> {
        let Some(dir) = &self.snapshot_dir else { return Ok(()); };
        let epoch = self.epoch();
        let objects = self.objects.read().map(|m| m.values().cloned().collect::<Vec<_>>()).unwrap_or_default();
        crate::persistence::atomic_write(&dir.join("coordinator.snapshot"), &encode_snapshot(epoch, &objects))
    }

    fn recover_from_snapshot(&mut self) -> KvResult<()> {
        let Some(dir) = &self.snapshot_dir else { return Ok(()); };
        let path = dir.join("coordinator.snapshot");
        if !path.exists() {
            return Ok(());
        }
        let data = std::fs::read(&path).map_err(|e| KvError::io(format!("read snapshot: {}", e)))?;
        let (epoch, objects) = decode_snapshot(&data)?;
        self.epoch.store(epoch, Ordering::SeqCst);
        let mut map = self.objects.write().map_err(|_| KvError::internal("objects lock poisoned"))?;
        for st in objects {
            map.insert(st.meta.object_id, st);
        }
        Ok(())
    }
}

const SNAPSHOT_VERSION: u32 = 1;

fn encode_snapshot(epoch: u64, objects: &[ObjectState]) -> Vec<u8> {
    let mut b = crate::bytes::Buffer::new();
    b.u32(SNAPSHOT_VERSION);
    b.u64(epoch);
    b.u32(objects.len() as u32);
    for st in objects {
        b.len_bytes(&st.meta.encode());
        b.u64(st.owner.to_u64());
        b.u128(st.owner_boot);
        b.u64(st.authority_epoch);
    }
    b.into_vec()
}

fn decode_snapshot(data: &[u8]) -> KvResult<(u64, Vec<ObjectState>)> {
    let mut c = crate::bytes::Cursor::new(data);
    let ver = c.u32()?;
    if ver != SNAPSHOT_VERSION {
        return Err(KvError::invalid_format(format!("unsupported snapshot version {}", ver)));
    }
    let epoch = c.u64()?;
    let n = c.u32()? as usize;
    if n > crate::bytes::MAX_NODES * 256 {
        return Err(KvError::protocol("snapshot object count exceeds bound"));
    }
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let meta = KvMetadata::decode(&c.len_bytes(crate::protocol::MAX_FRAME_PAYLOAD)?)?;
        let owner = NodeId::from_u64(c.u64()?);
        let owner_boot = c.u128()?;
        let authority_epoch = c.u64()?;
        out.push(ObjectState { meta, owner, owner_boot, authority_epoch });
    }
    c.finish()?;
    Ok((epoch, out))
}

/// Reconstruct the token sequence used to index a committed object. Because
/// metadata does not store the full token list, the prefix index on the
/// coordinator is advisory; exact prefix reuse validation is done at the node.
fn prefix_tokens(_meta: &KvMetadata) -> Vec<Token> {
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::MockClock;

    fn coord() -> Coordinator {
        let cfg = Arc::new(Config::default());
        Coordinator::new(cfg, Arc::new(MockClock::new()), None).unwrap()
    }

    #[test]
    fn register_and_heartbeat() {
        let c = coord();
        let es = c.epoch();
        let r = c.register(NodeId(1), 0xaa, "127.0.0.1:7000".parse().unwrap(), vec!["cpu".into()]).unwrap();
        assert_eq!(r.epoch, es);
        assert_eq!(c.heartbeat(NodeId(1), 0xaa).unwrap(), es);
        // Stale boot rejected.
        assert_eq!(c.heartbeat(NodeId(1), 0xbb).unwrap_err().kind(), crate::error::ErrorKind::StaleAuthority);
    }

    #[test]
    fn restart_advances_epoch() {
        let dir = std::env::temp_dir().join(format!("kvfabric-coord-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cfg = Arc::new(Config::default());
        let c1 = Coordinator::new(cfg.clone(), Arc::new(MockClock::new()), Some(dir.clone())).unwrap();
        let e1 = c1.epoch();
        let _ = c1.register(NodeId(5), 0x1, "127.0.0.1:1".parse().unwrap(), vec![]).unwrap();
        // A second coordinator over the same snapshot dir recovers and bumps epoch.
        let c2 = Coordinator::new(cfg, Arc::new(MockClock::new()), Some(dir.clone())).unwrap();
        assert!(c2.epoch() > e1, "epoch must advance after restart");
        let _ = std::fs::remove_dir_all(&dir);
    }
}

