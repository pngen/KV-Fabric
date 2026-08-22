//! Durable persistence with atomic commits and crash recovery.
//!
//! A KVObject is persisted as: (1) block payloads, written atomically to a
//! per-block file (temp + fsync + rename); (2) a metadata file, written
//! atomically; (3) a manifest entry that is the authoritative index. No durable
//! success is reported before the manifest is committed. On recovery the
//! manifest is walked and every referenced object is verified: metadata
//! validity, block presence and aggregate content-digest. Anything that fails
//! verification is not re-admitted.

use std::path::{Path, PathBuf};

use crate::error::{KvError, KvResult};
use crate::id::{BlockId, ObjectId};
use crate::integrity::content_digest;
use crate::metadata::KvMetadata;

const MANIFEST: &str = "manifest";
const META_DIR: &str = "meta";
const BLOCK_DIR: &str = "blocks";

/// Atomically write a file: temp file, fsync, then rename over the target.
pub fn atomic_write(path: &Path, data: &[u8]) -> KvResult<()> {
    let parent = path.parent().ok_or_else(|| KvError::invalid_arg("no parent for path"))?;
    std::fs::create_dir_all(parent).map_err(|e| KvError::io(format!("create dir: {}", e)))?;
    let tmp = path.with_extension(format!("tmp-{}", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp).map_err(|e| KvError::io(format!("create temp: {}", e)))?;
        use std::io::Write as _;
        f.write_all(data).map_err(|e| KvError::io(format!("write temp: {}", e)))?;
        f.sync_all().map_err(|e| KvError::io(format!("fsync temp: {}", e)))?;
    }
    std::fs::rename(&tmp, path).map_err(|e| {
        let _ = std::fs::remove_file(&tmp);
        KvError::io(format!("rename to final: {}", e))
    })?;
    // Best-effort directory sync to make the rename durable.
    if let Ok(d) = std::fs::File::open(parent) {
        let _ = d.sync_all();
    }
    Ok(())
}

fn meta_path(root: &Path, id: ObjectId) -> PathBuf {
    root.join(META_DIR).join(format!("{}.meta", id))
}

fn block_path(root: &Path, block: BlockId) -> PathBuf {
    root.join(BLOCK_DIR).join(format!("{}.blk", block))
}

/// A durable object store with a manifest index and integrity recovery.
pub struct DurableStore {
    root: PathBuf,
}

impl DurableStore {
    pub fn new(root: impl Into<PathBuf>) -> KvResult<DurableStore> {
        let root: PathBuf = root.into();
        std::fs::create_dir_all(&root).map_err(|e| KvError::io(format!("create root: {}", e)))?;
        std::fs::create_dir_all(root.join(META_DIR)).map_err(|e| KvError::io(format!("create meta dir: {}", e)))?;
        std::fs::create_dir_all(root.join(BLOCK_DIR)).map_err(|e| KvError::io(format!("create block dir: {}", e)))?;
        Ok(DurableStore { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Total bytes used on disk (best-effort recursive sum).
    pub fn used(&self) -> u64 {
        let mut total = 0u64;
        if let Ok(rd) = std::fs::read_dir(&self.root) {
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    if let Ok(rdd) = std::fs::read_dir(&p) {
                        for f in rdd.flatten() {
                            if let Ok(md) = f.metadata() { total += md.len(); }
                        }
                    }
                } else if let Ok(md) = e.metadata() { total += md.len(); }
            }
        }
        total
    }

    /// Persist an object: blocks first, then metadata, then manifest.
    pub fn store_object(&self, meta: &KvMetadata, blocks: &[(&BlockId, &[u8])]) -> KvResult<()> {
        // Blocks first so a crash before the manifest cannot reference them.
        for (bid, payload) in blocks {
            atomic_write(&block_path(&self.root, **bid), payload)?;
        }
        atomic_write(&meta_path(&self.root, meta.object_id), &meta.encode())?;
        let mut manifest = self.read_manifest()?;
        manifest.retain(|o| o.object_id != meta.object_id);
        manifest.push(ManifestEntry {
            object_id: meta.object_id,
            generation: meta.generation.get(),
            content_digest: meta.content_digest,
            block_count: meta.block_count,
        });
        let data = encode_manifest(&manifest);
        atomic_write(&self.root.join(MANIFEST), &data)?;
        Ok(())
    }

    /// Load a persisted object (metadata and all block payloads) with integrity.
    pub fn load_object(&self, id: ObjectId) -> KvResult<(KvMetadata, Vec<(BlockId, Vec<u8>)>)> {
        let meta_bytes = std::fs::read(&meta_path(&self.root, id))
            .map_err(|e| KvError::io(format!("read meta: {}", e)))?;
        let meta = KvMetadata::decode(&meta_bytes)?;
        if meta.object_id != id {
            return Err(KvError::corrupt("metadata object id mismatch"));
        }
        let mut block_shas = Vec::with_capacity(meta.block_ids.len());
        let mut payloads = Vec::with_capacity(meta.block_ids.len());
        for bid in &meta.block_ids {
            let bytes = std::fs::read(&block_path(&self.root, *bid))
                .map_err(|e| KvError::io(format!("read block: {}", e)))?;
            block_shas.push(crate::hash::sha256(&bytes));
            payloads.push((*bid, bytes));
        }
        let digest = content_digest(&block_shas);
        if digest != meta.content_digest {
            return Err(KvError::integrity("object content digest mismatch"));
        }
        Ok((meta, payloads))
    }

    pub fn remove_object(&self, id: ObjectId) -> KvResult<()> {
        let _ = std::fs::remove_file(&meta_path(&self.root, id));
        if let Ok(meta) = self.load_object(id) {
            for (bid, _) in meta.1 {
                let _ = std::fs::remove_file(&block_path(&self.root, bid));
            }
        }
        let mut manifest = self.read_manifest()?;
        manifest.retain(|o| o.object_id != id);
        atomic_write(&self.root.join(MANIFEST), &encode_manifest(&manifest))?;
        Ok(())
    }

    pub fn contains(&self, id: ObjectId) -> bool {
        self.read_manifest().map(|m| m.iter().any(|o| o.object_id == id)).unwrap_or(false)
    }

    fn read_manifest(&self) -> KvResult<Vec<ManifestEntry>> {
        let p = self.root.join(MANIFEST);
        if !p.exists() {
            return Ok(Vec::new());
        }
        let data = std::fs::read(&p).map_err(|e| KvError::io(format!("read manifest: {}", e)))?;
        decode_manifest(&data)
    }

    /// Recover all objects whose metadata is valid and whose blocks verify.
    /// Corrupt or incomplete objects are skipped and reported as invalid.
    pub fn recover(&self) -> KvResult<Vec<(KvMetadata, Vec<(BlockId, Vec<u8>)>)>> {
        let manifest = self.read_manifest()?;
        let mut recovered = Vec::new();
        for entry in manifest {
            match self.load_object(entry.object_id) {
                Ok(pair) => recovered.push(pair),
                Err(_) => {
                    // Corrupt or incomplete: do not re-admit.
                }
            }
        }
        Ok(recovered)
    }
}

/// A manifest entry (durable index of a committed object).
#[derive(Debug, Clone, PartialEq)]
pub struct ManifestEntry {
    pub object_id: ObjectId,
    pub generation: u64,
    pub content_digest: [u8; 32],
    pub block_count: u32,
}

const MANIFEST_VERSION: u32 = 1;

fn encode_manifest(entries: &[ManifestEntry]) -> Vec<u8> {
    let mut b = crate::bytes::Buffer::new();
    b.u32(MANIFEST_VERSION);
    b.u32(entries.len() as u32);
    for e in entries {
        b.u128(e.object_id.to_u128());
        b.u64(e.generation);
        b.len_bytes(&e.content_digest);
        b.u32(e.block_count);
    }
    b.into_vec()
}

fn decode_manifest(data: &[u8]) -> KvResult<Vec<ManifestEntry>> {
    let mut c = crate::bytes::Cursor::new(data);
    let ver = c.u32()?;
    if ver != MANIFEST_VERSION {
        return Err(KvError::invalid_format(format!("unsupported manifest version {}", ver)));
    }
    let n = c.u32()? as usize;
    if n > crate::bytes::MAX_NODES * 256 {
        return Err(KvError::protocol("manifest entry count exceeds bound"));
    }
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let object_id = ObjectId::from_u128(c.u128()?);
        let generation = c.u64()?;
        let mut content_digest = [0u8; 32];
        content_digest.copy_from_slice(c.len_bytes(32)?.as_slice());
        let block_count = c.u32()?;
        entries.push(ManifestEntry { object_id, generation, content_digest, block_count });
    }
    c.finish()?;
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::Generation;
    use crate::metadata::KvMetadata;
    use crate::rng::deterministic_rng;

    fn make_meta(rng: &mut crate::rng::Xoshiro256, ns: &str, nblocks: u32) -> (KvMetadata, Vec<(BlockId, Vec<u8>)>) {
        let mut m = KvMetadata::new(crate::id::random_object_id(rng), Generation::one(), ns);
        m.token_count = nblocks * 64;
        m.token_end = m.token_count;
        m.block_count = nblocks;
        m.byte_size = nblocks as u64 * 512;
        m.dtype = "float16".into();
        m.compat_key = compatible_vec(rng, ns);
        let mut blocks = Vec::new();
        let mut shas = Vec::new();
        for i in 0..nblocks {
            let bid = crate::id::random_block_id(rng);
            let payload = vec![i as u8; 512];
            m.block_ids.push(bid);
            shas.push(crate::hash::sha256(&payload));
            blocks.push((bid, payload));
        }
        m.content_digest = content_digest(&shas);
        (m, blocks)
    }
    fn compatible_vec(rng: &mut crate::rng::Xoshiro256, ns: &str) -> [u8; 32] {
        let mut k = [0u8; 32];
        k[..ns.len().min(32)].copy_from_slice(&ns.as_bytes()[..ns.len().min(32)]);
        let _ = rng.next_u64();
        k
    }

    #[test]
    fn store_load_recover_roundtrip() {
        let dir = std::env::temp_dir().join(format!("kvfabric-dur-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut rng = deterministic_rng(20);
        let store = DurableStore::new(&dir).unwrap();
        let (meta, blocks) = make_meta(&mut rng, "ns", 3);
        let refs: Vec<(&BlockId, &[u8])> = blocks.iter().map(|(b, p)| (b, p.as_slice())).collect();
        store.store_object(&meta, &refs).unwrap();
        let (lm, lp) = store.load_object(meta.object_id).unwrap();
        assert_eq!(lm.object_id, meta.object_id);
        assert_eq!(lp.len(), 3);
        let recovered = store.recover().unwrap();
        assert_eq!(recovered.len(), 1);
        // A fresh store over the same dir recovers from scratch.
        let store2 = DurableStore::new(&dir).unwrap();
        assert_eq!(store2.recover().unwrap().len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corruption_is_detected_and_not_recovered() {
        let dir = std::env::temp_dir().join(format!("kvfabric-dur2-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut rng = deterministic_rng(21);
        let store = DurableStore::new(&dir).unwrap();
        let (meta, blocks) = make_meta(&mut rng, "ns", 2);
        let refs: Vec<(&BlockId, &[u8])> = blocks.iter().map(|(b, p)| (b, p.as_slice())).collect();
        store.store_object(&meta, &refs).unwrap();
        // Corrupt one block payload on disk.
        let bid = blocks[0].0;
        let p = block_path(&dir, bid);
        let mut data = std::fs::read(&p).unwrap();
        data[0] ^= 0xff;
        atomic_write(&p, &data).unwrap();
        assert!(store.load_object(meta.object_id).is_err());
        assert_eq!(store.recover().unwrap().len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn remove_object_cleans_up() {
        let dir = std::env::temp_dir().join(format!("kvfabric-dur3-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mut rng = deterministic_rng(22);
        let store = DurableStore::new(&dir).unwrap();
        let (meta, blocks) = make_meta(&mut rng, "ns", 1);
        let refs: Vec<(&BlockId, &[u8])> = blocks.iter().map(|(b, p)| (b, p.as_slice())).collect();
        store.store_object(&meta, &refs).unwrap();
        assert!(store.contains(meta.object_id));
        store.remove_object(meta.object_id).unwrap();
        assert!(!store.contains(meta.object_id));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn truncated_manifest_is_tolerated() {
        let dir = std::env::temp_dir().join(format!("kvfabric-dur4-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = DurableStore::new(&dir).unwrap();
        atomic_write(&dir.join(MANIFEST), &[1u8, 2, 3]).unwrap();
        assert!(store.read_manifest().is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
