//! Integrity layer.
//!
//! Every object has a canonical manifest anchor (SHA-256) over its metadata and
//! a content digest. Every block carries a fast CRC-32C plus a SHA-256 anchor.
//! Corruption must be detected; corrupt state is quarantined and never served.

use crate::block::Block;
use crate::error::{KvError, KvResult};
use crate::hash::{crc32c, sha256, CanonicalHasher};

/// Format version for binary block payloads.
pub const BLOCK_FORMAT_VERSION: u32 = 1;

/// Verify a block payload against its recorded CRC-32C and SHA-256 anchors.
pub fn verify_block_payload(block: &Block, payload: &[u8]) -> KvResult<()> {
    if payload.len() as u64 != block.len {
        return Err(KvError::integrity(format!(
            "block size mismatch: expected {} got {}",
            block.len,
            payload.len()
        )));
    }
    let crc = crc32c(payload);
    if crc != block.crc32c {
        return Err(KvError::integrity(format!(
            "block crc32c mismatch (expected {:08x}, got {:08x})",
            block.crc32c, crc
        )));
    }
    let digest = sha256(payload);
    if digest != block.sha256 {
        return Err(KvError::integrity("block sha256 mismatch".to_string()));
    }
    Ok(())
}

/// Compute the CRC-32C of a payload (used when writing new blocks).
pub fn compute_block_crc(payload: &[u8]) -> u32 {
    crc32c(payload)
}

/// Compute the canonical manifest anchor for an object given its decoded
/// metadata bytes and the content digest of its blocks.
pub fn manifest_anchor(metadata_canonical: &[u8], content_digest: &[u8; 32]) -> [u8; 32] {
    let mut h = CanonicalHasher::new();
    h.version(BLOCK_FORMAT_VERSION);
    h.bytes(1, metadata_canonical);
    h.bytes(2, content_digest);
    h.digest()
}

/// Combine per-block digests into a single object content digest.
pub fn content_digest(block_sha256s: &[[u8; 32]]) -> [u8; 32] {
    let mut h = CanonicalHasher::new();
    h.version(BLOCK_FORMAT_VERSION);
    for (i, d) in block_sha256s.iter().enumerate() {
        h.bytes((i as u8).wrapping_add(1), d);
    }
    h.digest()
}

/// A recorded verification that can be replayed for auditing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerificationRecord {
    pub object_id: crate::id::ObjectId,
    pub verified_at_epoch: u64,
    pub content_digest: [u8; 32],
    pub block_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::id::{BlockId, Generation, ObjectId};
    use crate::rng::deterministic_rng;

    #[test]
    fn verify_passes_clean_block() {
        let mut _r = deterministic_rng(4);
        let mut b = Block::new(BlockId(7), ObjectId::from_u128(1), 0, Generation::one());
        b.set_payload(b"0123456789");
        assert!(verify_block_payload(&b, b"0123456789").is_ok());
    }

    #[test]
    fn verify_detects_size_mismatch() {
        let mut _r = deterministic_rng(4);
        let mut b = Block::new(BlockId(7), ObjectId::from_u128(1), 0, Generation::one());
        b.set_payload(b"0123456789");
        assert!(verify_block_payload(&b, b"01234567").is_err());
    }

    #[test]
    fn verify_detects_bitflip_in_crc() {
        let mut _r = deterministic_rng(4);
        let mut b = Block::new(BlockId(7), ObjectId::from_u128(1), 0, Generation::one());
        b.set_payload(b"0123456789");
        let mut corrupted = b"0123456789".to_vec();
        corrupted[3] ^= 0xff;
        assert!(verify_block_payload(&b, &corrupted).is_err());
    }

    #[test]
    fn manifest_anchor_is_deterministic() {
        let a = manifest_anchor(b"meta", &[1u8; 32]);
        let b = manifest_anchor(b"meta", &[1u8; 32]);
        assert_eq!(a, b);
        let c = manifest_anchor(b"meta2", &[1u8; 32]);
        assert_ne!(a, c);
    }
}
