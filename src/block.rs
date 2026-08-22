//! Block-level KV management.
//!
//! KV state is managed in fixed-size logical blocks so that a single sequence
//! can be partitioned, shared, migrated and reclaimed at block granularity.
//! Block identity is 128-bit and globally stable. Blocks are grouped into
//! logical KVObjects.

use crate::error::{KvError, KvResult};
use crate::hash::crc32c;
use crate::id::{BlockId, Generation, ObjectId};

/// Number of token slots per block. Configurable, defaulting to a power of two
/// that balances allocation granularity against fragmentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockSize {
    pub elements: u32,
    pub bytes_per_element: u32,
}

impl BlockSize {
    pub const fn new(elements: u32, bytes_per_element: u32) -> BlockSize {
        BlockSize { elements, bytes_per_element }
    }
    pub const fn default_block_size() -> BlockSize {
        BlockSize { elements: 64, bytes_per_element: 4 }
    }
    pub fn total_bytes(&self) -> u64 {
        // key and value elements per token slot
        self.elements as u64 * self.bytes_per_element as u64 * 2
    }
}

/// Static metadata for a physical block. The payload lives in a tier allocator;
/// this records its identity and integrity anchor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub id: BlockId,
    pub object_id: ObjectId,
    pub index: u32,
    pub generation: Generation,
    pub len: u64,
    pub crc32c: u32,
    pub sha256: [u8; 32],
}

impl Block {
    pub fn new(id: BlockId, object_id: ObjectId, index: u32, generation: Generation) -> Block {
        Block { id, object_id, index, generation, len: 0, crc32c: 0, sha256: [0u8; 32] }
    }

    /// Finalize integrity anchors from the payload.
    pub fn set_payload(&mut self, payload: &[u8]) {
        self.len = payload.len() as u64;
        self.crc32c = crc32c(payload);
        self.sha256 = crate::hash::sha256(payload);
    }
}

/// A logical group of blocks belonging to one object, indexed by token range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockGroup {
    pub object_id: ObjectId,
    pub first_index: u32,
    pub block_count: u32,
    pub block_ids: Vec<BlockId>,
}

/// Number of blocks required for a given number of tokens.
pub fn blocks_for_tokens(token_count: u32, per_block: u32) -> u32 {
    if per_block == 0 {
        return 0;
    }
    token_count.div_ceil(per_block)
}

/// Round a byte capacity up to a block-aligned value.
pub fn align_up(v: u64, align: u64) -> u64 {
    if align == 0 {
        return v;
    }
    v.div_ceil(align) * align
}

/// Validate that a requested allocation does not overflow.
pub fn checked_block_bytes(elements: u32, bytes_per_element: u32) -> KvResult<u64> {
    (elements as u64)
        .checked_mul(bytes_per_element as u64)
        .and_then(|v| v.checked_mul(2))
        .ok_or_else(|| KvError::capacity("block byte size overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::deterministic_rng;

    #[test]
    fn blocks_round_up() {
        assert_eq!(blocks_for_tokens(0, 64), 0);
        assert_eq!(blocks_for_tokens(1, 64), 1);
        assert_eq!(blocks_for_tokens(63, 64), 1);
        assert_eq!(blocks_for_tokens(64, 64), 1);
        assert_eq!(blocks_for_tokens(65, 64), 2);
        assert_eq!(blocks_for_tokens(130, 64), 3);
    }

    #[test]
    fn align_up_is_correct() {
        assert_eq!(align_up(0, 128), 0);
        assert_eq!(align_up(1, 128), 128);
        assert_eq!(align_up(128, 128), 128);
        assert_eq!(align_up(129, 128), 256);
        assert_eq!(align_up(200, 128), 256);
    }

    #[test]
    fn block_finalize_sets_integrity() {
        let mut r = deterministic_rng(9);
        let id = crate::id::random_block_id(&mut r);
        let mut b = Block::new(id, ObjectId::from_u128(0xabc), 0, Generation::one());
        b.set_payload(b"hello world");
        assert_eq!(b.crc32c, crc32c(b"hello world"));
        assert_eq!(b.len, 11);
    }

    #[test]
    fn checked_block_bytes_no_overflow() {
        assert!(checked_block_bytes(64, 4).is_ok());
        assert_eq!(checked_block_bytes(64, 4).unwrap(), 64 * 4 * 2);
    }
}
