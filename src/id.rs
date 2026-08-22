//! Stable identity types for KV Fabric.
//!
//! * `ObjectId` — 128-bit globally-stable identity of a reusable KV object.
//! * `BlockId` — 128-bit identity of a physical KV block.
//! * `PrefixId` — 64-bit identity of an immutable reusable prefix segment.
//! * `NodeId` — 64-bit identity of a runtime node.
//! * `Generation` — monotonically increasing object generation.
//! * `LeaseId` — 128-bit lease/authority identifier.
//! * `Token` — a token id (u32), used for prefix matching.

use std::fmt;
use std::str::FromStr;

use crate::error::{KvError, KvResult};

macro_rules! identity_u128 {
    ($name:ident) => {
        #[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub u128);

        impl $name {
            pub const fn from_u128(v: u128) -> $name {
                $name(v)
            }
            pub fn to_u128(self) -> u128 {
                self.0
            }
            pub fn is_zero(self) -> bool {
                self.0 == 0
            }
            pub fn as_u64_pair(self) -> (u64, u64) {
                ((self.0 >> 64) as u64, self.0 as u64)
            }
            pub fn from_u64_pair(hi: u64, lo: u64) -> $name {
                $name(((hi as u128) << 64) | lo as u128)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:032x}", self.0)
            }
        }
        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{:032x}", self.0)
            }
        }
        impl FromStr for $name {
            type Err = KvError;
            fn from_str(s: &str) -> KvResult<$name> {
                if s.is_empty() || s.len() > 32 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
                    return Err(KvError::invalid_format(format!(
                        "invalid {} hex string: '{}'",
                        stringify!($name),
                        s
                    )));
                }
                let padded = format!("{:0>32}", s.to_ascii_lowercase());
                let v = u128::from_str_radix(&padded, 16)
                    .map_err(|_| KvError::invalid_format(format!("bad {} hex", stringify!($name))))?;
                Ok($name(v))
            }
        }
    };
}

identity_u128!(ObjectId);
identity_u128!(BlockId);
identity_u128!(LeaseId);

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u64);

impl NodeId {
    pub const fn from_u64(v: u64) -> NodeId {
        NodeId(v)
    }
    pub const fn to_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}
impl fmt::Debug for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for NodeId {
    type Err = KvError;
    fn from_str(s: &str) -> KvResult<NodeId> {
        if s.is_empty() || s.len() > 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(KvError::invalid_format(format!("invalid node id hex: '{}'", s)));
        }
        let padded = format!("{:0>16}", s.to_ascii_lowercase());
        let v = u64::from_str_radix(&padded, 16)
            .map_err(|_| KvError::invalid_format("bad node id hex"))?;
        Ok(NodeId(v))
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PrefixId(pub u64);

impl PrefixId {
    pub const fn from_u64(v: u64) -> PrefixId {
        PrefixId(v)
    }
    pub const fn to_u64(self) -> u64 {
        self.0
    }
}

impl fmt::Display for PrefixId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}
impl fmt::Debug for PrefixId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// A generation counter. Generation 0 is the "never generated" sentinel.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Generation(pub u64);

impl Generation {
    pub const ZERO: Generation = Generation(0);
    pub const fn new(v: u64) -> Generation {
        Generation(v)
    }
    pub const fn one() -> Generation {
        Generation(1)
    }
    pub fn next(self) -> Generation {
        Generation(self.0.saturating_add(1))
    }
    pub const fn get(self) -> u64 {
        self.0
    }
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "g{}", self.0)
    }
}
impl fmt::Debug for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "g{}", self.0)
    }
}

/// A single token id in a prompt sequence.
pub type Token = u32;

/// Number of bytes in a stable object id string repr.
pub const OBJECT_ID_HEX_LEN: usize = 32;
pub const NODE_ID_HEX_LEN: usize = 16;

/// Generate a fresh 128-bit identity from an RNG.
pub fn random_object_id(rng: &mut crate::rng::Xoshiro256) -> ObjectId {
    ObjectId(crate::rng::random_u128(rng))
}
pub fn random_block_id(rng: &mut crate::rng::Xoshiro256) -> BlockId {
    BlockId(crate::rng::random_u128(rng))
}
pub fn random_lease_id(rng: &mut crate::rng::Xoshiro256) -> LeaseId {
    LeaseId(crate::rng::random_u128(rng))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::deterministic_rng;

    #[test]
    fn object_id_hex_roundtrip() {
        let mut r = deterministic_rng(1);
        let o = random_object_id(&mut r);
        let s = o.to_string();
        assert_eq!(s.len(), 32);
        assert_eq!(ObjectId::from_str(&s).unwrap(), o);
        assert!(ObjectId::from_str("nothex").is_err());
        assert!(ObjectId::from_str("").is_err());
    }

    #[test]
    fn generation_never_regresses() {
        let mut g = Generation::one();
        let mut prev = g;
        for _ in 0..10 {
            g = g.next();
            assert!(g > prev);
            prev = g;
        }
        assert!(Generation::ZERO.is_zero());
    }

    #[test]
    fn node_id_hex_roundtrip() {
        let n = NodeId(0xdead_beef_u64);
        assert_eq!(NodeId::from_str(&n.to_string()).unwrap(), n);
        assert!(NodeId::from_str("xyz").is_err());
    }

    #[test]
    fn block_id_u64_pair() {
        let b = BlockId::from_u64_pair(0x1111, 0x2222);
        assert_eq!(b.as_u64_pair(), (0x1111, 0x2222));
    }
}
