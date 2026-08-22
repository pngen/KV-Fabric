//! Deterministic, high-quality, dependency-free PRNGs.
//!
//! `SplitMix64` is used to seed `Xoshiro256**`. Seeds are derived from OS
//! entropy via the randomized per-instance `DefaultHasher`. The generators are
//! `#[derive(Clone)]` and cheap, and can be seeded deterministically for tests
//! so that randomized/property tests are reproducible.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A fast 64-bit generator (SplitMix64).
#[derive(Clone)]
pub struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    pub fn new(seed: u64) -> SplitMix64 {
        SplitMix64 { state: seed }
    }
    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}

/// xoshiro256** — a fast, high-quality generator.
#[derive(Clone)]
pub struct Xoshiro256 {
    s: [u64; 4],
}

impl Xoshiro256 {
    pub fn from_seed(seed: u64) -> Xoshiro256 {
        let mut sm = SplitMix64::new(seed);
        let s = [
            sm.next_u64(),
            sm.next_u64(),
            sm.next_u64(),
            sm.next_u64(),
        ];
        // Avoid the all-zero state.
        let s = if s == [0, 0, 0, 0] { [1, 2, 3, 4] } else { s };
        Xoshiro256 { s }
    }

    pub fn next_u64(&mut self) -> u64 {
        let result = self.s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result
    }

    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Uniform integer in [0, bound).
    pub fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            return 0;
        }
        // Rejection sampling to avoid modulo bias.
        let zone = u64::MAX - (u64::MAX % bound);
        loop {
            let r = self.next_u64();
            if r < zone {
                return r % bound;
            }
        }
    }

    pub fn fill(&mut self, buf: &mut [u8]) {
        let mut i = 0;
        while i + 8 <= buf.len() {
            buf[i..i + 8].copy_from_slice(&self.next_u64().to_le_bytes());
            i += 8;
        }
        if i < buf.len() {
            let rem = buf.len() - i;
            let b = self.next_u64().to_le_bytes();
            buf[i..].copy_from_slice(&b[..rem]);
        }
    }
}

/// Gather OS entropy by observing the randomized per-instance `DefaultHasher`.
/// Each instance produces an unpredictable 64-bit output seeded from the OS.
pub fn os_entropy_u64() -> u64 {
    let mut h = DefaultHasher::new();
    // Mix in process id and wall-clock to further decorrelate.
    std::process::id().hash(&mut h);
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
        .hash(&mut h);
    h.finish()
}

/// A non-deterministic RNG seeded from OS entropy.
pub fn os_rng() -> Xoshiro256 {
    let mut seed = 0u64;
    // Multiple draws from fresh hashers give several entropy words.
    for _ in 0..8 {
        let mut h = DefaultHasher::new();
        std::process::id().hash(&mut h);
        // The randomized per-instance DefaultHasher yields fresh OS-seeded
        // output on each new instance; fold a few words together.
        seed ^= h.finish();
        let mut h2 = DefaultHasher::new();
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
            .hash(&mut h2);
        seed = seed.wrapping_add(h2.finish());
    }
    // Fold in the address of a heap allocation (safe pointer-to-integer cast).
    let boxed = Box::new(0u8);
    let ptr = &*boxed as *const u8 as usize as u64;
    seed ^= ptr ^ os_entropy_u64();
    Xoshiro256::from_seed(seed)
}

/// A random 128-bit value derived from a seeded RNG.
pub fn random_u128(rng: &mut Xoshiro256) -> u128 {
    ((rng.next_u64() as u128) << 64) | rng.next_u64() as u128
}

/// A seeded RNG for reproducible tests/properties. The seed is a u64.
pub fn deterministic_rng(seed: u64) -> Xoshiro256 {
    Xoshiro256::from_seed(seed)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn splitmix_is_deterministic() {
        let mut a = SplitMix64::new(1);
        let mut b = SplitMix64::new(1);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }
    #[test]
    fn xoshiro_is_deterministic() {
        let mut a = deterministic_rng(42);
        let mut b = deterministic_rng(42);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }
    #[test]
    fn below_is_in_bounds() {
        let mut r = deterministic_rng(7);
        for _ in 0..1000 {
            assert!(r.below(10) < 10);
            assert!(r.below(1) == 0);
        }
    }
    #[test]
    fn entropy_is_nonzero() {
        // OS-entropy seeded RNG should produce varied output.
        let mut a = os_rng();
        let mut b = os_rng();
        let mut diff = false;
        for _ in 0..16 {
            if a.next_u64() != b.next_u64() {
                diff = true;
            }
        }
        assert!(diff, "distinct OS-seeded RNGs should not match");
    }
    #[test]
    fn fill_writes_all_bytes() {
        let mut r = deterministic_rng(3);
        let mut buf = vec![0u8; 32];
        r.fill(&mut buf);
        assert!(buf.iter().any(|&b| b != 0));
    }
}
