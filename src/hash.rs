//! Cryptographic and fast hashing primitives.
//!
//! * `sha256` — FIPS 180-4 for durable identity and integrity anchors.
//! * `crc32c` — Castagnoli (CRC-32C) for fast block checksums.
//! * `CanonicalHasher` — unambiguous, length-prefixed canonical encoding so
//!   that field boundaries cannot be forged by concatenation ambiguity.
//!
//! Everything here is implemented from the public specifications in safe Rust;
//! no external dependency and no `unsafe`.

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4)
// ---------------------------------------------------------------------------

const SHA256_K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5,
    0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
    0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc,
    0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3,
    0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
    0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
    0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

const SHA256_H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/// Incremental SHA-256.
pub struct Sha256 {
    state: [u32; 8],
    buf: [u8; 64],
    buf_len: usize,
    total_len: u64,
}

impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256 {
    pub fn new() -> Sha256 {
        Sha256 {
            state: SHA256_H0,
            buf: [0u8; 64],
            buf_len: 0,
            total_len: 0,
        }
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes([
                block[i * 4],
                block[i * 4 + 1],
                block[i * 4 + 2],
                block[i * 4 + 3],
            ]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = self.state[0];
        let mut b = self.state[1];
        let mut c = self.state[2];
        let mut d = self.state[3];
        let mut e = self.state[4];
        let mut f = self.state[5];
        let mut g = self.state[6];
        let mut h = self.state[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }

        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
        self.state[4] = self.state[4].wrapping_add(e);
        self.state[5] = self.state[5].wrapping_add(f);
        self.state[6] = self.state[6].wrapping_add(g);
        self.state[7] = self.state[7].wrapping_add(h);
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.total_len = self.total_len.wrapping_add(data.len() as u64);
        if self.buf_len > 0 {
            let need = 64 - self.buf_len;
            let take = need.min(data.len());
            self.buf[self.buf_len..self.buf_len + take].copy_from_slice(&data[..take]);
            self.buf_len += take;
            data = &data[take..];
            if self.buf_len == 64 {
                // Emit only when the padded block's tail is not being constructed.
                // We compress full blocks except when we must hold the last block
                // for the length padding; handled in finalize by compressing here.
                // To keep it simple and correct, we compress immediately and
                // finalize handles the final partial block separately.
                let mut block = [0u8; 64];
                block.copy_from_slice(&self.buf);
                self.compress(&block);
                self.buf_len = 0;
            }
        }
        while data.len() >= 64 {
            let mut block = [0u8; 64];
            block.copy_from_slice(&data[..64]);
            self.compress(&block);
            data = &data[64..];
        }
        if !data.is_empty() {
            self.buf[..data.len()].copy_from_slice(data);
            self.buf_len = data.len();
        }
    }

    pub fn finalize(mut self) -> [u8; 32] {
        let bit_len = self.total_len.wrapping_mul(8);
        // Append 0x80.
        self.update_raw(&[0x80]);
        // Zero-fill until remaining length == 56 mod 64.
        while self.buf_len != 56 {
            self.update_raw(&[0x00]);
        }
        // Append 64-bit big-endian bit length.
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&bit_len.to_be_bytes());
        self.update_raw(&len_bytes);

        let mut out = [0u8; 32];
        for (i, v) in self.state.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&v.to_be_bytes());
        }
        out
    }

    /// Internal update that does not accumulate `total_len` (used for padding).
    fn update_raw(&mut self, mut data: &[u8]) {
        if self.buf_len > 0 {
            let need = 64 - self.buf_len;
            let take = need.min(data.len());
            self.buf[self.buf_len..self.buf_len + take].copy_from_slice(&data[..take]);
            self.buf_len += take;
            data = &data[take..];
            if self.buf_len == 64 {
                let mut block = [0u8; 64];
                block.copy_from_slice(&self.buf);
                self.compress(&block);
                self.buf_len = 0;
            }
        }
        while data.len() >= 64 {
            let mut block = [0u8; 64];
            block.copy_from_slice(&data[..64]);
            self.compress(&block);
            data = &data[64..];
        }
        if !data.is_empty() {
            self.buf[..data.len()].copy_from_slice(data);
            self.buf_len = data.len();
        }
    }
}

/// One-shot SHA-256 digest of arbitrary bytes.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(data);
    h.finalize()
}

/// Hex-encode bytes to a lowercase string.
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", b);
    }
    s
}

pub fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// CRC-32C (Castagnoli)
// ---------------------------------------------------------------------------

const CRC32C_POLY_REFLECTED: u32 = 0x82f63b78;

/// Compute the CRC-32C table at compile time.
const fn crc32c_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut k = 0;
        while k < 8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ CRC32C_POLY_REFLECTED
            } else {
                crc >> 1
            };
            k += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
}

static CRC32C_TABLE: [u32; 256] = crc32c_table();

/// Compute CRC-32C over the provided bytes.
pub fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &b in data {
        let idx = ((crc ^ b as u32) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[idx];
    }
    !crc
}

/// A fast incremental CRC-32C.
pub struct Crc32c {
    crc: u32,
}

impl Default for Crc32c {
    fn default() -> Self {
        Self::new()
    }
}

impl Crc32c {
    pub fn new() -> Crc32c {
        Crc32c { crc: !0u32 }
    }
    pub fn update(&mut self, data: &[u8]) {
        for &b in data {
            let idx = ((self.crc ^ b as u32) & 0xff) as usize;
            self.crc = (self.crc >> 8) ^ CRC32C_TABLE[idx];
        }
    }
    pub fn finalize(&self) -> u32 {
        !self.crc
    }
}

// ---------------------------------------------------------------------------
// Canonical hasher — unambiguous field framing
// ---------------------------------------------------------------------------

/// A hasher that writes length-prefixed parts so `field a + field b` can never
/// be confused with a different split because the concatenation is ambiguous.
/// All integer/number fields are encoded big-endian and length-prefixed.
#[derive(Clone)]
pub struct CanonicalHasher {
    bytes: Vec<u8>,
}

impl Default for CanonicalHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl CanonicalHasher {
    pub fn new() -> CanonicalHasher {
        CanonicalHasher { bytes: Vec::with_capacity(128) }
    }

    fn field_bytes(&mut self, tag: u8, body: &[u8]) {
        let mut prefix = [0u8; 9];
        prefix[0] = tag;
        let len = body.len() as u64;
        prefix[1..].copy_from_slice(&len.to_be_bytes());
        self.bytes.extend_from_slice(&prefix);
        self.bytes.extend_from_slice(body);
    }

    pub fn u8(&mut self, tag: u8, v: u8) -> &mut Self {
        self.field_bytes(tag, &[v]);
        self
    }
    pub fn u32(&mut self, tag: u8, v: u32) -> &mut Self {
        self.field_bytes(tag, &v.to_be_bytes());
        self
    }
    pub fn u64(&mut self, tag: u8, v: u64) -> &mut Self {
        self.field_bytes(tag, &v.to_be_bytes());
        self
    }
    pub fn u128(&mut self, tag: u8, v: u128) -> &mut Self {
        self.field_bytes(tag, &v.to_be_bytes());
        self
    }
    pub fn bool(&mut self, tag: u8, v: bool) -> &mut Self {
        self.field_bytes(tag, &[if v { 1 } else { 0 }]);
        self
    }
    pub fn bytes(&mut self, tag: u8, v: &[u8]) -> &mut Self {
        self.field_bytes(tag, v);
        self
    }
    pub fn str(&mut self, tag: u8, v: &str) -> &mut Self {
        self.field_bytes(tag, v.as_bytes());
        self
    }
    /// Specify an interface version so that two objects using different
    /// serialization rules never collide.
    pub fn version(&mut self, v: u32) -> &mut Self {
        self.u32(0, v)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn digest(&self) -> [u8; 32] {
        sha256(&self.bytes)
    }

    pub fn digest_hex(&self) -> String {
        hex_encode(&self.digest())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_empty() {
        assert_eq!(
            hex_encode(&sha256(b"")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_abc() {
        assert_eq!(
            hex_encode(&sha256(b"abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn sha256_long_message() {
        // 1,000,000 bytes of 'a' — FIPS test vector.
        let data = vec![b'a'; 1_000_000];
        assert_eq!(
            hex_encode(&sha256(&data)),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn sha256_incremental_matches_oneshot() {
        let data = b"The quick brown fox jumps over the lazy dog";
        let mut h = Sha256::new();
        h.update(&data[..10]);
        h.update(&data[10..]);
        assert_eq!(h.finalize(), sha256(data));
    }

    #[test]
    fn crc32c_known() {
        // Known CRC-32C value ("123456789").
        assert_eq!(crc32c(b"123456789"), 0xe3069283);
    }

    #[test]
    fn crc32c_incremental_matches_oneshot() {
        let mut c = Crc32c::new();
        c.update(b"1234");
        c.update(b"56789");
        assert_eq!(c.finalize(), crc32c(b"123456789"));
    }

    #[test]
    fn canonical_framing_is_unescapable() {
        let mut a = CanonicalHasher::new();
        a.bytes(1, b"ab").bytes(2, b"c");
        let mut b = CanonicalHasher::new();
        b.bytes(1, b"a").bytes(2, b"bc");
        // Different field boundaries must give different digests.
        assert_ne!(a.digest(), b.digest());
    }

    #[test]
    fn hex_roundtrip() {
        let d = sha256(b"hello");
        let e = hex_encode(&d);
        assert_eq!(hex_decode(&e).unwrap(), d.to_vec());
        assert!(hex_decode("abc").is_none());
    }
}
