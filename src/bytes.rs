//! Bounded, overflow-safe byte buffer helpers used by the protocol and
//! persistence layers. Every decode validates remaining length before reading;
//! every length is bounded by explicit limits; no length prefix can overflow.

use crate::error::{KvError, KvResult};

pub const MAX_FRAME_LEN: usize = 1 << 30; // 1 GiB hard cap for a single frame
pub const MAX_MSG_LEN: usize = 64 << 20; // 64 MiB for most control messages
pub const MAX_TOKENS: usize = 1 << 20; // upper bound on a prefix token sequence
pub const MAX_BLOCKS_PER_OBJECT: usize = 1 << 20;
pub const MAX_NAMESPACE_LEN: usize = 256;
pub const MAX_NODES: usize = 1 << 16;

fn check(cond: bool, msg: &str) -> KvResult<()> {
    if cond {
        Ok(())
    } else {
        Err(KvError::protocol(msg.to_string()))
    }
}

/// A forward-only, bounds-checked reader over a byte slice.
#[derive(Clone)]
pub struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(data: &'a [u8]) -> Cursor<'a> {
        Cursor { data, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }
    pub fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }
    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn take(&mut self, n: usize) -> KvResult<&'a [u8]> {
        check(self.remaining() >= n, "buffer underrun (short read)")?;
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    pub fn u8(&mut self) -> KvResult<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn bool(&mut self) -> KvResult<bool> {
        let b = self.u8()?;
        match b {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(KvError::protocol("invalid bool byte")),
        }
    }
    pub fn u16(&mut self) -> KvResult<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> KvResult<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> KvResult<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> KvResult<u128> {
        Ok(u128::from_be_bytes(self.take(16)?.try_into().unwrap()))
    }

    /// Read a length-prefixed byte vector with a hard bound.
    pub fn len_bytes(&mut self, max: usize) -> KvResult<Vec<u8>> {
        let len = self.u32()? as usize;
        check(len <= max, "length prefix exceeds bound")?;
        Ok(self.take(len)?.to_vec())
    }

    /// Read a length-prefixed string with a hard bound.
    pub fn len_str(&mut self, max: usize) -> KvResult<String> {
        let b = self.len_bytes(max)?;
        String::from_utf8(b).map_err(|_| KvError::protocol("non-utf8 string"))
    }

    pub fn remaining_slice(&mut self) -> KvResult<&'a [u8]> {
        Ok(self.take(self.remaining())?)
    }

    pub fn finish(&self) -> KvResult<()> {
        check(self.remaining() == 0, "trailing bytes after message")
    }
}

/// A growable writer that length-prefixes variable-length fields.
#[derive(Default, Clone)]
pub struct Buffer {
    data: Vec<u8>,
}

impl Buffer {
    pub fn new() -> Buffer {
        Buffer { data: Vec::new() }
    }
    pub fn with_capacity(n: usize) -> Buffer {
        Buffer { data: Vec::with_capacity(n) }
    }
    pub fn len(&self) -> usize {
        self.data.len()
    }
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    pub fn into_vec(self) -> Vec<u8> {
        self.data
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.data.push(v);
        self
    }
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.data.push(if v { 1 } else { 0 });
        self
    }
    pub fn u16(&mut self, v: u16) -> &mut Self {
        self.data.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.data.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.data.extend_from_slice(&v.to_be_bytes());
        self
    }
    pub fn u128(&mut self, v: u128) -> &mut Self {
        self.data.extend_from_slice(&v.to_be_bytes());
        self
    }

    pub fn len_bytes(&mut self, v: &[u8]) -> &mut Self {
        self.u32(v.len() as u32);
        self.data.extend_from_slice(v);
        self
    }
    /// Append raw bytes with no length prefix.
    pub fn bytes(&mut self, v: &[u8]) -> &mut Self {
        self.data.extend_from_slice(v);
        self
    }
    pub fn len_str(&mut self, v: &str) -> &mut Self {
        self.len_bytes(v.as_bytes())
    }
}

/// Encode a multiple-of-8 block of bytes as a debug hex string.
pub fn debug_hex(bytes: &[u8]) -> String {
    crate::hash::hex_encode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_reads_correctly() {
        let mut b = Buffer::new();
        b.u32(7).u64(9).len_bytes(b"abc").len_str("hello");
        let mut c = Cursor::new(&b.as_slice());
        assert_eq!(c.u32().unwrap(), 7);
        assert_eq!(c.u64().unwrap(), 9);
        assert_eq!(c.len_bytes(10).unwrap(), b"abc");
        assert_eq!(c.len_str(10).unwrap(), "hello");
        c.finish().unwrap();
    }

    #[test]
    fn cursor_rejects_underrun() {
        let b = [1u8, 2];
        let mut c = Cursor::new(&b);
        assert_eq!(c.u32().unwrap_err().kind(), crate::error::ErrorKind::Protocol);
    }

    #[test]
    fn cursor_rejects_oversized_len() {
        let mut b = Buffer::new();
        b.u32(1000).u8(0);
        let mut c = Cursor::new(&b.as_slice());
        assert!(c.len_bytes(10).is_err());
    }

    #[test]
    fn cursor_rejects_trailing() {
        let mut b = Buffer::new();
        b.u8(1).u8(2);
        let mut c = Cursor::new(&b.as_slice());
        c.u8().unwrap();
        // one byte remains => finish fails
        assert!(c.finish().is_err());
    }
}
