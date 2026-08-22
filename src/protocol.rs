//! Framed TCP control-plane protocol.
//!
//! Frames are length-prefixed and CRC-validated. Every frame carries a request
//! id, an operation code, a version and a bounded payload. Decoding rejects
//! malformed, oversized or unknown frames. State-changing RPCs carry enough
//! authority state for the receiver to reject stale authority and stale
//! generation.

use std::io::{Read, Write};

use crate::bytes::{Buffer, Cursor};
use crate::error::{KvError, KvResult};
use crate::hash::crc32c;
use crate::id::{NodeId, ObjectId, Token};
use crate::lifecycle::LifecycleState;

pub const PROTOCOL_MAGIC: u32 = 0x4b56_4630; // "KVF0"
pub const PROTOCOL_VERSION: u8 = 1;
pub const FRAME_HEADER_LEN: usize = 4 + 1 + 1 + 8 + 1 + 4; // magic+ver+flags+reqid+op+len
pub const MAX_FRAME_PAYLOAD: usize = 64 << 20;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Op {
    Register = 1,
    Heartbeat = 2,
    Lookup = 3,
    LeaseAcquire = 4,
    LeaseRelease = 5,
    ObjectOp = 6,
    FetchBlocks = 7,
    MigrateCommit = 8,
    Status = 9,
    Stats = 10,
    Reply = 128,
    ErrCode = 129,
}

impl Op {
    pub fn from_u8(v: u8) -> Option<Op> {
        Some(match v {
            1 => Op::Register,
            2 => Op::Heartbeat,
            3 => Op::Lookup,
            4 => Op::LeaseAcquire,
            5 => Op::LeaseRelease,
            6 => Op::ObjectOp,
            7 => Op::FetchBlocks,
            8 => Op::MigrateCommit,
            9 => Op::Status,
            10 => Op::Stats,
            128 => Op::Reply,
            129 => Op::ErrCode,
            _ => return None,
        })
    }
}

/// A decoded frame.
#[derive(Debug, Clone)]
pub struct Frame {
    pub request_id: u64,
    pub op: Op,
    pub payload: Vec<u8>,
}

/// Encode a frame into bytes (with the trailing CRC).
pub fn encode_frame(request_id: u64, op: Op, payload: &[u8]) -> KvResult<Vec<u8>> {
    if payload.len() > MAX_FRAME_PAYLOAD {
        return Err(KvError::protocol("payload exceeds frame bound"));
    }
    let mut b = Buffer::new();
    b.u32(PROTOCOL_MAGIC);
    b.u8(PROTOCOL_VERSION);
    b.u8(0); // flags
    b.u64(request_id);
    b.u8(op as u8);
    b.u32(payload.len() as u32);
    b.bytes(payload);
    let mut raw = b.into_vec();
    let crc = crc32c(&raw);
    raw.extend_from_slice(&crc.to_be_bytes());
    Ok(raw)
}

/// Read exactly one frame from a stream (blocking). Bounded and validated.
pub fn read_frame<R: Read>(r: &mut R) -> KvResult<Frame> {
    let mut header = [0u8; FRAME_HEADER_LEN];
    r.read_exact(&mut header).map_err(|e| KvError::network(format!("read header: {}", e)))?;
    let mut c = Cursor::new(&header);
    let magic = c.u32()?;
    if magic != PROTOCOL_MAGIC {
        return Err(KvError::protocol("bad frame magic"));
    }
    let version = c.u8()?;
    if version != PROTOCOL_VERSION {
        return Err(KvError::protocol(format!("unsupported protocol version {}", version)));
    }
    let _flags = c.u8()?;
    let request_id = c.u64()?;
    let op_u8 = c.u8()?;
    let op = Op::from_u8(op_u8).ok_or_else(|| KvError::protocol(format!("unknown op {}", op_u8)))?;
    let payload_len = c.u32()? as usize;
    if payload_len > MAX_FRAME_PAYLOAD {
        return Err(KvError::protocol("payload exceeds frame bound"));
    }
    let mut payload = vec![0u8; payload_len];
    r.read_exact(&mut payload).map_err(|e| KvError::network(format!("read payload: {}", e)))?;
    let mut crc_buf = [0u8; 4];
    r.read_exact(&mut crc_buf).map_err(|e| KvError::network(format!("read crc: {}", e)))?;
    let expected = u32::from_be_bytes(crc_buf);
    let mut all = Vec::with_capacity(header.len() + payload_len);
    all.extend_from_slice(&header);
    all.extend_from_slice(&payload);
    let actual = crc32c(&all);
    if actual != expected {
        return Err(KvError::integrity("frame crc mismatch"));
    }
    Ok(Frame { request_id, op, payload })
}

/// Write a frame to a stream (flushing).
pub fn write_frame<W: Write>(w: &mut W, request_id: u64, op: Op, payload: &[u8]) -> KvResult<()> {
    let raw = encode_frame(request_id, op, payload)?;
    w.write_all(&raw).map_err(|e| KvError::network(format!("write frame: {}", e)))?;
    w.flush().map_err(|e| KvError::network(format!("flush: {}", e)))?;
    Ok(())
}

// --- payload codecs --------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct RegisterMsg {
    pub node_id: NodeId,
    pub boot_id: u128,
    pub listen_port: u16,
    pub capabilities: Vec<String>,
}

pub fn encode_register(m: &RegisterMsg) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u64(m.node_id.to_u64());
    b.u128(m.boot_id);
    b.u16(m.listen_port);
    b.u32(m.capabilities.len() as u32);
    for c in &m.capabilities {
        b.len_str(c);
    }
    b.into_vec()
}

pub fn decode_register(p: &[u8]) -> KvResult<RegisterMsg> {
    let mut c = Cursor::new(p);
    let node_id = NodeId::from_u64(c.u64()?);
    let boot_id = c.u128()?;
    let listen_port = c.u16()?;
    let count = c.u32()? as usize;
    if count > 256 {
        return Err(KvError::protocol("too many capabilities"));
    }
    let mut caps = Vec::with_capacity(count);
    for _ in 0..count {
        caps.push(c.len_str(256)?);
    }
    c.finish()?;
    Ok(RegisterMsg { node_id, boot_id, listen_port, capabilities: caps })
}

#[derive(Debug, Clone, PartialEq)]
pub struct LookupMsg {
    pub namespace: String,
    pub compat_key: [u8; 32],
    pub tokens: Vec<Token>,
}

pub fn encode_lookup(m: &LookupMsg) -> Vec<u8> {
    let mut b = Buffer::new();
    b.len_str(&m.namespace);
    b.len_bytes(&m.compat_key);
    b.u32(m.tokens.len() as u32);
    for t in &m.tokens {
        b.u32(*t);
    }
    b.into_vec()
}

pub fn decode_lookup(p: &[u8]) -> KvResult<LookupMsg> {
    let mut c = Cursor::new(p);
    let namespace = c.len_str(crate::bytes::MAX_NAMESPACE_LEN)?;
    let mut compat_key = [0u8; 32];
    compat_key.copy_from_slice(c.len_bytes(32)?.as_slice());
    let n = c.u32()? as usize;
    if n > crate::bytes::MAX_TOKENS {
        return Err(KvError::protocol("token count exceeds bound"));
    }
    let mut tokens = Vec::with_capacity(n);
    for _ in 0..n {
        tokens.push(c.u32()?);
    }
    c.finish()?;
    Ok(LookupMsg { namespace, compat_key, tokens })
}

/// Result of a lookup carried in a Reply payload.
#[derive(Debug, Clone, PartialEq)]
pub struct LookupResult {
    pub prefix_id: Option<u64>,
    pub token_len: usize,
    pub object_ids: Vec<ObjectId>,
}

pub fn encode_lookup_result(r: &LookupResult) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u8(if r.prefix_id.is_some() { 1 } else { 0 });
    if let Some(p) = r.prefix_id {
        b.u64(p);
    }
    b.u32(r.token_len as u32);
    b.u32(r.object_ids.len() as u32);
    for o in &r.object_ids {
        b.u128(o.to_u128());
    }
    b.into_vec()
}

pub fn decode_lookup_result(p: &[u8]) -> KvResult<LookupResult> {
    let mut c = Cursor::new(p);
    let has = c.u8()? != 0;
    let prefix_id = if has { Some(c.u64()?) } else { None };
    let token_len = c.u32()? as usize;
    let n = c.u32()? as usize;
    if n > 1024 {
        return Err(KvError::protocol("object count exceeds bound"));
    }
    let mut object_ids = Vec::with_capacity(n);
    for _ in 0..n {
        object_ids.push(ObjectId::from_u128(c.u128()?));
    }
    c.finish()?;
    Ok(LookupResult { prefix_id, token_len, object_ids })
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeaseAcquireMsg {
    pub object_id: ObjectId,
    pub holder: NodeId,
    pub boot_id: u128,
    pub kind: u8,
    pub ttl: u64,
}

pub fn encode_lease_acquire(m: &LeaseAcquireMsg) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u128(m.object_id.to_u128());
    b.u64(m.holder.to_u64());
    b.u128(m.boot_id);
    b.u8(m.kind);
    b.u64(m.ttl);
    b.into_vec()
}
pub fn decode_lease_acquire(p: &[u8]) -> KvResult<LeaseAcquireMsg> {
    let mut c = Cursor::new(p);
    let object_id = ObjectId::from_u128(c.u128()?);
    let holder = NodeId::from_u64(c.u64()?);
    let boot_id = c.u128()?;
    let kind = c.u8()?;
    let ttl = c.u64()?;
    c.finish()?;
    Ok(LeaseAcquireMsg { object_id, holder, boot_id, kind, ttl })
}

#[derive(Debug, Clone, PartialEq)]
pub struct LeaseReleaseMsg {
    pub object_id: ObjectId,
    pub lease_id: u128,
    pub holder: NodeId,
    pub boot_id: u128,
}

pub fn encode_lease_release(m: &LeaseReleaseMsg) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u128(m.object_id.to_u128());
    b.u128(m.lease_id);
    b.u64(m.holder.to_u64());
    b.u128(m.boot_id);
    b.into_vec()
}
pub fn decode_lease_release(p: &[u8]) -> KvResult<LeaseReleaseMsg> {
    let mut c = Cursor::new(p);
    let object_id = ObjectId::from_u128(c.u128()?);
    let lease_id = c.u128()?;
    let holder = NodeId::from_u64(c.u64()?);
    let boot_id = c.u128()?;
    c.finish()?;
    Ok(LeaseReleaseMsg { object_id, lease_id, holder, boot_id })
}

/// Residency/lifecycle operation request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ObjectOpKind {
    Promote = 1,
    Demote = 2,
    Persist = 3,
    Restore = 4,
    Reclaim = 5,
    Retire = 6,
}

impl ObjectOpKind {
    pub fn from_u8(v: u8) -> Option<ObjectOpKind> {
        Some(match v {
            1 => ObjectOpKind::Promote,
            2 => ObjectOpKind::Demote,
            3 => ObjectOpKind::Persist,
            4 => ObjectOpKind::Restore,
            5 => ObjectOpKind::Reclaim,
            6 => ObjectOpKind::Retire,
            _ => return None,
        })
    }
    pub fn as_str(self) -> &'static str {
        match self {
            ObjectOpKind::Promote => "promote",
            ObjectOpKind::Demote => "demote",
            ObjectOpKind::Persist => "persist",
            ObjectOpKind::Restore => "restore",
            ObjectOpKind::Reclaim => "reclaim",
            ObjectOpKind::Retire => "retire",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectOpMsg {
    pub object_id: ObjectId,
    pub kind: ObjectOpKind,
    pub target_tier: u8,
}

pub fn encode_object_op(m: &ObjectOpMsg) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u128(m.object_id.to_u128());
    b.u8(m.kind as u8);
    b.u8(m.target_tier);
    b.into_vec()
}
pub fn decode_object_op(p: &[u8]) -> KvResult<ObjectOpMsg> {
    let mut c = Cursor::new(p);
    let object_id = ObjectId::from_u128(c.u128()?);
    let kind = ObjectOpKind::from_u8(c.u8()?).ok_or_else(|| KvError::protocol("bad object op"))?;
    let target_tier = c.u8()?;
    c.finish()?;
    Ok(ObjectOpMsg { object_id, kind, target_tier })
}

#[derive(Debug, Clone, PartialEq)]
pub struct FetchBlocksMsg {
    pub object_id: ObjectId,
    pub from_generation: u64,
}

pub fn encode_fetch_blocks(m: &FetchBlocksMsg) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u128(m.object_id.to_u128());
    b.u64(m.from_generation);
    b.into_vec()
}
pub fn decode_fetch_blocks(p: &[u8]) -> KvResult<FetchBlocksMsg> {
    let mut c = Cursor::new(p);
    let object_id = ObjectId::from_u128(c.u128()?);
    let from_generation = c.u64()?;
    c.finish()?;
    Ok(FetchBlocksMsg { object_id, from_generation })
}

#[derive(Debug, Clone, PartialEq)]
pub struct MigrateCommitMsg {
    pub object_id: ObjectId,
    pub new_owner: NodeId,
    pub new_owner_boot: u128,
    pub generation: u64,
    pub authority_epoch: u64,
}

pub fn encode_migrate_commit(m: &MigrateCommitMsg) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u128(m.object_id.to_u128());
    b.u64(m.new_owner.to_u64());
    b.u128(m.new_owner_boot);
    b.u64(m.generation);
    b.u64(m.authority_epoch);
    b.into_vec()
}
pub fn decode_migrate_commit(p: &[u8]) -> KvResult<MigrateCommitMsg> {
    let mut c = Cursor::new(p);
    let object_id = ObjectId::from_u128(c.u128()?);
    let new_owner = NodeId::from_u64(c.u64()?);
    let new_owner_boot = c.u128()?;
    let generation = c.u64()?;
    let authority_epoch = c.u64()?;
    c.finish()?;
    Ok(MigrateCommitMsg { object_id, new_owner, new_owner_boot, generation, authority_epoch })
}

pub fn encode_lifecycle_state(state: LifecycleState) -> Vec<u8> {
    let mut b = Buffer::new();
    b.u8(state.code());
    b.into_vec()
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn frame_roundtrip() {
        let payload = encode_register(&RegisterMsg {
            node_id: NodeId(1),
            boot_id: 0xabc,
            listen_port: 7000,
            capabilities: vec!["cpu".into(), "cuda".into()],
        });
        let raw = encode_frame(42, Op::Register, &payload).unwrap();
        let mut cur = std::io::Cursor::new(&raw);
        let frame = read_frame(&mut cur).unwrap();
        assert_eq!(frame.request_id, 42);
        assert_eq!(frame.op, Op::Register);
        let back = decode_register(&frame.payload).unwrap();
        assert_eq!(back.node_id, NodeId(1));
        assert_eq!(back.boot_id, 0xabc);
        assert_eq!(back.capabilities, vec!["cpu".to_string(), "cuda".to_string()]);
    }
    #[test]
    fn frame_rejects_crc_corruption() {
        let raw = encode_frame(1, Op::Heartbeat, b"x").unwrap();
        let mut corrupted = raw.clone();
        let n = corrupted.len();
        corrupted[n - 1] ^= 0xff;
        let mut cur = std::io::Cursor::new(&corrupted);
        assert!(read_frame(&mut cur).is_err());
    }
    #[test]
    fn frame_rejects_bad_magic() {
        let raw = encode_frame(1, Op::Heartbeat, b"x").unwrap();
        let mut bad = raw.clone();
        bad[0] ^= 0xff;
        let mut cur = std::io::Cursor::new(&bad);
        let e = read_frame(&mut cur).unwrap_err();
        assert_eq!(e.kind(), crate::error::ErrorKind::Protocol);
    }
    #[test]
    fn lookup_roundtrip() {
        let m = LookupMsg { namespace: "ns".into(), compat_key: [9u8; 32], tokens: vec![1, 2, 3] };
        let p = encode_lookup(&m);
        assert_eq!(decode_lookup(&p).unwrap(), m);
        let r = LookupResult { prefix_id: Some(7), token_len: 3, object_ids: vec![ObjectId::from_u128(5)] };
        let rp = encode_lookup_result(&r);
        assert_eq!(decode_lookup_result(&rp).unwrap(), r);
    }
    #[test]
    fn object_op_roundtrip() {
        let m = ObjectOpMsg { object_id: ObjectId::from_u128(1), kind: ObjectOpKind::Promote, target_tier: 1 };
        let p = encode_object_op(&m);
        assert_eq!(decode_object_op(&p).unwrap(), m);
        assert_eq!(ObjectOpKind::from_u8(2).map(|k| k.as_str()), Some("demote"));
        assert!(ObjectOpKind::from_u8(99).is_none());
    }
}