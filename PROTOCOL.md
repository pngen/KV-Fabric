# Protocol

KV Fabric uses a bounded, length-prefixed, CRC-validated framed TCP protocol for the control plane.

## Frame layout

Each frame is: `magic(4) | version(1) | flags(1) | request_id(8) | op(1) | payload_len(4) | payload | crc32c(4)`.

+ `magic` is 0x4b564630 (KVF0). A wrong magic is a protocol error.
+ `version` is currently 1. A mismatch is rejected.
+ `request_id` correlates a request to its reply (replay resistance requires the caller to bound outstanding request ids).
+ `op` is the operation code (1..10 requests, 128 reply, 129 error).
+ `payload_len` is bounded by `MAX_FRAME_PAYLOAD` (64 MiB).
+ `crc32c` covers the header and payload. Corruption is detected.

## Operations

+ `Register` (1): node id, boot id, listen port, capabilities. Returns the current authority epoch.
+ `Heartbeat` (2): node id + boot id. Returns the epoch, or rejects a stale boot.
+ `Lookup` (3): namespace, compatibility key, token list. Returns the longest reusable prefix match.
+ `LeaseAcquire` (4) / `LeaseRelease` (5): object, holder, boot, kind, ttl / lease id. Enforce authority.
+ `ObjectOp` (6): promote / demote / persist / restore / reclaim / retire an object.
+ `FetchBlocks` (7): fetch resident block payloads for an object from a peer.
+ `MigrateCommit` (8): commit a migration to a new owner with a bumped generation.
+ `Status` (9) / `Stats` (10): runtime/cluster inspection.
+ `Reply` (128) / `ErrCode` (129): structured success / error with a stable error code + message.

## Validation and security

+ All lengths are validated before any allocation, and bounded by explicit limits (namespace length, token count, capability count, object count).
+ Malformed, truncated, oversized or unknown-op frames are rejected, never partially interpreted.
+ Path-transversal is impossible because all on-disk filenames are derived from hex-encoded keys.
+ State-changing messages carry holder + boot id + epoch + generation, enabling stale-authority / stale-generation / invalid-lease rejection.
+ Errors are structured (a stable code + message), never a bare boolean.

## Timeouts and retry

Connections carry a bounded RPC timeout. Ambiguous state-changing RPCs are not blindly retried; the caller must establish idempotency (e.g. deterministic request ids, generation monotonicity) before retrying.

## Multi-process

The `coordinator` and `node` CLI subcommands run as separate OS processes speaking this protocol. The multi-process integration test starts both and queries the coordinator from a client over TCP.