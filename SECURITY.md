# Security

KV Fabric assumes the control plane and durable store may receive untrusted or malformed input. The runtime enforces the following boundaries.

## Input bounds

+ Bounded frame and message sizes (a hard cap on payload length).
+ Strict decoding of every field; any overrun, non-UTF8 string, unknown op, or bad enum is rejected.
+ Integer-overflow protection throughout (checked arithmetic and saturating helpers).
+ Bounded collection sizes (namespace length, token count, capability/object/replica counts).

## Path safety

All on-disk filenames are derived from hex-encoded keys, so path traversal (../../) is impossible. Remote callers cannot choose arbitrary filesystem paths.

## Isolation and authority

+ Namespace isolation is part of cache lookup and authorization; different namespaces never collide even for identical prompts.
+ Stale authority (from a previous epoch or a restarted worker with a different boot id) is rejected.
+ Invalid or expired leases are rejected; only the holder+boot pair may release a lease.
+ Invalid (stale or non-monotonic) generations are rejected.
+ A mutation by a non-owner is unauthorized.

## Transport hardening

+ Frame CRC-32C validation detects corruption/truncation; malformed frames are rejected, not partially interpreted.
+ Request ids and (in state-changing messages) epoch + generation + boot id provide replay resistance; ambiguous state-changing RPCs are not blindly retried.
+ Connections are retired cleanly; a remote peer cannot cause unbounded allocation.

## Resource quotas

Per-namespace quotas bound object count and bytes; host and accelerator tiers are hard-capped. Admission and reclamation keep usage bounded. A remote peer cannot exhaust arbitrary memory.

## Acknowledgement

This is a security *design* document for a research-grade runtime; it is not a claim of formal verification. Fuzz-like malformed-input and corruption tests are part of the suite.