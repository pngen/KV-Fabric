# Contributing

KV Fabric is a systems project. Contributions are welcome when they preserve correctness, determinism, boundedness and explicit authority.

## Build and test

```
cargo build
cargo test
cargo build --features cuda   # optional, validates the CUDA loader
cargo bench
```

The build is configured with `-D warnings`, so the public tree is warning-free by construction. A new change must compile with zero warnings.

## Guidelines

+ Prefer pure safe Rust in the core crate; the crate has `forbid(unsafe_code)` (the only exception is the contained `kvfabric-ffi` CUDA loader).
+ Do not add external dependencies; the crate is intentionally dependency-free for offline reproducibility.
+ Keep a `forbid`/deny on state-machine transitions; never allow an illegal transition.
+ Never re-enter the same `RwLock` while holding a guard (that deadlocks). Snapshot/recovery work happens after releasing the guard.
+ Keep planning decisions deterministic (sorted block order, explicit tie-breaks, policy version ids).
+ Add tests and, where relevant, property/failure-injection coverage for every behavior change.
+ Use plain-markdown style in docs; keep claims matched to what the code actually proves.

## Reporting a bug

Give the exact failing invariant, the reproduction, and the kind of failure. Because the project treats reuse correctness as paramount, a false cache hit or a stale-replica read is a critical bug.