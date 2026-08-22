# Validation Report — KV Fabric 1.0.0

This report records only what was actually built, executed and observed in the final validation runs. No number is asserted without a corresponding run.

## Build

+ **Build status:** clean (debug + release).
+ **Compiler/toolchain:** rustc 1.93.0 (2026-01-19), host `x86_64-pc-windows-msvc`, LLVM 21.1.8.
+ **Platform:** Windows (Win32), `x86_64-pc-windows-msvc` target.
+ **Warnings:** 0. The build is configured with `-D warnings`; `cargo build`, `cargo build --release`, `cargo test`, `cargo test --release` and example builds all terminated with no warnings.
+ **Dependencies:** zero external crates. The core crate and `kvfabric-ffi` use only the Rust standard library. Fully offline-reproducible.
+ **CUDA feature:** `cargo build --features cuda` compiles cleanly.

## Tests

+ **Total tests:** 106 (94 unit + 10 integration + 1 multi-process TCP + 1 CUDA FFI crate test).
+ **Passing:** 106. **Failed:** 0. **Ignored:** 0.
+ **Unit tests (94):** cover the hash (SHA-256 FIPS-180 vectors, CRC-32C Castagnoli, canonical framing), identity, bytes/bounded codec, compatibility key, lifecycle & residency state machines, prefix trie (exact, longest, branching, namespace/compat isolation, idempotent commit, concurrent reads), block math, planner (economy), admission, reclaim, leases (expiry, stale boot, write conflict), integrity (size/corruption/anchors), telemetry, storage (host, atomic filesystem, path-safety, capacity, corruption), persistence (store/load/recover, corruption detected, manifest truncation, remove), accelerator (CPU store), protocol (frame round-trip, CRC corruption, bad magic, payload round-trips), coordinator (epoch advance on restart, boot rejection), node (create/commit/lookup, persist/restore cycles), migration (ownership transfer), api (end-to-end lifecycle).
+ **Integration tests (10):** prefix reuse correctness, compatibility & namespace scoping, branching isolation, bounded capacity, persistence recovery round-trip, corruption detection + non-readmission, concurrent lookups, migration ownership, lifecycle+integrity round-trip, coordinator boot rejection.
+ **Multi-process test (1):** spawns a real `kvfabric coordinator` and a real `kvfabric node` as separate OS processes, then queries the coordinator over framed TCP from a client and asserts the authority epoch and node registration.
+ **Restart / stability:** the full suite was executed 3 times independently (debug, release, debug) and the seven examples were run with a clean data directory; every run passed with no dependency on stale state (test dirs are per-process temp paths and removed; example dirs are removed).

## Property / security / failure coverage

+ **Property-style:** compatibility-key determinism, canonical-framing unescapability, monotonic generations, deterministic planner/admission/reclaim tie-breaking, CRC-32C/SHA-256 known vectors, block sizing/alignment rounding.
+ **Concurrency:** concurrent prefix reads (threaded), concurrent lookups (threaded).
+ **Corruption:** integrity bit-flip detection, persistence corruption not re-admitted, truncated manifest tolerated, frame CRC corruption rejected, bad-magic rejected.
+ **Capacity / quota:** host tier exact byte accounting + hard capacity, storage tier capacity, append/commit capacity rejection.
+ **Failure injection:** storage write/rename paths, manifest truncation, corrupt block bit-flip, restart with recovered durable state, coordinator epoch advance.
+ **Authority / stale rejection:** stale boot id rejected, stale generation rejected, invalid lease rejected, non-owner lease release rejected.

## Examples

**7 examples**, all executing cleanly under a fresh data directory:

+ **A. Shared system prompt** — reused 1000/1003 tokens (prefix hit).
+ **B. Branching conversation** — branch B saw only branch B state (branch isolation).
+ **C. Tiered residency** — promote (accelerator) -> demote (host) -> persist -> reclaim -> restore -> verify.
+ **D. Distributed reuse** — node B missed before transfer, hit after transfer + re-association; canonical owner changed via migration.
+ **E. Capacity pressure** — 16 admitted, 1 capacity rejection; resident bytes freed to 0 after reclamation.
+ **F. Crash recovery** — object persisted, process simulated crash, fresh runtime recovered it and served it.
+ **G. Compatibility rejection** — incompatible model/revision rejected (0 tokens), exact compatible hit (8 tokens).

## Benchmarks (release)

+ commit: 0.027 ms/op
+ exact/longest-prefix lookup: 4.988 us/op (2000 hits)
+ integrity (CRC-32C + SHA-256 over 1 MiB): 442.07 MiB/s
+ persist + reclaim + restore: 7.797 ms
+ promote + demote: 0.023 ms

## Sanitizers

+ ASan/MSan/TSan were **not run** because only a stable toolchain is installed (these require a nightly `-Z sanitizer` build).
+ Equivalent safeguards in place: `forbid(unsafe_code)` in the core crate (zero `unsafe` outside the contained `kvfabric-ffi` CUDA loader, which is limited to dynamic-library loading and function-pointer calls), `overflow-checks = true` in dev and release profiles, and full debug assertions. This is an honest limitation, not a substitute for a sanitizer run.

## Backends

+ **CPU:** validated (allocation + bounded capacity + integrity + promotion/demotion).
+ **CUDA:** validated as a **real** backend. The `kvfabric-ffi` CUDA loader test performed a device allocation, host-to-device copy, device-to-host copy and free on an NVIDIA GeForce RTX 5090 (CUDA driver 610.88, CUDA toolchain 12.9/13.1).

## Recovery

Recovery validated through: persistence recover round-trip, crash_recovery example, coordinator restart epoch advance, corruption-detect-and-skip on recovery. No phantom objects, no invented commits, no corrupt object re-admitted, no stale authority after restart hierarchy observed.

## Resource accounting

Host and accelerator tiers report exact used/capacity. The capacity-pressure example shows resident bytes returning to 0 after reclamation; persistence recovery releases the scratch directory. No resource leaked in the examples or tests.

## Known limitations

+ HIP, Level Zero, Metal and Vulkan backends are architecturally supported (same `DeviceStore` trait) but not implemented for 1.0.0.
+ ASan/MSan/TSan not run (stable toolchain only).
+ Durable objects recover metadata and blocks; the caller re-associates the token sequence for prefix reuse (metadata does not store the token list).
+ Framed TCP is the transport; QUIC is a future option. RPC timeouts are bounded but not yet a full distributed retry/backoff protocol.
+ The CUDA backend validates allocation/transfer/free; device H2D/D2H bandwidth is not yet a benchmark axis.

## Release criteria status

The repository builds (zero warnings), all 106 tests pass, 7 examples run, the benchmark suite runs, the multi-process control plane works, persistence works, corruption is detected, crash recovery works, capacity is bounded, authority rules are enforced, stale generations and stale replicas are rejected, prefix compatibility is enforced, lifecycle transitions are validated, cleanup returns resources, and documentation matches the implemented behavior. No known critical correctness defect remains.