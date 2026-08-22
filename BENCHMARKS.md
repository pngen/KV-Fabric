# Benchmarks

KV Fabric ships a `harness = false` benchmark binary under `benches/benchmarks.rs` (runnable via `cargo bench`). It measures actual operations through the runtime facade on a scratch directory; nothing is fabricated. Measurements below were produced on the release profile on the development machine (x86_64-pc-windows-msvc, NVIDIA RTX 5090 with CUDA 13 driver / CUDA 12.9 toolchain).

## Results (release)

+ commit (create + append + commit), 50 objects with a 4 KiB block: **0.027 ms/op** (1.355 ms total).
+ exact / longest-prefix lookup, 2000 iterations over a 1024-token index: **4.988 us/op**, **2000 hits**.
+ integrity (CRC-32C + SHA-256 over 1 MiB), 50 iterations: **442.07 MiB/s**.
+ persist + reclaim + restore (one ~16 KiB object): **7.797 ms** total.
+ promote + demote (host to accelerator, then back): **0.023 ms**.

## What is measured

+ exact lookup
+ longest-prefix lookup (reuse of a committed prefix)
+ object admission / commit
+ block write
+ host/resident copy
+ persistence + restore (durable store round trip)
+ integrity verification (CRC-32C + SHA-256 throughput)
+ promotion / demotion between tiers

## Benchmark targets addressed

The suite exercises the read path (lookup), the write/admit path (commit), the durability path (persist/restore), the integrity path, and the residency path (promote/demote). Migration and replica-fetch are exercised by the multi-process and integration tests rather than by the micro-benchmark, because they are dominated by the transfer/authority logic.

## CUDA

The `cuda` feature enables the CUDA device backend. The FFI crate test performs a real device allocation, host-to-device copy, device-to-host copy and free, validated on NVIDIA hardware. Device H2D/D2H bandwidth is not yet included as a benchmark axis in the release suite.