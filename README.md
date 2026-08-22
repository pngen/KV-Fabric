# KV Fabric 1.0.0

**A vendor-neutral distributed runtime for governing reusable KV inference state across accelerator memory, system memory, persistent storage, processes, and execution nodes.**

KV Fabric answers a specific systems question:

> **Where should reusable inference state live, when should it move, who may use it, and when is reuse cheaper than recomputation?**

Modern AI inference accumulates large amounts of reusable state: transformer KV cache, prefix cache, prompt cache, paged-attention blocks, session state, recurrent state, multimodal context, speculative-decoding state, shared system prompts, reusable conversation prefixes, and application-defined cache objects. This state is expensive to reconstruct, yet it consumes scarce accelerator memory. KV Fabric governs that tradeoff explicitly.

## What KV Fabric is

+ A runtime for **identity**, **residency**, **placement**, **reuse**, **sharing**, **migration**, **promotion**, **demotion**, **persistence**, **recomputation economics**, **reclamation**, **integrity**, **lifecycle**, **crash recovery**, and **distributed authority** for reusable inference state.
+ First-class 128-bit object identity with explicit, versioned metadata covering compatibility identity, geometry, lineage, lifecycle, durability, integrity and economics.
+ A correct-first reusable-prefix structure (a per-(namespace, compatibility) hashed trie) that supports exact and longest-prefix reuse across branching conversations and shared system prompts. A false compatibility hit is a correctness failure, so the compatibility key is a canonical SHA-256 over every field that can change token meaning or cache layout.
+ Block-level KV management with configurable block sizing, bounded capacity, and exact resource accounting per tier.
+ Tier abstraction (accelerator, host, persistent storage) with an explicit residency state machine, policy-driven promotion/demotion, and a deterministic transfer-vs-recompute planner.
+ Bounded admission control, deterministic reclamation, ownership and leases, a coordinator with authority epochs and node-boot identities, replica management, and a disciplined migration sequence (plan, reserve, transfer, verify, commit, retire).
+ Durable persistence with atomic commits and first-class crash recovery; SHA-256 manifest anchors and CRC-32C block checksums detect corruption.
+ A real framed-TCP control plane so coordinator and node can run as separate processes (no fake in-process distributed behavior).
+ A real CUDA accelerator backend (opt-in cuda feature, loaded dynamically with no link-time CUDA dependency) plus a universally-validated CPU backend.
## What KV Fabric is not

+ It is **not** an LRU cache, a local allocator, a tensor dictionary, a storage cache, or an inference-server plugin.
+ It does not depend on vLLM, SGLang, TensorRT-LLM, llama.cpp, or any specific engine; it exposes an engine-adapter boundary.
+ It is not a replacement for FlashTier (bytes), Context Fabric (reusable computational state), Compute Fabric (execution placement), Reclaim Fabric (state reclamation), or Checkpoint Fabric (execution survival). It is a specialized vertical runtime that applies those principles to reusable KV and prefix state.
## Architecture

The crate is a self-contained, dependency-free Rust library plus a `kvfabric` binary. The core modules are:

| Module | Responsibility |
|--------|----------------|
| `id`, `hash`, `bytes`, `rng` | 128-bit identity, SHA-256/CRC-32C, bounded wire buffers, deterministic PRNG |
| `metadata` | The first-class KVObject metadata and canonical serialization |
| `lifecycle` | Residency and lifecycle state machines with enforced transitions |
| `prefix` | Reusable-prefix trie (exact + longest-prefix, branching, immutability) |
| `block` | Block identities, sizing, grouping, alignment |
| `economy` | Deterministic transfer-vs-recompute planner |
| `admission`, `reclaim` | Bounded admission and deterministic reclamation policy |
| `lease` | Leases and mutation authority |
| `integrity`, `telemetry` | SHA-256/CRC-32C verification, quarantine, counters |
| `storage`, `accelerator` | Host + persistent stores; CPU + CUDA device backends |
| `persistence` | Atomic durable object store with manifest + recovery |
| `protocol` | Framed TCP control-plane codec |
| `coordinator` | Authority epochs, node registry, ownership, leases, quotas, durable snapshot |
| `node` | Local runtime: residency, transfers, eviction, recovery, serve |
| `migration`, `replica` | Migration sequence and replica validity |
| `api`, `cli`, `bench` | Public facade, CLI, in-process benchmark suite |
## Supported tiers

+ **Accelerator** (CPU fallback, or real CUDA via the cuda feature)
+ **Host** (memory, byte-accounted and capacity-bounded)
+ **Persistent storage** (local filesystem/NVMe with atomic commits)
## Quick start

Build and run the test suite:

```
cargo build
cargo test
```

Run the seven worked examples:

```
for exe in shared_system_prompt branching_conversation tiered_residency distributed_reuse capacity_pressure crash_recovery compatibility_rejection; do
  ./target/debug/examples/$exe
done
```

Run the benchmark suite:

```
cargo bench
```

Run a coordinator and node as separate processes, then query the cluster:

```
./target/debug/kvfabric coordinator --listen 127.0.0.1:7200 --data ./coord
./target/debug/kvfabric node --id 7 --coordinator 127.0.0.1:7200 --listen 127.0.0.1:7300 --data ./node7
./target/debug/kvfabric status --coordinator 127.0.0.1:7200
```

The CLI also manipulates a durable per-directory store, so a create in one invocation is recoverable by a later lookup:

```
./target/debug/kvfabric create --ns default --model llama --tokens 1,2,3,4 --size 64 --data ./kvfabric-data
./target/debug/kvfabric lookup --ns default --model llama --tokens 1,2,3,4 --data ./kvfabric-data
```

## Examples

+ **A. Shared system prompt** - build a long prefix once, then show a later request reuses it (reduced recomputation).
+ **B. Branching conversation** - a shared immutable prefix forks into two independent branches without cross-branch mutation.
+ **C. Tiered residency** - an object moves hot to host to storage and back.
+ **D. Distributed reuse** - node A creates state, node B requests it; the runtime transfers a replica or migrates ownership.
+ **E. Capacity pressure** - bounded admission and reclamation keep usage within capacity.
+ **F. Crash recovery** - persist, simulate a crash, restart, and recover usable cache state.
+ **G. Compatibility rejection** - reuse across incompatible model/tokenizer/runtime identity is proven rejected.
## Build requirements

+ Rust 1.93+ (stable), the standard library only (zero external crates, so the build is fully offline-reproducible).
+ A C toolchain for the MSVC target (bundled with rustup for the default host).
+ Optional: CUDA (12.x or newer) to enable the cuda feature. The CUDA runtime library is loaded dynamically, so no link-time CUDA dependency is required.
## Testing

`cargo test` runs the full suite: 94 unit tests, 10 integration tests, and a real multi-process coordinator+node TCP test. It covers property, concurrency, persistence, corruption, failure, and recovery behavior.
## Limitations and roadmap

+ The accelerator backend validates real CUDA allocation / host-device transfer / free on NVIDIA hardware. HIP, Level Zero, Metal and Vulkan backends are architecturally supported but not yet implemented.
+ Durable objects recover their metadata and blocks; the token sequence used for prefix reuse is re-associated by the caller on first reuse (metadata deliberately does not store the token list). This is a documented boundary.
+ The coordinator persists epoch and object ownership by atomic snapshot; a restarted coordinator advances its epoch and requires workers to re-register, so stale authority is rejected. (See RECOVERY.md.)
+ Networking uses framed TCP on Windows and POSIX; QUIC is a future option.
## License

Apache-2.0. See `LICENSE`.
