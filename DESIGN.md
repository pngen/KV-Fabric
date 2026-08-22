# Design

## Object model (`metadata`)

Every `KVObject` has a 128-bit `ObjectId`, a monotonic `Generation`, and explicit metadata covering namespace, canonical compatibility identity, model/tokenizer/engine identities, token range, block list, geometry (head count/dimension, layer range), device compatibility, creation/last-access epochs, access/reuse/ref counts, estimated reconstruction and transfer costs, persistence/integrity/lifecycle/residency states, owner and authority epoch, lineage (parent/children), replicas, retention and priority. Metadata is versioned and canonical-serialized (length-prefixed, tagged fields) so it can be hashed and persisted without ambiguity.

## Compatibility identity (`compat`)

Reuse correctness is the highest priority. The compatibility key is a canonical SHA-256 over model identity, model revision, tokenizer identity/config, engine ABI/version, rope config, attention config, dtype, quant mode, cache layout and multimodal preprocessing. Missing fields are encoded as empty strings so that an absent field never equals a different value. Different namespace or compatibility group cannot share a trie, so cross-tenant or cross-model matches are impossible.

## Reusable prefix structure (`prefix`)

A per-(namespace, compatibility) hashed trie maps token sequences to committed objects. Nodes are immutable once committed; a new branch only adds children. Lookup walks the trie token-by-token, tracking the deepest node with a committed object (longest reusable prefix) and the node that consumes the whole query (exact match). Because commits append and never modify existing nodes, branching conversations and shared system prompts reuse state without cross-branch mutation. Reads take a shared lock; commits take an exclusive lock.

## Blocks (`block`)

State is partitioned into configurable blocks with 128-bit block identities and per-block CRC-32C/SHA-256 anchors. Block groups form objects. Sizing is power-of-two aligned, and capacity is checked with overflow safety.

## Translation economics (`economy`)

The planner is a pure, deterministic function. For a request it computes the candidate actions (reuse in place, restore, fetch remote, promote, recompute, wait, reject) and selects the lowest estimated cost with a fixed tie-break order. It exposes all cost terms and the selected reasoning for telemetry. Reuse in place is always cheapest for a resident object; a transfer is only chosen when it is cheaper than recomputation.

## Admission and reclamation (`admission`, `reclaim`)

Admission considers capacity, expected reuse, reconstruction cost, priority, namespace quota and pressure, and returns an explicit mode (Hot, Cold, PersistOnly, Transient, Decline, Replace). Reclamation is deterministic (low value, then old, then cheap-to-reconstruct) and never touches leased or protected objects.

## Durability (`persistence`, `integrity`)

Persistence is staged as: write blocks atomically (temp + fsync + rename), write metadata atomically, then write the manifest atomically. The manifest is the authoritative index; durability is only claimed after the manifest commit. Every block and object carries a CRC-32C and a SHA-256 content digest; corruption is detected and quarantined.

## Determinism

Policy decisions use sorted block order, deterministic tie-breaking and explicit policy version identifiers. For identical state and inputs, planning is reproducible. No unordered iteration affects a decision.

## Safety invariants

+ No stale authority accepted (epoch + boot id checked).
+ No stale generation accepted; a mutation/number must be strictly monotonic.
+ No replica served at a stale generation.
+ No false compatibility hit.
+ No namespace exceeds its quota.
+ No reclaim while actively leased.
+ No committed object refers to missing blocks.
+ No corrupted object is served.
+ No durable success is reported before the durability boundary is crossed.
+ No impossible residency or lifecycle state (enforced by state machine transition tables).