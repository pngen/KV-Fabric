# Architecture

KV Fabric is a vertical runtime that applies the principles of FlashTier (bytes), Context Fabric (reusable computational state), Compute Fabric (execution placement), Reclaim Fabric (reclamation) and Checkpoint Fabric (survival) to reusable KV and prefix state. It is independently useful, but its seams are explicit.

## Layering

+ **Contract** (`api`, `cli`): the public facade and the command-line driver.
+ **Authority** (`coordinator`): authority epochs, node registry (with boot identities), canonical ownership, placement, leases, namespace quotas, and a durable snapshot that survives restart.
+ **Policy** (`economy`, `admission`, `reclaim`): a deterministic transfer-vs-recompute planner, bounded admission, and deterministic reclamation.
+ **State** (`metadata`, `prefix`, `block`, `lifecycle`): the KVObject model, the reusable-prefix trie, block management, and the enforced state machines.
+ **Durability** (`persistence`, `storage`, `integrity`): atomic durable object storage with a manifest, host/accelerator/storage tier stores, and SHA-256 + CRC-32C integrity.
+ **Transport** (`protocol`): a bounded, length-prefixed, CRC-validated framed TCP codec.
+ **Execution** (`node`, `migration`, `replica`): the local runtime carrying the object lifecycle and the disciplined migration / replica semantics.

## Concurrency model

The runtime uses interior mutability. Each node and the coordinator guard their collections with `RwLock`/`Mutex`. A hard invariant: **a thread never re-enters the same `RwLock` while holding a guard on it** (read-inside-write or write-inside-read would deadlock). Snapshot operations are therefore performed after releasing the relevant guard. All object metadata and content-digest computations are deterministic; block hashing uses sorted index order.

## Authority and epochs

The coordinator owns an authority epoch. Workers register with a node id and a fresh boot id. A restart of the coordinator advances the epoch and requires re-registration, so a worker valid before the restart no longer holds authority. Object ownership records carry both the holder and the epoch; authority checks compare against the live epoch.

## Integration seams

+ **FlashTier**: KV Fabric delegates physical residency to `storage` host/accelerator/storage tiers; a FlashTier-backed tier can implement the same `TierStore`/`DeviceStore` traits.
+ **Context Fabric**: object metadata carries lineage (parent, children, sequence identity, compatibility identity) suitable for reuse across a Context Fabric.
+ **Compute Fabric**: placement metadata and the planner expose residency decisions for locality-aware placement.
+ **Reclaim Fabric**: object economics (reuse value, reconstruction cost, last-access, durability) are exposed so an external reclaimer can drive decisions.
+ **Checkpoint Fabric**: the durable store and recovery provide a durable recovery path for inference-relevant state.