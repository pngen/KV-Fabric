# Persistence

Persistence uses a versioned on-disk format with atomic commits and an authoritative manifest.

## Layout

+ `<root>/blocks/<block_hex>.blk` - block payloads, written atomically.
+ `<root>/meta/<object_hex>.meta` - canonical KVObject metadata (KvMetadata encode), written atomically.
+ `<root>/manifest` - the authoritative index of committed objects (object id, generation, content digest, block count), written atomically.

## Atomic commit

`atomic_write` writes to a temp file, fsyncs it, then renames it over the target (atomic on the same volume on Windows and POSIX), then best-effort fsyncs the directory. An object is committed in the order: blocks, then metadata, then manifest. A crash between steps leaves no manifest entry referencing missing blocks, so recovery never invents a commit.

## Integrity

Each object records a SHA-256 content digest over its block SHA-256 digests, plus a per-block CRC-32C and SHA-256. On recovery the blocks are re-hashed in sorted index order and the aggregate content digest is compared; any mismatch marks the object corrupt and it is never re-admitted.

## Durable success

No durable success is reported before the manifest is committed. `store_object` only returns Ok after all three atomic writes succeed.

## Coordinator snapshot

The coordinator persists its epoch and object ownership to `<dir>/coordinator.snapshot` atomically. On restart it loads the snapshot, then advances the epoch so that any authority from before the restart is invalid.