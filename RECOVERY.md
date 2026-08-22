# Recovery

Crash recovery is a first-class concern. The durable boundary is the manifest; recovery reconstructs exactly what crossed it and never invents what did not.

## Recovery algorithm

On node startup, `DurableStore::recover` walks the manifest and, for each entry, loads the metadata, verifies block presence and re-hashes the blocks in sorted index order, comparing the aggregate content digest to the stored one. Only entries that pass verification are re-admitted into the local registry. Corrupt or incomplete entries are silently excluded (not served).

## Guarantees

After recovery:

+ **No phantom objects** - only manifest entries are considered.
+ **No invented commits** - an object is durable only if its manifest entry committed.
+ **No stale authority** - the coordinator advances its epoch on restart; workers must re-register with a fresh boot id, and object ownership records carry the authority epoch.
+ **No corrupt object accepted** - content digests and CRC-32C must match.
+ **No duplicate ownership** - object ownership is a single canonical record per object.
+ **No generation regression** - generations are monotonic; a migration/mutation must strictly increase.
+ **No invalid replica considered current** - replica validity is generation-gated.

## Recovery authority

Durable existence is separate from execution authority. A recovered object exists durably but is not automatically reusable; it is verified (format, integrity, generation, namespace, compatibility, lifecycle, ownership, epoch) and re-enters prefix service only when its token sequence is re-associated (since the token list is deliberately not stored in metadata).

## Tested crash windows

Recovery is exercised at object creation, block write, persistence, metadata commit, migration, promotion, demotion, coordinator update, lease transition and retirement. Failure-injection and corruption tests verify that corrupt state is detected and never served.