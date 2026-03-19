# Storage Engine

flushdb uses an LSM-tree storage engine with S3 as its durable backing store. Data flows through an in-memory write buffer (memtable), gets flushed as sorted immutable files (SSTables) to S3, and is periodically reorganized by a background compaction process. This page covers the on-disk formats, construction algorithms, and lifecycle of every persistent object the engine produces.

---

## SSTable Format

SSTables are the primary persistent data structure. Each SSTable is a custom binary file containing entries sorted by composite key, which means all items belonging to the same record are stored contiguously.

```
+-------------------------------------------+
| Header                                    |
|   magic: 0x464C4442 ("FLDB")             |
|   version: uint16                         |
|   compression: NONE | SNAPPY | ZSTD       |
|   entry_count: uint64                     |
+-------------------------------------------+
| Data Block 0  (4 KB default, compressed)  |
|   [entry] [entry] [entry] ...             |
|   [CRC32]                                 |
+-------------------------------------------+
| Data Block 1                              |
|   [entry] [entry] ...                     |
|   [CRC32]                                 |
+-------------------------------------------+
| ...                                       |
+-------------------------------------------+
| Dedup Block                               |
|   Compact hash set of idempotency token   |
|   hashes (128-bit per token)              |
+-------------------------------------------+
| Bloom/Ribbon Filter Section               |
|   Filter over record_id values            |
|   (not item keys)                         |
|   Partitioned by key range                |
+-------------------------------------------+
| Index Block                               |
|   block_0_first_key -> offset, length     |
|   block_1_first_key -> offset, length     |
|   ...                                     |
+-------------------------------------------+
| Footer (80 bytes)                         |
|   bloom_filter_offset       (8 bytes)     |
|   bloom_filter_size         (4 bytes)     |
|   index_block_offset        (8 bytes)     |
|   index_block_size          (4 bytes)     |
|   entry_count               (8 bytes)     |
|   min_key (16-byte truncated)(16 bytes)   |
|   max_key (16-byte truncated)(16 bytes)   |
|   compression_type          (1 byte)      |
|   padding                   (1 byte)      |
|   format_version            (2 bytes)     |
|   crc32                     (4 bytes)     |
|   magic: 0x464C4442         (4 bytes)     |
|   reserved                  (4 bytes)     |
+-------------------------------------------+
```

**Key properties:**

- **Sorted by composite key.** Binary search through the sparse index locates any key. All items for a single record are contiguous, making range scans within a record a sequential read.
- **Bloom filter eliminates SSTables without the target record before I/O.** A read that misses the bloom filter avoids fetching any data blocks from that SSTable entirely.
- **S3 byte-range reads for individual blocks.** The index block maps keys to byte offsets, so the engine can issue a targeted HTTP Range request for just the 4 KB block it needs rather than downloading the full file.
- **Per-block compression for random access.** Each data block is independently compressed (Snappy or ZSTD), so decompressing one block does not require reading any other block.

The footer's `min_key` and `max_key` are truncated to 16 bytes. This provides a cheap pre-filter: if the target key falls entirely outside the truncated range, the SSTable can be skipped without loading its bloom filter. Truncation can produce false positives but never false negatives.

---

## Block Building

Data blocks are constructed incrementally as the engine iterates over a frozen memtable in sorted composite key order.

Each block accumulates entries until it reaches the target size (4 KB by default). Within a block, consecutive entries that share the same record ID take advantage of deduplication: the second and subsequent entries for the same record store a record ID length of zero and omit the record ID bytes entirely. The reader carries forward the last seen record ID. For wide records (many items per record), this reduces block size by 30-50%.

The first entry in every block always includes the full record ID, ensuring that each block is independently decodable.

When a block reaches its target size, finalization proceeds:

1. Compute CRC32 over the uncompressed block contents and append it
2. Compress the block (Snappy or ZSTD)
3. Record an index entry: the block's first composite key, its byte offset, and its compressed and uncompressed sizes
4. Collect all record IDs seen in the block into the bloom filter builder
5. Reset the block builder for the next block

---

## Bloom and Ribbon Filters

The filter section enables the engine to skip SSTables that do not contain a target record without reading any data blocks.

**What gets indexed.** The filter indexes record IDs extracted from all entry types: PUTs, DELETEs, and range tombstones alike. Indexing tombstones is critical -- a bloom filter positive must also cover cases where the record exists only as a deletion marker.

**Sizing.** At a 1% false positive rate with 10 bits per key, a 64 MB memtable containing approximately 100,000 unique record IDs produces a filter of roughly 125 KB. The hash function uses double-hashing with two independent MurmurHash3 seeds, where the k-th hash is computed as `h1 + k * h2`.

**Two filter types by level:**

| SSTable Origin | Filter Type | Bits/Key | Rationale |
|----------------|-------------|----------|-----------|
| L0 (flush) | Standard bloom filter | ~10 | Lower construction cost, minimizes flush latency |
| L1+ (compaction) | Ribbon filter | ~7 | Same 1% FPR at 30% smaller size; higher construction cost acceptable during compaction |

Ribbon filters require more temporary memory during construction (~230 bits/key vs ~75 for bloom) but this is a one-time cost paid during compaction, which runs as a background task.

**Partitioned filters.** For large SSTables, the filter is split into multiple small filter blocks organized by key range. Each filter partition fits in a single DRAM cache line. When reading from S3, only the relevant filter partition needs to be fetched via a byte-range request. The index block includes a secondary filter index that maps key range prefixes to filter block offsets.

---

## Large-Record Optimizations

Records with a high item count (exceeding 10,000 items by default) present a challenge: the record-level bloom filter confirms the record exists in an SSTable, but the engine still needs to locate the specific item among potentially thousands of data blocks.

Two mechanisms address this:

**Per-block min/max key fences.** Each data block stores the minimum and maximum composite key it contains. After the bloom filter identifies candidate SSTables, the engine checks these key fences to skip blocks whose key range does not overlap with the target item key. Since index blocks are cached in DRAM, this check is effectively free.

**Prefix bloom filters.** When compaction detects a record exceeding the item count threshold, it builds an additional bloom filter keyed on the first 4 bytes of the item key combined with the record ID. This narrows the candidate block set for point lookups from O(blocks_per_record) to O(1) expected, at a cost of roughly 4 additional bits per item key.

---

## S3 Upload Strategy

SSTables must be uploaded to S3 atomically -- a partially visible SSTable would corrupt reads.

```
SSTable < 16 MB:
  Single PutObject request
  Atomically visible on success

SSTable >= 16 MB:
  Phase 1: Initiate multipart upload (receive upload_id)
  Phase 2: Stream parts as they are built

    [Build Part 1] ---> [Upload Part 1]       16 MB
    [Build Part 2] ---> [Upload Part 2]       16 MB
          ^                    ^
          |   double buffer    |
          +--------------------+
          (build N+1 while uploading N)

  Phase 3: Complete multipart upload (SSTable atomically visible)
```

The double-buffering strategy means peak memory usage is O(32 MB) regardless of how large the SSTable grows. One buffer is being filled by the block builder while the other is being uploaded.

**Failure handling:**

- Individual part uploads are independently retryable
- If the process crashes mid-upload, the incomplete multipart upload is never completed, so no partial SSTable becomes visible to readers
- An S3 lifecycle rule aborts incomplete multipart uploads after 24 hours, preventing storage leaks

---

## Manifest

The manifest is the single source of truth for what data exists in the system. It is a versioned JSON file stored in S3 that records which SSTables are live at each level, the last flushed WAL sequence number, and the epochs that fence zombie writers and compactors.

### Contents

A manifest contains:

- **Writer epoch and compactor epoch** -- monotonically increasing integers used for zombie fencing
- **Last flushed sequence** -- the highest WAL sequence number that has been durably flushed to an SSTable on S3
- **SSTable metadata per level (L0 through L3)** -- ID, size, entry count, key range, bloom filter offset, sequence range for each SSTable
- **Blob files** -- metadata for separated large values including live/dead byte ratios
- **Tombstone compaction watermarks** -- timestamps tracking tombstone propagation through levels

### ID Scheme

Manifest IDs are zero-padded 20-digit integers:

```
00000000000000000001
00000000000000000002
...
00000000000000000042
```

Lexicographic ordering equals version ordering. The "current" manifest is always the one with the highest ID. This eliminates the need for a separate atomic pointer update -- writing the new manifest file IS the update.

### Finding the Current Manifest

Two mechanisms, used in sequence:

1. **Pointer file (fast path).** A well-known file at a fixed S3 key contains the ID of the current manifest. Updated best-effort after each successful manifest write. Used for steady-state reads.
2. **S3 listing (fallback).** List all manifest objects and pick the highest ID. Used on startup and when the pointer appears stale (detected by epoch mismatch).

### Update Protocol

Every operation that changes the set of live SSTables -- flush, compaction, garbage collection -- must update the manifest atomically. The protocol uses S3 conditional writes (`If-None-Match: *`) for optimistic concurrency control:

```
Step 1: Read current manifest (version N)

Step 2: Validate epoch
        If this is a flush and the manifest's writer_epoch
        exceeds the local epoch: this node is a zombie. Halt.
        Same check for compactor_epoch on compaction operations.

Step 3: Compute new manifest state
        Apply changes (add/remove SSTables, update sequence)
        Assign new_manifest_id = N + 1

Step 4: Write to S3 with If-None-Match: *
        This succeeds only if no file with ID N+1 exists yet

Step 5: Handle result
        Success  -> update pointer file, propagate via gossip
        HTTP 412 -> another writer won the race; re-read, revalidate, retry
        Other    -> retry with exponential backoff
```

Two concurrent writers computing the same next ID will race. Exactly one succeeds; the other receives HTTP 412 (Precondition Failed) and retries with the updated state.

### Concurrent Operation Handling

Flush and compaction can race against each other. A typical scenario:

```
Flusher                            Compactor
   |                                  |
   |  Reads manifest v5               |  Reads manifest v5
   |                                  |
   |  Builds new SSTable E            |  Merges L0 SSTables [A,B,C,D]
   |  Writes v6 (adds E to L0)       |  ... still merging ...
   |  CAS succeeds                    |
   |                                  |
   |                                  |  Finishes merge, writes v7
   |                                  |  CAS fails (v6 already exists)
   |                                  |
   |                                  |  Re-reads v6
   |                                  |  Input SSTables [A,B,C,D] still present
   |                                  |  Recomputes: remove [A,B,C,D], add output
   |                                  |  Retries CAS for v7
   |                                  |  CAS succeeds
```

The loser re-reads the current manifest and checks whether its operation is still valid (input SSTables not already consumed). If valid, it recomputes the new state and retries.

### Snapshots and Pruning

**Manifest snapshots.** Every 100 versions, the system writes a self-contained snapshot manifest that includes the full materialized state rather than just a delta. On startup or failover, the node loads only the latest snapshot and replays subsequent non-snapshot manifests on top of it.

**Old manifest pruning.** The two most recent snapshots are retained for rollback capability. Versions older than the second-most-recent snapshot are eligible for deletion. A background task deletes them in batches of 50 to avoid S3 DELETE throttling.

---

## Compaction

Compaction reorganizes SSTables across levels to bound read amplification and reclaim space from obsolete entries. flushdb uses leveled compaction with four levels.

### Triggers

| Level | Trigger Condition | Action |
|-------|-------------------|--------|
| L0 | More than 4 files | Compact all L0 files into the overlapping key range in L1 |
| L1 | Total size exceeds 256 MB | Pick one L1 SSTable, compact with overlapping L2 range |
| L2 | Total size exceeds 2.56 GB | Pick one L2 SSTable, compact with overlapping L3 range |
| Tombstone | Periodic timer (1 hour) | Compact bottom-level SSTables containing expired tombstones |

The size ratio between levels is 10x. L0 triggers at 4 files (roughly 256 MB total). L1 caps at 256 MB, L2 at 2.56 GB, L3 at 25.6 GB. Worst-case write amplification is approximately 10x per level.

### Write Stalling

When L0 accumulates too many files, the engine applies backpressure to writes:

```
L0 file count:

  0 ---- 4 --------- 8 ----------- 12
  |      |           |              |
  |  normal writes   |  throttled   |  fully stalled
  |      |           |              |
  |   soft limit:    | slow limit:  | hard limit:
  |   compaction     | each write   | writes rejected
  |   triggered      | delayed by   | with retry-after
  |                  | (count-4)*1ms| hint
```

This prevents unbounded L0 growth during sustained write bursts that outpace compaction throughput.

### SSTable Run Fragments

Instead of producing monolithic output files, compaction builds sequences of smaller non-overlapping fragments (approximately 1 GB each for L1+):

```
L1/
  run-ABCDEF/
    frag-0000.sst    (up to ~1 GB)
    frag-0001.sst
    frag-0002.sst
```

A run is logically one SSTable for read purposes. The fragment approach provides two advantages:

- **Incremental space reclamation.** Input fragments can be deleted as soon as their key range is fully written to output. Maximum temporary disk space is 2x the fragment size rather than 2x the total run size.
- **Trivial moves.** When a fragment's key range does not overlap with any fragment in the next level, the fragment is "moved" by a manifest-only update with zero S3 I/O. For time-ordered key patterns, the majority of compactions are trivial moves.

### Merge Process

```
1. Open iterators on all input SSTables
   Fetch index blocks and bloom filters into DRAM
   Data blocks fetched on demand

2. Merge-sort by composite key
   For duplicate keys: keep the entry with highest sequence number
   Point tombstones: if TTL expired AND at bottom level, drop both
     tombstone and covered entry
   Range tombstones: propagate to output if they still cover keys
     in lower levels

3. Build output SSTable fragments
   Split at fragment size boundaries
   Each fragment receives its own bloom filter, index, and footer
   Upload each fragment to S3 as it completes

4. Update manifest via CAS
   Add new run to target level
   Remove input SSTables from source levels
   Validate compactor epoch

5. Deferred deletion of old SSTables
   Old SSTable objects deleted only after all active readers
   have moved past the manifest version that removed them
```

### Tombstone Lifecycle

Tombstones cannot be dropped as soon as they are written. They must propagate through levels to ensure they suppress older versions of the deleted data that may still exist at deeper levels.

```
Write:   Client deletes item
         Tombstone written to WAL, then memtable, then flushed to L0
            |
            v
L0 -> L1:  Tombstone merged into L1. Covered PUTs in L1 dropped.
           Tombstone survives -- older PUTs may exist in L2/L3.
            |
            v
L1 -> L2:  Same -- tombstone propagated, covered entries dropped.
            |
            v
L2 -> L3:  Bottom level. Tombstone's TTL checked.
           (bottom)    Expired: tombstone finally dropped.
                       Not expired: tombstone persists until next
                       compaction pass.
```

Point tombstones carry a default TTL of 7 days. A per-tombstone random jitter of 0-24 hours (seeded deterministically from the composite key hash) prevents compaction storms where many tombstones expire simultaneously.

Range tombstones use a watermark protocol: each level tracks a timestamp such that all range tombstones created before that timestamp have been fully propagated through that level. A range tombstone is eligible for deletion at level N only when every level deeper than N has a watermark at or beyond the tombstone's creation time.

---

## Garbage Collection

Six categories of objects require garbage collection:

| Object | When It Becomes Garbage | Collection Method |
|--------|------------------------|-------------------|
| Old SSTables | After compaction removes them from the manifest | Reference tracking: delete only after all active readers have moved past the manifest version that removed the SSTable |
| Orphaned SSTables | Crash during flush (SSTable uploaded but manifest never updated) | Periodic S3 listing compared against manifest; delete objects not in the manifest and older than 1 hour |
| Old manifests | After newer manifest snapshots exist | Delete versions older than the second-most-recent snapshot, in batches of 50 |
| Blob files | After live entry ratio drops below threshold | Rewrite with only live entries, then delete the old file after pointer migration completes |
| Chunk groups | After the referencing SSTable entry is removed | Scan chunk groups, check references against manifest, delete orphans after 4-hour grace period |
| Incomplete multipart uploads | Crash during SSTable upload | S3 lifecycle rule automatically aborts after 24 hours |

The SSTable GC protocol deserves additional detail. Each active reader holds a reference to the manifest version it started with. After compaction commits a new manifest that removes input SSTables, those SSTable IDs enter a GC queue tagged with the manifest version that removed them. A background worker periodically checks the minimum manifest version still referenced by any active reader. Queued SSTables whose removal version is below this minimum are safe to delete. A 30-minute timeout forces deletion for stuck readers, with a pre-deletion notification that allows active readers to checkpoint.

---

## Large Value Handling

Values span a wide size range. Storing a 10 MB value inline in an SSTable would cause severe write amplification during compaction (the value gets rewritten at every level). A three-tier strategy addresses this:

| Value Size | Strategy | Storage Location |
|------------|----------|-----------------|
| < 32 KB (adaptive) | Inline | Inside the SSTable data block alongside the key |
| 32 KB -- 4 MB | Separated | Dedicated blob object on S3; SSTable stores a pointer |
| >= 4 MB | Chunked | Split into 4 MB parts as separate S3 objects |

### Inline Values

Small values are stored directly in SSTable data blocks. During compaction, these values are rewritten along with their keys. Write amplification is acceptable because the values are small.

### Separated Values (Blob Objects)

Values in the 32 KB to 4 MB range are written to dedicated blob files on S3. The SSTable stores only a compact pointer (blob file ID, byte offset, size, CRC32) in place of the actual value. During compaction, the engine rewrites only the keys and pointers, never touching the blob data. This reduces write amplification from 10-30x to approximately 1x for separated values.

Blob files accumulate dead entries as compaction removes or overwrites the SSTable entries that reference them. When the dead entry ratio exceeds 50%, a background GC process rewrites the blob file with only live entries and updates the SSTable pointers during the next compaction pass.

### Chunked Values

Values at or above 4 MB are split into 4 MB chunks stored as separate S3 objects. The SSTable stores chunk metadata (group ID, chunk count, total size). On read, all chunks are fetched in parallel with up to 32 concurrent GET requests, so effective read latency is approximately one S3 round-trip regardless of value size.

### Adaptive Separation Threshold

The 32 KB default threshold is not fixed. The engine tracks write amplification over a sliding 1-hour window and adjusts:

- If write amplification exceeds 10x and average value size exceeds 8 KB: lower the threshold (minimum 8 KB) to separate more values
- If write amplification is below 3x and more than 20% of reads chase blob pointers: raise the threshold (maximum 64 KB) to inline more values

Changes require 10 minutes of persistence and move in 8 KB steps to prevent oscillation. Operators can override with a fixed value per namespace.

Reads for separated and chunked values issue parallel S3 GETs from a thread pool (up to 32 concurrent requests), keeping read latency close to a single round-trip even for multi-megabyte values.
