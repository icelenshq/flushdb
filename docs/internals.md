# Storage Engine Internals

This page walks through every layer of the storage engine, from the moment a write arrives to how it eventually lands on S3 and gets read back. Each section builds on the previous one.

---

## 1. Composite Key Encoding

Every data structure in the engine — WAL, memtable, SSTable — operates on a single flat keyspace of composite keys.

```
composite_key = [record_id_bytes] [0x00] [item_key_bytes]
```

The first `0x00` in the key unambiguously marks the boundary between record ID and item key. Record IDs are UTF-8 (no null bytes allowed), so this is safe. Item keys are arbitrary bytes.

**Sort order:** Raw byte comparison gives correct two-level ordering — first by record ID, then by item key within a record. `0x00` sorts before any valid UTF-8 continuation byte, so all items for `"aaa"` sort before all items for `"aab"`.

| Field | Max Length |
|-------|-----------|
| `record_id` | 256 bytes |
| `item_key` | 4,096 bytes |
| `composite_key` | 4,353 bytes |

**Range tombstones** use a synthetic key with a `0xFF` prefix after the separator. This sorts after all valid item keys within the record, keeping tombstone metadata separate from data.

---

## 2. Write-Ahead Log (WAL)

The WAL ensures durability for writes that haven't yet been flushed to S3.

### Segment Architecture

The WAL is a sequence of fixed-size **segments** (default 32 MB) on local disk.

```
wal/partition-{id}/
    segment-000000000001.wal    ← oldest active
    segment-000000000002.wal
    segment-000000000003.wal    ← current append target
```

Segments are deleted whole after a flush confirms data on S3 — no mid-file truncation. Segment numbers are monotonically increasing 12-digit zero-padded integers. Gaps from deleted segments are normal.

### Entry Wire Format

Each entry is length-prefixed and CRC-protected:

```
┌─────────────────────────────────────────────────────┐
│ entry_length: uint32 (little-endian)                │  4 bytes
├─────────────────────────────────────────────────────┤
│ sequence_number: uint64 (little-endian)             │  8 bytes
│ entry_type: uint8 (0=PUT, 1=DELETE, 2=RANGE_DELETE) │  1 byte
│ namespace_len: uint16                               │  2 bytes
│ namespace: bytes                                    │  variable
│ record_id_len: uint16                               │  2 bytes
│ record_id: bytes                                    │  variable
│ item_key_len: uint16                                │  2 bytes
│ item_key: bytes                                     │  variable
│ item_value_len: uint32                              │  4 bytes
│ item_value: bytes                                   │  variable
│ item_metadata_len: uint16                           │  2 bytes
│ item_metadata: bytes                                │  variable
│ idempotency_token: bytes                            │  24 bytes
├─────────────────────────────────────────────────────┤
│ crc32: uint32 (over all preceding bytes)            │  4 bytes
└─────────────────────────────────────────────────────┘
```

**Recovery:** Read `entry_length`, read payload + CRC, validate. CRC failure at segment tail = partial write from crash (safe to discard). CRC failure mid-segment = corruption (halt and alert).

### Group Commit

Individual fsync per write is expensive. The WAL batches writes:

```
  Buffering (in-memory)          Commit              ACK
┌───────────────────────┐   ┌──────────────┐   ┌────────────┐
│ Write 1               │   │              │   │            │
│ Write 2    → buffer   │──►│   fsync()    │──►│  ACK all   │
│ Write 3               │   │              │   │  clients   │
│ Write 4               │   │              │   │            │
└───────────────────────┘   └──────────────┘   └────────────┘
 Trigger: 200μs or 256KB
```

Writes are appended to a buffer and inserted into the memtable immediately but **not ACK'd** until the buffer is fsynced. Every 200μs or 256KB (whichever comes first), the batch is fsynced and all writes in it are ACK'd simultaneously. Under sustained load, hundreds of writes share a single fsync — **5-10x throughput improvement**.

The commit interval is configurable down to 50μs for latency-sensitive namespaces.

Two fsync modes per namespace:

| Mode | Behavior | Durability | Latency |
|------|----------|------------|---------|
| `SYNC` (default) | fsync after every batch | Survives power loss | ~0.5-2ms |
| `BATCH_SYNC` | fsync on 10ms timer | May lose last 10ms | ~50-100μs |

### Dirty Segment Tracking

A segment cannot be deleted until **all** memtable generations that wrote to it have been flushed to S3. Each segment maintains:

```
dirty_map: HashMap<memtable_generation_id, highest_sequence>
```

When generation G is flushed: remove G from all segments' dirty maps. If a segment's map becomes empty, it's safe to delete.

**Low-write partitions** may never fill a memtable, pinning segments indefinitely. Two pressure valves:
- **Age trigger:** Segments pinned > 5 minutes → force-flush referencing memtables.
- **Size trigger:** Total WAL > 512 MB → force-flush the oldest pinned memtable.

**Backpressure:** WAL > 256 MB (4x memtable threshold) → writes stalled with `RESOURCE_EXHAUSTED` until a flush completes.

---

## 3. Memtable

The memtable buffers writes in-memory before they are flushed to SSTables on S3.

### Skip List

```
Level 3:  HEAD ──────────────────────────────────► 47 ────────────────► NIL
Level 2:  HEAD ──────────► 12 ──────────────────► 47 ────────────────► NIL
Level 1:  HEAD ──────────► 12 ────► 25 ──────────► 47 ──► 53 ────────► NIL
Level 0:  HEAD ──► 3 ──► 12 ──► 19 ──► 25 ──► 31 ──► 47 ──► 53 ──► 61 ► NIL
```

O(log n) insert, lookup, and iteration. Max height 12 (supports ~4 billion entries). Probability 1/4 per level.

Since this is shard-per-core, the owning core is the sole writer — inserts are plain pointer writes with no CAS or locking.

Each entry stores:

```rust
MemtableEntry {
    composite_key:    Bytes,            // [record_id][0x00][item_key]
    value:            Bytes,
    metadata:         Bytes,
    idempotency_key:  IdempotencyToken, // dedup during unflushed window
    sequence_number:  u64,              // WAL sequence for ordering
    entry_type:       EntryType,        // PUT | DELETE | RANGE_DELETE
}
```

### Arena Allocator

Each memtable owns a bump allocator backed by 1 MB blocks:

```rust
Arena {
    blocks: Vec<Box<[u8; 1_048_576]>>,
    current_offset: usize,
    total_allocated: usize,
}
```

Thousands of skip list node allocations become pointer bumps. Cache locality improves because nodes are contiguous. Deallocation is O(1) — drop all blocks when the memtable is released after flush. No atomics needed (single-owner).

### Freeze and Swap

When `total_allocated >= 64 MB` or 5 minutes elapse:

1. **Swap** the active memtable pointer with a new empty memtable.
2. The old memtable becomes **frozen** — pushed onto a read-only list, still checked during reads.
3. A background flush task is notified.

If 3 frozen memtables accumulate (flush can't keep up), writes are rejected — 192 MB is the ceiling.

### Range Tombstone Index

A secondary index for range deletes, sorted by `(record_id, start_key)`:

```rust
RangeTombstone {
    record_id: Bytes,
    start_key: Bytes,       // inclusive
    end_key: Bytes,          // exclusive
    sequence_number: u64,
}
```

During point reads, this index determines if a key falls within a range tombstone with a higher sequence number. If so, the entry is dead.

### Idempotency Dedup

Each memtable maintains a `HashSet` of idempotency tokens. On write: check active + frozen sets. If found, skip (already applied). Tokens are flushed into the SSTable's dedup block and expire after 10 minutes.

---

## 4. SSTable Format

SSTables are immutable sorted files on S3. Each one is produced by flushing a frozen memtable (L0) or by compaction (L1+).

### Binary Layout

```
┌──────────────────────────────────────────┐
│ Header                                    │
│   magic: 0x464C4442 ("FLDB")            │
│   version: uint16                         │
│   compression: NONE | SNAPPY | ZSTD       │
│   entry_count: uint64                     │
├──────────────────────────────────────────┤
│ Data Block 0 (4 KB target)                │
│   Entry, Entry, Entry, ...                │
│   CRC32 (uncompressed)                    │
├──────────────────────────────────────────┤
│ Data Block 1...N                          │
├──────────────────────────────────────────┤
│ Dedup Block                               │
│   128-bit hashes of idempotency tokens    │
├──────────────────────────────────────────┤
│ Bloom/Ribbon Filter                       │
│   Over record_id values (NOT item keys)   │
├──────────────────────────────────────────┤
│ Index Block                               │
│   first_key → (block_offset, block_size)  │
├──────────────────────────────────────────┤
│ Footer (80 bytes fixed)                   │
│   Section offsets, entry count,           │
│   min/max key (16B truncated),            │
│   compression, version, CRC, magic        │
└──────────────────────────────────────────┘
```

### Data Blocks

4 KB target, independently compressed (Snappy or ZSTD). Each block contains sorted entries:

```
[record_id_len: varint] [record_id: bytes]
[item_key_len: varint]  [item_key: bytes]
[value_len: varint]     [value: bytes]
[metadata_len: varint]  [metadata: bytes]
[entry_type: u8]
[sequence_number: varint]
```

**Record ID deduplication:** Consecutive entries sharing a record ID store `record_id_len = 0` and omit the bytes. The reader carries forward the last seen value. Saves **30-50%** block space for wide records. Empty record IDs are forbidden at the API layer, making `0` an unambiguous dedup signal. The first entry in every block always includes the full record ID.

Each block is finalized with a CRC32 over uncompressed bytes, then compressed. The index builder records `(first_key, offset, compressed_size)`. Unique record IDs are fed to the bloom filter builder.

### Bloom and Ribbon Filters

Filters index **record IDs**, not full composite keys. One check covers all items in a record.

| SSTable Source | Filter Type | Bits/Key | FPR |
|---------------|------------|----------|-----|
| Memtable flush (L0) | Bloom filter | ~10 | ~1% |
| Compaction (L1+) | Ribbon filter | ~7 | ~1% |

Ribbon filters are 30% smaller at the same FPR. Higher construction cost, but that's a one-time cost at compaction time. Hash function: double-hashing with two MurmurHash3 seeds (`h1 + k * h2`).

Filters cover all entry types (PUT, DELETE, RANGE_DELETE) so a filter positive also covers tombstones.

### Index Block

Sparse index mapping first composite key → block location:

```rust
IndexEntry { first_key, block_offset: u64, block_size: u32, uncompressed_size: u32 }
```

Binary search locates the single block that could contain any target key.

### Footer

Fixed 80 bytes at the end of every SSTable:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | `bloom_filter_offset` |
| 8 | 4 | `bloom_filter_size` |
| 12 | 8 | `index_block_offset` |
| 20 | 4 | `index_block_size` |
| 24 | 8 | `entry_count` |
| 32 | 16 | `min_key` (truncated) |
| 48 | 16 | `max_key` (truncated) |
| 64 | 1 | `compression_type` |
| 66 | 2 | `format_version` |
| 68 | 4 | `crc32` |
| 72 | 4 | `magic` (0x464C4442) |

`min_key`/`max_key` truncation can produce false positives but never false negatives — a cheap pre-filter before loading bloom filters.

### S3 Access Pattern

A cold read (nothing cached) requires up to 4 byte-range GETs:

```
Step 1: GET bytes=-80               → Footer (find offsets)
Step 2: GET bytes={bloom_range}     → Bloom filter (check record_id)
Step 3: GET bytes={index_range}     → Index block (binary search for block)
Step 4: GET bytes={block_range}     → Data block (decompress, scan for key)
```

For small SSTables, footer + bloom + index are contiguous — a single ~200 KB GET fetches all metadata.

### Upload Strategy

| SSTable Size | Method |
|-------------|--------|
| < 16 MB | Single `PutObject` |
| ≥ 16 MB | Streaming multipart upload (16 MB parts, double-buffered) |

Peak memory: O(32 MB) regardless of SSTable size. Incomplete uploads cleaned up by S3 lifecycle rule (24 hours).

---

## 5. Manifest

The manifest defines which SSTables are live at each level. It is the **commit point** for all state changes.

### Structure

```json
{
  "format_version": 1,
  "manifest_id": "00000000000000000042",
  "writer_epoch": 7,
  "compactor_epoch": 3,
  "namespace": "my-namespace",
  "last_flushed_sequence": 458923,
  "levels": {
    "L0": [{ "id", "size_bytes", "entry_count", "min_key", "max_key",
             "bloom_filter_offset", "bloom_filter_size",
             "sequence_range", "record_id_count", ... }],
    "L1": [...], "L2": [], "L3": []
  },
  "tombstone_compaction_watermarks": { "L1": ..., "L2": ... },
  "previous_manifest_id": "00000000000000000041"
}
```

**Manifest IDs** are 20-digit zero-padded integers. The highest ID is always the current manifest — S3 `ListObjectsV2` gives you the latest. Two writers computing the same next ID race: one wins, the other retries.

**S3 path:** `s3://{bucket}/{hash % 128}/flushdb/{namespace}/manifests/{id}`

### CAS Update Protocol

Every flush, compaction, and GC operation updates the manifest atomically:

```
1. Read current manifest (ID = N)
2. Validate epoch (zombie check)
3. Compute new manifest (N+1)
4. PUT to S3 with If-None-Match: *
5. SUCCESS → done
   412 PreconditionFailed → re-read, recompute, retry
```

S3 conditional writes reject a PUT if the key already exists. No external coordination needed.

### Concurrent Flush + Compaction

```
Time ─────────────────────────────────────────────────►

Flusher                          Compactor
   │                                │
   │  Read manifest v5              │  Read manifest v5
   │  L0: [A,B,C,D]                │  L0: [A,B,C,D]
   │                                │
   │  Build & upload SSTable E      │  Merge [A,B,C,D] → L1 [F,G]
   │                                │
   │  CAS: v6 (add E to L0)        │
   │  → SUCCESS                     │
   │                                │
   │                                │  CAS: v7 (expected prev=v5)
   │                                │  → FAIL (412)
   │                                │
   │                                │  Re-read v6. Inputs [A,B,C,D] still valid.
   │                                │  Recompute, CAS: v7 → SUCCESS
   │                                │  L0=[E], L1=[F,G]
```

### Epoch-Based Fencing

**Problem:** Node A begins flushing, its lease expires, Node B takes over, A's flush completes and tries to update the manifest.

**Solution:** Each manifest carries `writer_epoch` and `compactor_epoch`. On lease acquisition, the new owner bumps the epoch. The old node's writes are rejected:

```
Node A (epoch=5)                  Node B
   │                                │
   │  Begins flush...               │
   │  ── Lease expires ──           │
   │                                │  Takes lease, CAS manifest: epoch=6
   │                                │  Begins serving.
   │  Flush done.                   │
   │  Read manifest... epoch=6      │
   │  6 > 5 → ZOMBIE. HALT.        │
```

### Snapshots and Pruning

Every 100 versions, a self-contained **snapshot manifest** is written. On startup, read the latest snapshot and apply subsequent deltas. Versions older than the second-most-recent snapshot are eligible for deletion.

A **pointer file** (`manifest.json`) provides fast lookup of the current manifest ID. Falls back to `ListObjectsV2` if stale.

---

## 6. Flush Pipeline

The complete sequence from frozen memtable to durable S3 state:

```
1. TRIGGER      memtable ≥ 64 MB or 5 minutes elapsed
2. FREEZE       Pointer swap: active → frozen, new empty → active
3. BUILD        Iterate frozen memtable in sorted order → SSTable (blocks, bloom, index, footer)
4. UPLOAD       PutObject or streaming multipart to S3 L0 path
5. MANIFEST     CAS: add SSTable to L0, set last_flushed_sequence, validate epoch
6. WAL CLEANUP  Delete segments with empty dirty maps
7. RELEASE      Drop frozen memtable arena
8. COMPACT?     If L0 > 4 files, schedule L0 → L1 compaction
```

### Failure Modes

| Crash Point | S3 State | Recovery |
|-------------|----------|----------|
| During build | Unchanged | WAL replay rebuilds memtable |
| During upload | Incomplete multipart | S3 lifecycle aborts it. WAL replay. |
| During CAS | SSTable on S3, not in manifest | Orphan GC deletes it. WAL replay. |
| After CAS, before WAL cleanup | Manifest updated, WAL intact | WAL replay is idempotent |

**The WAL is the safety net. The manifest CAS is the commit point.**

---

## 7. Compaction

Compaction merges SSTables to reduce read amplification and reclaim tombstone space.

### Leveled Strategy (10x size ratio)

| Level | Max | Trigger | Action |
|-------|-----|---------|--------|
| L0 | 4 files | Overflow | All L0 → overlapping L1 |
| L1 | 256 MB | Overflow | One L1 file → overlapping L2 |
| L2 | 2.56 GB | Overflow | One L2 file → overlapping L3 |
| L3 | 25.6 GB | Timer (1h) | Drop expired tombstones |

### Write Stalling

| L0 Count | Effect |
|----------|--------|
| ≤ 4 | Normal. Compaction triggered. |
| 5–8 | Throttled. Each write sleeps `(count - 4) × 1ms`. |
| 9–12 | Stalled. `RESOURCE_EXHAUSTED` with `retry-after`. |

### Merge Process

```
1. Open iterators on all input SSTables
   Index blocks + bloom filters loaded into DRAM

2. Merge-sort by composite key:
   - Duplicate keys → keep highest sequence_number
   - Tombstones at bottom level with expired TTL → drop both
   - Range tombstones → propagate if they still cover lower-level keys

3. Build output fragments (~1 GB each for L1+)
   Each fragment: own bloom filter, index, footer
   Upload each fragment as completed

4. CAS manifest: add outputs, remove inputs, validate compactor_epoch

5. Deferred deletion of old SSTables via reference tracking
```

### SSTable Run Fragments

Compaction output is a **run** of smaller fragments, not a monolith:

```
sstables/L1/run-{id}/frag-0000.sst
sstables/L1/run-{id}/frag-0001.sst
sstables/L1/run-{id}/frag-0002.sst
```

Max temporary space = `2 × fragment_size` instead of `2 × total_run_size`.

**Trivial move:** If a fragment's key range doesn't overlap the next level, it's "moved" by manifest-only update — zero S3 I/O. For time-ordered keys, most compactions are trivial moves.

### Tombstone Lifecycle

```
Write: DeleteItems → WAL → Memtable → L0
  │
  ▼
L1 compaction: Covered PUTs dropped. Tombstone survives — older PUTs may exist below.
  │
  ▼
L2 compaction: Same.
  │
  ▼
L3 (bottom level): Tombstone TTL checked. Expired (7d default + random 0-24h jitter) → dropped.
```

Range tombstones use a watermark protocol: only eligible for deletion when all levels below have been compacted past the tombstone's creation timestamp.

---

## 8. Three-Tier Cache

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                       DRAM Cache                         │
│  ┌─────────────┐     ┌───────────────────────────────┐  │
│  │ Window (1%) │     │          Main (99%)            │  │
│  │ LRU, admits │────►│  Protected (80%) + Probation   │  │
│  │ all new     │     │  Admitted only if freq >       │  │
│  │ entries     │     │  eviction candidate's freq     │  │
│  └─────────────┘     └───────────────────────────────┘  │
│  Frequency Sketch (Count-Min Sketch, halved periodically)│
└────────────────────────────┬────────────────────────────┘
                             │ evict
                    ┌────────▼─────────┐
                    │   NVMe (<5ms)    │ same frequency check for admission
                    └────────┬─────────┘
                             │ miss
                    ┌────────▼─────────┐
                    │   S3 (50-200ms)  │ source of truth
                    └──────────────────┘
```

**Cache key:** `(sstable_id, block_offset)` — one entry per 4 KB data block.

### Key Behaviors

**Pinned metadata:** Index blocks and bloom filters for live SSTables are pinned in DRAM permanently (<1% of SSTable size, accessed on every read). Swapped when compaction replaces SSTables.

**Continuity tracking:** After a full record scan (`match_all`) caches all blocks, the tracker records the range as complete. A subsequent point read for a missing key skips S3 entirely. Invalidated when compaction produces a new manifest for the affected range.

**Coalescing fetches:** Multiple adjacent block requests merge into a single S3 byte-range GET:

```
Without:  GET 4096-8191, GET 8192-12287, GET 12288-16383  (3 requests)
With:     GET 4096-16383                                    (1 request)
```

**GET budget:** Max 8 S3 GETs per read (configurable). Excess deferred with staleness flag.

**Compaction-aware eviction:** When compaction invalidates SSTables, their blocks are evicted asynchronously. Evictions to NVMe are skipped for compacted SSTables.

**Adaptive pagination:** First page estimates item count from cached avg item size. Actual avg stored in page token for subsequent pages. Server-side avg cache continuously updated per namespace.

---

## 9. S3 Integration

### Object Layout

```
s3://{bucket}/{hash(id) % 128}/flushdb/{namespace}/
  ├── manifests/          00000000000000000001.json, ...
  ├── sstables/
  │   ├── L0/             {ulid}.sst
  │   ├── L1/             {run-id}/frag-0000.sst, ...
  │   ├── L2/             ...
  │   └── L3/             ...
  ├── blobs/              {blob-id}.blob
  ├── chunks/             {chunk-group-id}/chunk-0000, ...
  └── leases/             partition-{id}/lease-{version}.json
```

**128-way prefix sharding** avoids S3 partition throttling. SSTable IDs are ULIDs (time-sortable, hash-distributable).

### Conditional Writes

S3 `If-None-Match: *` (available since August 2024) is the coordination primitive. Used for manifest CAS and lease acquisition. No external coordination service needed.

### Distributed Leases

Partition ownership via versioned S3 lease keys. Highest lexicographic version = current lease. 30-second TTL, 10-second renewal interval (three chances before expiry).

Acquisition: list lease keys → PUT next version with `If-None-Match: *` → on 412, re-list and retry.

### Large Value Handling

| Value Size | Strategy |
|-----------|----------|
| < 32 KB | Inline in SSTable |
| 32 KB – 4 MB | **Value separation**: blob object on S3, SSTable holds `(blob_id, offset, size)` pointer. Compaction rewrites only pointers, not data. Write amplification drops from 10-30x to ~1x. |
| ≥ 4 MB | **Chunked**: split into 4 MB S3 objects, fetched in parallel (up to 32 concurrent GETs). |

The separation threshold is adaptive — adjusted based on observed write amplification and read patterns.

### Garbage Collection

| What | When Garbage | How |
|------|-------------|-----|
| SSTables | Compaction removes from manifest | Reference tracking → deferred DELETE after no active readers hold the old manifest version |
| Orphans | Crash during flush | List objects not in manifest, delete after 1 hour |
| Manifests | After 100+ newer versions | Delete beyond 2-snapshot retention |
| Blobs | Live ratio < 50% | Rewrite live entries, pointer migration during compaction |
| Chunks | Referencing entry removed | Scan references, delete orphans after 4h grace |
| Multipart uploads | Crash during upload | S3 lifecycle rule (24h) |

SSTable GC protocol: each reader holds a manifest version reference. GC only deletes SSTables removed in versions older than the minimum active reader version. 30-minute timeout for stuck readers.

---

## 10. End-to-End Data Flow

### Write

```
Client PutItems(namespace, record_id, items, token)
  │
  ├─► Route to partition owner
  ├─► Append WAL entry (buffered, fsync via group commit)
  ├─► Insert into memtable (composite key sort order)
  ├─► ACK to client with OrderedKey version
  │
  └─► Background:
        ├─► Freeze memtable when full → flush to SSTable on S3
        ├─► CAS manifest → truncate WAL → release memtable
        └─► Compaction merges L0 → L1 → L2 → L3
```

### Read

```
Client GetItems(namespace, record_id, predicate, selection)
  │
  ├─► Route to partition owner
  ├─► Merge-read across layers:
  │     Active memtable
  │     Frozen memtables
  │     L0 SSTables (bloom check, parallel GETs)
  │     L1-L3 SSTables (bloom check, at most 1 per level)
  │
  ├─► For each layer: seek to (record_id, start_key), scan forward
  ├─► Merge-sort by item_key, apply tombstone filtering
  ├─► Accumulate until byte budget exhausted
  └─► Return items + page_token
```

### Recovery

```
Node startup:
  1. Read latest manifest from S3 → restore SSTable level state
  2. Replay WAL entries with sequence > last_flushed_sequence
  3. Rebuild memtable + dedup set from replayed entries
  4. Resume normal operation
```

Zero data loss for ACK'd writes, assuming the WAL survived the crash.
