# flushdb — Complete System Design

This document is the authoritative design reference for flushdb: a distributed key-value database that uses S3 as its durable source of truth while maintaining fast local writes through an LSM-tree architecture. It covers the data model, storage engine internals, cluster coordination, API, multi-tenancy, CDC, and operational concerns.

---

## 1. Overview

flushdb draws from Cassandra's storage model and Netflix's KV Data Abstraction Layer patterns to provide a system where data durability is delegated to object storage, and the coordination layer exists only to protect in-flight writes that haven't yet reached S3.

### 1.1 Problem Statement

Traditional distributed KV stores (Cassandra, DynamoDB) couple storage durability with the database cluster itself — replication, disk management, and failure recovery are all internal concerns. This creates operational overhead: disk provisioning, replica repair, anti-entropy, and complex multi-node consistency protocols.

S3 offers 11 nines of durability, strong read-after-write consistency, and effectively unlimited capacity — but it has high latency (~50ms per request) and no support for point writes. flushdb bridges this gap: fast local writes with WAL-backed durability, background flushes to S3 for permanent storage, and a thin coordination layer that only protects the unflushed window.

### 1.2 Target Users

- Teams that need a KV store with S3-tier durability without managing disk replication
- Applications with write-heavy workloads that can tolerate eventual flush to object storage
- Multi-tenant platforms that need namespace-level isolation with per-tenant partitioning strategies
- Systems that need CDC event streams from their KV layer without running a separate change tracking system

---

## 2. Data Model

### 2.1 Two-Level Map

The fundamental data structure in flushdb is a two-level map:

```
HashMap<String, SortedMap<Bytes, Bytes>>
```

The first level is a **Record**, identified by a hashed string ID (the record ID, which is also the partition key input). The second level is a **sorted map of Items** — each Item is a key-value pair of raw bytes, sorted by key in ascending byte order within the record.

```
Record ID ──► SortedMap
                ├── item_key_0 → item_value_0
                ├── item_key_1 → item_value_1
                ├── item_key_2 → item_value_2
                └── ...
```

Each Item carries optional metadata alongside its value:

```
Item {
  key:      Bytes        // sort key within the record
  value:    Bytes        // payload
  metadata: Bytes        // optional (content type, schema version, etc.)
  chunk:    Integer      // chunk index for large values (0 for non-chunked)
}
```

### 2.2 Supported Data Patterns

The two-level sorted map unifies multiple data patterns into one primitive:

| Pattern | Record ID | Item Key | Item Value | Example |
|---------|-----------|----------|------------|---------|
| **Simple KV** | entity ID | empty bytes `""` | payload | `user:123 → {"" → profile_json}` |
| **Named Set** | set name | member | empty bytes `""` | `followers:alice → {bob → "", carol → ""}` |
| **Sorted Events** | entity ID | timestamp (8-byte BE) | event data | `activity:u1 → {ts1 → e1, ts2 → e2}` |
| **Versioned Record** | entity ID | version key (ordered) | snapshot | `doc:42 → {v001 → s1, v002 → s2}` |
| **Adjacency List** | node ID | neighbor ID | edge metadata | `graph:nodeA → {nodeB → weight, nodeC → weight}` |
| **Counter / Aggregation** | entity ID | dimension key | counter bytes | `metrics:api → {2024-09-18 → count_bytes}` |
| **Prefix Tree** | root path | sub-path segments | leaf data | `config:/app → {/db/host → val, /db/port → val}` |

### 2.3 Why Sorted Values Matter

1. **Range queries are first-class** — `match_range(start, end)` resolves to a contiguous scan within a single partition. No scatter-gather, no secondary indexes.
2. **Single tombstone for range deletes** — Deleting all items in a range writes one range tombstone marker rather than N individual tombstones. This directly addresses Cassandra's tombstone compaction problem.
3. **Predictable read amplification** — Items within a record are co-located in SSTables, so reading a record touches at most `L0_count + 1` SSTables. Bloom filters on record IDs eliminate SSTables that don't contain the target record.
4. **Natural merge during compaction** — Items for the same record from different SSTables merge together during compaction, deduplicating updates and dropping expired tombstones in a single pass.
5. **Efficient byte-based pagination** — Sorted order means the page token is just the last item key seen. Resume by seeking to `(record_id, last_key + 1)`.

---

## 3. Composite Key Encoding

The entire storage engine operates on a single flat keyspace of composite keys. Every data structure (WAL, memtable, SSTable) depends on this encoding.

### 3.1 Binary Format

```
composite_key = [record_id_bytes] [0x00] [item_key_bytes]
```

The separator is `0x00` (null byte). This works because:

- Record IDs are UTF-8 strings. Valid UTF-8 never contains `0x00` (null bytes are only valid as the single-byte encoding of U+0000, which we disallow in record IDs).
- Item keys are arbitrary bytes, so `0x00` can appear in them — but only *after* the separator.
- The first `0x00` in the composite key unambiguously marks the boundary.

**Parsing:** Scan forward from byte 0 until the first `0x00`. Everything before it is `record_id`. Everything after it is `item_key`. An empty item key (for simple KV pattern) produces `[record_id] [0x00]` — the composite key ends with the separator.

### 3.2 Sort Order Guarantee

Composite keys sort correctly with raw byte comparison (`memcmp`):

1. Keys are ordered first by `record_id` (lexicographic on UTF-8 bytes)
2. Then by `item_key` (lexicographic on raw bytes)

This is because `0x00` sorts before any valid UTF-8 continuation byte, so all items for record `"aaa"` sort before all items for record `"aab"`, regardless of item key content.

This means:
- **Point lookup** of a single item: binary search for exact `(record_id, item_key)`
- **Range scan** within a record: seek to `(record_id, start_key)`, scan forward until `record_id` changes or `end_key` is reached
- **Full record read**: seek to `(record_id, MIN_KEY)`, scan until `record_id` changes

### 3.3 Key Length Limits

| Field | Max Length | Rationale |
|-------|-----------|-----------|
| `record_id` | 256 bytes | Partition key hashing input; larger IDs waste bloom filter bits |
| `item_key` | 4096 bytes | Generous for timestamps, UUIDs, paths; still fits in one SSTable data block |
| `composite_key` | 4353 bytes | 256 + 1 + 4096 |

### 3.4 Composite Key for Range Tombstones

Range tombstones use a synthetic composite key with a sentinel marker:

```
range_tombstone_key = [record_id] [0x00] [RANGE_TOMBSTONE_PREFIX] [start_key]
range_tombstone_val = [end_key] [inclusive_flags]
```

Where `RANGE_TOMBSTONE_PREFIX` is `0xFF` — sorts after all valid item keys within the record, keeping tombstone metadata separate from data entries in the memtable and SSTable. During reads, tombstones are checked by scanning the tombstone region for the record.

---

## 4. Shard-Per-Core Architecture

The storage engine pins each partition's data structures to a specific CPU core, eliminating lock contention from the hot path entirely.

**Design:**
- Each core owns its own: active memtable, frozen memtable list, WAL buffer, compaction state, LRU cache region
- Zero lock acquisitions on the write or read hot path — data is partitioned, not shared
- Cross-core communication via lock-free SPSC (single-producer single-consumer) queues
- Priority-based cooperative scheduling with preemption

**Partition-to-core mapping:** Virtual partitions (vnodes) are mapped to cores using `vnode_id % num_cores`. Cross-partition queries post tasks to target cores via SPSC queues.

**Priority-based task scheduling:** Each core runs a priority scheduler with three tiers:

| Priority | Tasks | Preemption |
|----------|-------|------------|
| P0 (critical) | Read requests, write path (memtable insert + WAL buffer append) | Immediate — preempts P1/P2 |
| P1 (normal) | Memtable flush, WAL group commit flush | Yields to P0 within 10μs |
| P2 (background) | Compaction, bloom/ribbon filter construction, blob GC | Yields to P0/P1 within 50μs |

Compaction tasks check a per-core preemption flag every 50μs. When a read or write arrives, the scheduler sets the flag, and the compaction task yields at its next check point. This bounds compaction-induced read latency to 50μs worst case. Compaction progress is not significantly impacted because its throughput is I/O-bound (S3 reads/writes), not CPU-bound.

---

## 5. Write-Ahead Log (WAL)

### 5.1 Segment-Based Architecture

The WAL is not a single file — it is a sequence of **segments**. Each segment is a fixed-size file (default 32 MB) on local disk.

```
wal/
  partition-{id}/
    segment-000000000001.wal    # oldest active segment
    segment-000000000002.wal
    segment-000000000003.wal    # current segment (append target)
```

**Why segments instead of one file:**
- Truncation is file deletion, not file rewriting. After a flush confirms on S3, we delete entire segments rather than truncating the middle of a file.
- Each segment can be independently fsynced and rotated.
- Segment numbers are monotonically increasing 12-digit zero-padded integers. Gaps in numbering are fine (deleted segments leave gaps).

### 5.2 Entry Wire Format

Each WAL entry is length-prefixed and CRC-protected:

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
│ idempotency_token: bytes                            │  24 bytes (fixed)
├─────────────────────────────────────────────────────┤
│ crc32: uint32 (over all preceding bytes in entry)   │  4 bytes
└─────────────────────────────────────────────────────┘
```

**Idempotency token format:** 24 bytes = 8 bytes (client-assigned request ID, uint64) + 16 bytes (random UUID v4). Used for at-least-once delivery deduplication. An all-zero token (24 zero bytes) means "no idempotency" — the entry is always applied.

**Entry type semantics:**
- `PUT` (0): `item_key` is the key, `item_value` is the value. Standard put operation.
- `DELETE` (1): `item_key` is the key to delete. `item_value` is empty (length 0).
- `RANGE_DELETE` (2): `item_key` is the range start key (inclusive). `item_value` encodes the range end key (exclusive). Both are scoped to the `record_id` in the same entry.

**Recovery parsing:** Read `entry_length`, read that many bytes + 4 (CRC), validate CRC. If CRC fails at the tail of the segment, the entry was a partial write (crash during append) — safe to discard. If CRC fails mid-segment, the segment is corrupted — recovery halts and alerts.

### 5.3 Group Commit (WAL Batching)

Individual fsync + replication per write is prohibitively expensive. The write path uses group commit:

1. **Buffering phase:** Incoming writes are appended to an in-memory WAL buffer and inserted into the memtable immediately. The write is not yet ACK'd.
2. **Commit phase:** Every 200μs (configurable) or when the buffer reaches 256KB (whichever comes first), the buffer is fsynced as a single batch and replicated to followers as a single batch message.
3. **ACK phase:** All writes in the committed batch are ACK'd to their respective clients simultaneously.

This reduces fsync calls from N/sec (one per write) to ~5,000/sec (one per batch), and follower replication from N round-trips to one round-trip per batch. Under sustained load, hundreds of writes share a single fsync + replication round-trip, yielding 5-10x throughput improvement.

**Commit ordering and partial failure:** The group commit phases execute in strict order — fsync must complete before replication begins, and replication quorum must be met before any write in the batch is ACK'd:

1. **Owner crashes after fsync, before replication:** The batch is in the owner's local WAL but no follower has it. Since no ACK was sent to clients, no data loss from the client's perspective. If the dead node's disk is recoverable, idempotency tokens deduplicate the replayed entries against any retries the new owner already accepted.
2. **Owner crashes after partial replication (quorum not met):** Same — no client ACK sent. WAL reconciliation protocol collects partial entries from surviving followers and merges them.
3. **Owner crashes after quorum replication, before ACK:** The batch is durable. Clients retry and are deduplicated via idempotency tokens. No data loss, no duplicates.

**Key invariant:** A write is only considered durable when the client receives an ACK, and an ACK is only sent after fsync + quorum replication.

**Latency trade-off:** Group commit adds up to 200μs of buffering latency. For latency-sensitive namespaces, the commit interval is configurable down to 50μs. A `flush_immediate: bool` flag on PutItems bypasses batching entirely.

### 5.4 Fsync Strategy

Two modes, configurable per namespace:

| Mode | Behavior | Durability | Latency |
|------|----------|------------|---------|
| `SYNC` (default) | `fsync()` after every write batch | Survives power loss | ~0.5-2ms per write |
| `BATCH_SYNC` | `fsync()` on timer (every 10ms) | May lose last 10ms on power loss | ~50-100μs per write |

In `BATCH_SYNC` mode, the write path appends to an OS buffer and returns immediately. A background thread calls `fsync()` every 10ms. For QUORUM consistency, `BATCH_SYNC` on the owner is acceptable if followers use `SYNC`.

### 5.5 Dirty Segment Tracking

A segment cannot be deleted until **all** memtable generations that wrote to it have been flushed to S3.

Each segment maintains a dirty map:

```
dirty_map: HashMap<memtable_generation_id, highest_sequence_in_this_segment>
```

When a memtable (generation G) is flushed and its SSTable is confirmed on S3:
1. Mark generation G as flushed in the segment manager
2. For each segment, remove G from its dirty map
3. If a segment's dirty map is empty, the segment is safe to delete

**Forced flush on WAL pressure:** A low-write partition may never reach the memtable size threshold, pinning WAL segments indefinitely. Two additional flush triggers:

1. **Segment age trigger:** If any WAL segment has been pinned for longer than `wal_segment_max_age` (default: 5 minutes), force-flush all memtables that reference it.
2. **WAL size pressure trigger:** If total WAL size on disk exceeds `wal_max_total_bytes` (default: 512 MB), force-flush the memtable referencing the oldest pinned segment.

Both triggers produce undersized L0 SSTables, but L0→L1 compaction will merge them.

**WAL size backpressure:** If the WAL exceeds `max_wal_size` (default 256MB — 4x the memtable threshold), writes to that partition are stalled with `RESOURCE_EXHAUSTED` until a flush completes. This prevents local disk exhaustion during prolonged S3 unavailability.

### 5.6 WAL for Follower Nodes

Followers maintain a separate `follower-wal/` directory with the same segment format. Key differences:

- No fsync required (followers are a secondary durability layer)
- Entries include the owner's sequence number (not a local sequence)
- On failover, the new owner replays from `last_flushed_sequence` in the manifest
- Follower WAL is truncated on the same schedule as the owner (after S3 flush confirmation, propagated via gossip)

**Failover divergence handling:** During a network partition, the old owner and a follower could have divergent WAL entries. On failover:

1. The new owner reads `last_flushed_sequence` from the manifest.
2. Replays its local follower WAL entries with `sequence > last_flushed_sequence`.
3. Any entries from the old owner's WAL that were never replicated — and never flushed to S3 — are **lost**. This is the expected data loss window.
4. The old owner's zombie writes are fenced by the epoch mechanism (Section 8.4).

---

## 6. Memtable Internals

### 6.1 Data Structure: Skip List

The memtable is a skip list owned by a single CPU core — no concurrent access from other cores (shard-per-core model).

**Skip list properties:**
- Max height: 12 levels (supports ~4 billion entries efficiently)
- Probability: 1/4 (each level has 1/4 the entries of the level below)
- Node allocation: Arena-based. All nodes are allocated from contiguous memory, improving cache locality.

**Concurrency model:**
- **Single-writer:** The owning core is the sole writer. No CAS loops, no contention. Inserts are plain pointer writes.
- **Reads from owning core:** Direct traversal, zero synchronization.
- **Cross-core reads:** Posted as tasks via SPSC queue to the owning core, which executes the read and returns the result.

Each memtable entry stores:

```
MemtableEntry {
  composite_key:    Bytes           // record_id + separator + item_key
  value:            Bytes           // item value (or tombstone marker)
  metadata:         Bytes           // item metadata
  idempotency_key:  IdempotencyToken // for dedup during the unflushed window
  sequence_number:  uint64          // WAL sequence number for ordering
  entry_type:       PUT | DELETE | RANGE_DELETE
}
```

### 6.2 Arena Allocator

Each memtable owns a bump allocator with pre-allocated blocks (default 1 MB each):

```
Arena {
  blocks: Vec<Box<[u8; 1_048_576]>>   // 1 MB blocks
  current_offset: usize               // offset within current block (no atomic — single-writer)
  total_allocated: usize              // running total for threshold check
}
```

Arena deallocation is O(1) — drop all blocks when the memtable is released after SSTable flush. `total_allocated` is the memtable size metric. In the shard-per-core model, offset and total are plain integers (no atomics needed).

### 6.3 Memtable Freeze and Swap

When `total_allocated >= threshold` (default 64MB) **or** the time threshold fires (default 5 minutes):

1. **Swap:** Replace the active memtable pointer with a new empty memtable. Plain pointer swap on the owning core. The old memtable becomes "frozen."
2. **Frozen memtable list:** The frozen memtable is pushed onto a read-only list. Reads still check frozen memtables.
3. **Flush trigger:** A background flush task is notified.

The time-based trigger bounds the unflushed window for low-throughput partitions. Without it, a partition writing 1KB/s would take ~18 hours to reach 64MB.

**Critical invariant:** Between freeze and flush completion, the frozen memtable MUST remain accessible for reads.

### 6.4 Sequence Number Assignment

Each write gets a monotonically increasing 64-bit sequence number from a per-partition counter:

- Initialized from the WAL's last sequence number on startup
- Incremented for each write
- Written to both WAL entries and memtable entries

Purposes:
1. **Ordering:** When the same composite key appears multiple times, the highest sequence number wins
2. **WAL recovery:** Replay entries with sequence numbers > manifest's `last_flushed_sequence`

### 6.5 Range Tombstone Index

The memtable maintains a secondary index for range tombstones:

```rust
struct RangeTombstoneIndex {
    // Sorted by (record_id, start_key)
    tombstones: Vec<RangeTombstone>,
}

struct RangeTombstone {
    record_id: Bytes,
    start_key: Bytes,      // inclusive
    end_key: Bytes,         // exclusive
    sequence_number: u64,
}
```

During point reads, this index is checked to determine if the target key falls within a range tombstone with a higher sequence number than the found entry.

---

## 7. SSTable Format and Construction

### 7.1 Binary Format

A custom binary format. Entries are stored by composite key, so all items for a record are contiguous and sorted:

```
┌──────────────────────────────────┐
│ Header                           │
│   magic: 0x464C4442 ("FLDB")    │
│   version: uint16                │
│   compression: NONE|SNAPPY|ZSTD  │
│   entry_count: uint64            │
├──────────────────────────────────┤
│ Data Block 0 (4KB default)       │
│   Entry: [record_id_len | record_id | item_key_len | item_key │
│           | value_len | value | metadata_len | metadata        │
│           | entry_type | sequence_number]                      │
│   Entry: ...                     │
├──────────────────────────────────┤
│ Data Block 1                     │
│   ...                            │
├──────────────────────────────────┤
│ ...                              │
├──────────────────────────────────┤
│ Dedup Block                      │
│   compact hash set of            │
│   idempotency token hashes       │
│   (128-bit per token)            │
├──────────────────────────────────┤
│ Bloom/Ribbon Filter Section      │
│   filter over record_id values   │
│   (not item keys)                │
│   partitioned by key range       │
├──────────────────────────────────┤
│ Index Block                      │
│   block_0_first_key → offset, len│
│   block_1_first_key → offset, len│
│   ...                            │
├──────────────────────────────────┤
│ Footer (80 bytes)                │
│   bloom_filter_offset: u64       │
│   bloom_filter_size: u32         │
│   index_block_offset: u64        │
│   index_block_size: u32          │
│   entry_count: u64               │
│   min_key: [u8; 16]             │
│   max_key: [u8; 16]             │
│   compression_type: u8           │
│   _padding: [u8; 1]             │
│   format_version: u16            │
│   crc32: u32                     │
│   magic: 0x464C4442             │
│   _reserved: [u8; 4]            │
└──────────────────────────────────┘
```

Key properties:
- **Sorted by composite key** — enables binary search via the sparse index and contiguous range scans within a record
- **Bloom/ribbon filter on record IDs** — eliminates SSTables that don't contain the target record before any I/O
- **S3 byte-range reads** — individual data blocks can be fetched without downloading the entire file
- **Compression per block** — each data block is independently compressed, allowing random access

### 7.2 Block Building

Data blocks are built incrementally as the frozen memtable is iterated in sorted order:

```
BlockBuilder {
    buffer: Vec<u8>,
    entry_count: u32,
    block_size_target: usize, // default 4 KB
    first_key: Option<CompositeKey>,
    last_key: Option<CompositeKey>,
    record_ids_seen: HashSet<RecordId>,  // fed to bloom filter
}
```

**Entry encoding within a block:**

```
[record_id_len: varint] [record_id: bytes]
[item_key_len: varint]  [item_key: bytes]
[value_len: varint]     [value: bytes]
[metadata_len: varint]  [metadata: bytes]
[entry_type: u8]
[sequence_number: varint]
```

**Record ID deduplication within blocks:** When consecutive entries share the same `record_id` (very common), the second and subsequent entries store `record_id_len = 0` and omit the record_id bytes. The reader carries forward the last seen record_id. Can reduce block size by 30-50% for wide records.

**Constraint:** Empty record IDs are forbidden at the API layer. This makes `record_id_len = 0` an unambiguous dedup signal. The first entry in every block MUST include the full record_id.

When `buffer.len() >= block_size_target`, the block is finalized:
1. Compute CRC32 over the uncompressed buffer and append it (4 bytes)
2. Compress the buffer + CRC (Snappy or ZSTD, configurable)
3. Record `(first_key, offset, compressed_len, uncompressed_len)` in the index
4. Collect `record_ids_seen` into the bloom filter builder
5. Reset the block builder

### 7.3 Bloom Filter and Ribbon Filter Construction

Built incrementally as blocks are finalized:

```
BloomFilterBuilder {
    bits: BitVec,
    num_hash_functions: u32,
    record_ids_added: u64,
}
```

**Important:** The bloom filter indexes `record_id`s from *all* entry types — PUTs, DELETEs, and range tombstones alike. This ensures a bloom filter positive also covers any tombstones for that record.

**Sizing:** At 1% FPR with 10 bits/key, a 64 MB memtable with ~100K unique record IDs produces a ~125 KB bloom filter. Loaded into memory when the SSTable is opened.

**Hash function:** Double-hashing with two independent MurmurHash3 seeds. The k-th hash is computed as `h1 + k * h2`.

**Ribbon filters for compaction-produced SSTables:** SSTables produced by compaction (L1+) use ribbon filters instead of standard bloom filters. Ribbon filters achieve the same 1% FPR at ~7 bits/key instead of ~10 bits/key — 30% smaller. Construction cost is higher (~230 bits/key temporary memory vs. ~75 for bloom) but this is a one-time cost at compaction time. L0 SSTables (produced by memtable flush) continue to use standard bloom filters for lower flush latency.

**Partitioned filters:** For large SSTables, filters are split into small filter blocks by key range. Cache-friendly (fit in one DRAM cache line). For S3, this enables fetching only the relevant filter partition via a byte-range request. The SSTable index block includes a secondary "filter index" mapping key range prefixes to filter block offsets.

**Large-record optimization:** For records with a high item count (exceeding configurable threshold, default 10,000 items), record-level bloom filters cause excessive read amplification. Mitigations:

1. **Per-block min/max key fences:** Each data block stores the minimum and maximum composite key. After the bloom filter identifies candidate SSTables, per-block key fences skip blocks that don't overlap the target item key range. Effectively free since index blocks are cached in DRAM.
2. **Prefix bloom filters for hot records:** When compaction detects a record exceeding the threshold, it builds an additional prefix bloom filter keyed on `(record_id, item_key_prefix)` using the first 4 bytes of the item key. Narrows the candidate block set for point lookups from O(blocks_per_record) to O(1) expected, at ~4 additional bits per item key.

### 7.4 Index Block Construction

The sparse index maps the first composite key of each data block to its offset:

```
IndexEntry {
    first_key: CompositeKey,
    block_offset: u64,
    block_size: u32,
    uncompressed_size: u32,
}
```

Entries are stored sorted. Binary search locates the block that could contain any target key.

### 7.5 Footer Details

Fixed-size (80 bytes) trailer at the end of the SSTable:

```
Footer {                                                          // Offset  Size
    bloom_filter_offset: u64,                                     //  0       8
    bloom_filter_size: u32,                                       //  8       4
    index_block_offset: u64,                                      // 12       8
    index_block_size: u32,                                        // 20       4
    entry_count: u64,                                             // 24       8
    min_key: [u8; 16],           // truncated min composite key   // 32      16
    max_key: [u8; 16],           // truncated max composite key   // 48      16
    compression_type: u8,                                         // 64       1
    _padding: [u8; 1],                                            // 65       1
    format_version: u16,                                          // 66       2
    crc32: u32,                                                   // 68       4
    magic: u32,                  // 0x464C4442 ("FLDB")           // 72       4
    _reserved: [u8; 4],                                           // 76       4
}                                                                 // Total:  80
```

**`min_key`/`max_key` truncation:** Keys are truncated to 16 bytes. Can produce false positives but never false negatives. The footer key range is a cheap pre-filter to avoid loading bloom filters for completely disjoint SSTables.

**Prefix-heavy workload mitigation:** For multi-tenant workloads where record IDs share long common prefixes, the footer stores **prefix-stripped keys**: compute the longest common prefix (LCP) of `min_key` and `max_key`, store the LCP length as a `u8` field (repurposes first byte of `_reserved`), then store 16 bytes starting *after* the LCP.

### 7.6 S3 Upload Strategy: Streaming Multipart

For SSTables > 16 MB, use S3 multipart upload with streaming:

```
Phase 1: Initiate multipart upload → get upload_id
Phase 2: Upload parts concurrently as they're built
         [Build Part 1] ──► [Upload Part 1]     (16 MB)
         [Build Part 2] ──► [Upload Part 2]     (overlap: build 2 while uploading 1)
         ...
Phase 3: Complete multipart upload → SSTable is atomically visible
```

- Part size: 16 MB (empirically optimal for S3 throughput)
- Concurrency: Build part N+1 while uploading part N (double buffering)
- Peak memory: O(32 MB) regardless of SSTable size
- If SSTable < 16 MB: single `PutObject` instead
- S3 lifecycle rule: abort incomplete multipart uploads after 24 hours

**Failure handling:**
- If any part upload fails: retry that part (individually retryable)
- If the process crashes mid-upload: incomplete multipart upload cleaned up by lifecycle rule. No orphaned data visible to readers.

---

## 8. Manifest: The Single Source of Truth

The manifest is the most critical data structure in flushdb. It defines which SSTables are live at each level, and its update protocol determines the correctness of the entire system.

### 8.1 Manifest Contents

```json
{
  "format_version": 1,
  "manifest_id": "00000000000000000042",
  "writer_epoch": 7,
  "compactor_epoch": 3,
  "namespace": "my-namespace",
  "created_at_ms": 1709251200000,

  "last_flushed_sequence": 458923,

  "levels": {
    "L0": [
      {
        "id": "01JKQW3XYZ-L0-0001",
        "size_bytes": 67108864,
        "entry_count": 52341,
        "min_key": "dGVuYW50LTE...",
        "max_key": "dGVuYW50LTk...",
        "bloom_filter_offset": 66846720,
        "bloom_filter_size": 131072,
        "index_offset": 66977792,
        "index_size": 65536,
        "created_at_ms": 1709251195000,
        "sequence_range": [450000, 458923],
        "record_id_count": 12000
      }
    ],
    "L1": [
      {
        "id": "01JKPV2ABC-L1-0001",
        "run_id": "run-01JKPV2ABC",
        "fragment_index": 0,
        "size_bytes": 67108864,
        "entry_count": 80000,
        "min_key": "...",
        "max_key": "...",
        "bloom_filter_offset": 66846720,
        "bloom_filter_size": 131072,
        "index_offset": 66977792,
        "index_size": 65536,
        "created_at_ms": 1709250000000,
        "sequence_range": [400000, 449999],
        "record_id_count": 25000
      }
    ],
    "L2": [],
    "L3": []
  },

  "blob_files": [
    {
      "id": "blob-01JKQW4DEF",
      "size_bytes": 10485760,
      "live_bytes": 8388608,
      "entry_count": 5,
      "referenced_by_ssts": ["01JKPV2ABC-L1-0001"]
    }
  ],

  "tombstone_compaction_watermarks": {
    "L1": 1709200000000,
    "L2": 1709100000000
  },

  "previous_manifest_id": "00000000000000000041"
}
```

### 8.2 Manifest ID Scheme

Manifest IDs are **zero-padded 20-digit integers**:

```
00000000000000000001
00000000000000000002
...
00000000000000000042
```

**Why this scheme:**

1. **Lexicographic ordering = version ordering.** The "current" manifest is always the one with the highest ID when listed with S3 `ListObjectsV2`.
2. **Eliminates the two-step update problem.** Writing the new manifest IS the update — readers always pick the highest ID.
3. **Natural CAS.** Two writers computing the same "next ID" will race: exactly one succeeds via `If-None-Match: *`, the other gets HTTP 412 and retries.

**S3 path:**
```
s3://{bucket}/flushdb/{namespace}/manifests/00000000000000000042
```

**Finding the current manifest:**

1. **`manifest.json` pointer (fast path):** A well-known pointer file at `s3://.../{namespace}/manifest.json` contains `{"current_id": "00000000000000000042"}`. Updated best-effort after each successful manifest CAS.
2. **Full listing (fallback/validation):** `ListObjectsV2` with prefix `flushdb/{namespace}/manifests/`. Used on startup and when the pointer seems stale.

**Staleness window:** If the pointer is stale (crash between manifest CAS and pointer update), a reader sees an older manifest. For QUORUM reads, the read path validates the pointer by checking `writer_epoch` against the expected epoch from the lease. If stale, falls back to listing. Cache the validated manifest locally with a TTL of 1 second.

### 8.3 Manifest Update Protocol (CAS)

Every operation that changes the set of live SSTables must update the manifest atomically.

```
ManifestUpdate {
    trigger: FLUSH | COMPACTION | GC,
    expected_previous_id: ManifestId,
    add_sstables: Vec<(Level, SSTableMeta)>,
    remove_sstables: Vec<(Level, SSTableId)>,
    new_last_flushed_sequence: Option<u64>,
    writer_epoch: u64,
    compactor_epoch: u64,
}
```

**Protocol:**

```
Step 1: Read current manifest (ID = N)

Step 2: Validate epoch
        if trigger == FLUSH && current.writer_epoch > my_writer_epoch:
            HALT — zombie writer
        if trigger == COMPACTION && current.compactor_epoch > my_compactor_epoch:
            HALT — zombie compactor

Step 3: Compute new manifest
        new_manifest = apply(current, update)
        new_manifest.manifest_id = current.manifest_id + 1
        new_manifest.previous_manifest_id = current.manifest_id

Step 4: Write to S3 with conditional write
        PUT s3://.../{new_manifest_id}
            If-None-Match: *

Step 5: Handle result
        SUCCESS → update manifest.json pointer, bump gossip version
        412 PreconditionFailed → re-read, re-validate, retry
        other error → retry with backoff
```

S3 conditional writes (`If-None-Match: *`, available since August 2024) reject a `PutObject` if the key already exists. This gives optimistic concurrency control without external coordination.

### 8.4 Epoch-Based Fencing

**The zombie writer problem:** Node A owns partition P, begins flushing. Node A's lease expires. Node B acquires the lease. Node A's flush completes and it tries to update the manifest — if it succeeds, it could cause Node B's unflushed writes to be skipped during recovery.

**Writer epochs:**

```
On lease acquisition:
    1. Acquire lease (S3 CAS on lease object)
    2. Read current manifest
    3. new_epoch = manifest.writer_epoch + 1
    4. Write new manifest with writer_epoch = new_epoch (via CAS)
    5. Only after step 4 succeeds, begin accepting writes
    6. CRITICAL: No writes accepted between steps 1 and 5.
       The node is in "fencing" state — serves reads from old manifest
       but rejects writes until the epoch is committed.

On manifest update (flush):
    1. Read current manifest
    2. If manifest.writer_epoch > my_epoch: zombie. HALT.
    3. Proceed with CAS protocol
```

**Write barrier after epoch acquisition:** The new owner waits before serving writes:
- **Lease expired (old owner presumed dead):** Barrier = `old_owner_max_inflight_time` (default 5 seconds)
- **Lease preempted (forced takeover during rebalance):** Barrier = `lease_ttl / 2` (default 15 seconds)
- During the barrier, reads are served from flushed manifest state plus recovered WAL entries.

**Compactor epochs** work identically with a separate `compactor_epoch` field.

### 8.5 Manifest Rollback

If a manifest update produces a bad state, rollback is simple:

1. Write a new manifest with `manifest_id = current + 1` that has the same contents as a known-good older manifest
2. The "rolled back" manifest has a higher ID, so it becomes current
3. SSTables referenced by the bad manifest but not the rollback target become orphans — GC cleans them up

### 8.6 Manifest Compaction

The manifest grows with every flush and compaction.

**Manifest snapshots:** Every 100 manifest versions (configurable via `manifest_snapshot_interval`), the system writes a self-contained **snapshot manifest** that includes the full materialized state. Tagged with `snapshot: true`. On startup or failover, the node reads only the latest snapshot manifest and applies subsequent non-snapshot manifests on top.

**Old manifest pruning:** Versions older than the second-most-recent snapshot are eligible for deletion. Two snapshots are retained for rollback. A background task deletes in batches of 50, rate-limited to avoid S3 DELETE throttling.

**Manifest size budget:** If a single manifest exceeds `max_manifest_size` (default 16MB), the system forces a snapshot on the next write.

### 8.7 Concurrent Operations

| Operation | Frequency | Conflict Probability |
|-----------|-----------|---------------------|
| Memtable flush (L0 add) | Every ~60s per partition | Low |
| L0→L1 compaction | When L0 reaches 4 files | Medium (can overlap with flush) |
| L1→L2 compaction | When L1 exceeds 256 MB | Low |
| L2→L3 compaction | When L2 exceeds 2.56 GB | Very low |
| Tombstone GC | Background timer | Very low |

**Conflict resolution:** Pure retry. Re-read current manifest, check if our update is still valid (inputs not already consumed), re-apply if valid, retry CAS.

---

## 9. Flush Pipeline: Memtable to S3

### 9.1 End-to-End Flush Sequence

```
1. TRIGGER: memtable.total_allocated >= threshold OR time >= 5 minutes
   │
2. FREEZE: Pointer swap (active → frozen, new empty → active)
   │
3. BUILD SSTable:
   │  Iterate frozen memtable in sorted order
   │  Build data blocks (4 KB target, compressed)
   │  Build bloom filter over record IDs
   │  Build sparse index + footer
   │
4. UPLOAD to S3:
   │  If SSTable < 16 MB: single PutObject
   │  If SSTable >= 16 MB: multipart upload (streaming)
   │
5. UPDATE MANIFEST (CAS protocol):
   │  Add new SSTable to L0
   │  Set last_flushed_sequence = max sequence in flushed memtable
   │  Validate writer epoch
   │
6. TRUNCATE WAL:
   │  Delete segments where all referenced generations are flushed
   │
7. RELEASE frozen memtable:
   │  Drop arena allocator, remove from frozen list
   │
8. NOTIFY:
   │  Bump manifest version in gossip payload
   │
9. CHECK compaction trigger:
      If L0 now has > 4 SSTables, schedule L0→L1 compaction
```

### 9.2 Failure Modes During Flush

| Failure Point | Impact | Recovery |
|---------------|--------|----------|
| Crash during build (step 3) | No S3 state changed | WAL replay recreates memtable |
| Crash during upload (step 4) | Incomplete multipart upload on S3 | S3 lifecycle rule aborts it. WAL replay. |
| Crash during manifest CAS (step 5) | SSTable on S3 but not referenced | Orphaned SSTable — GC cleans it. WAL replay. |
| Crash after manifest CAS, before WAL truncation (step 6) | Manifest updated, WAL not truncated | WAL replay replays already-flushed entries. Idempotent. |
| CAS failure in step 5 | Another writer updated manifest | Retry CAS. SSTable already on S3. |

**Key insight:** The WAL is the safety net. The manifest update is the commit point.

### 9.3 Flush Backpressure

1. **Second frozen memtable:** Active fills again while first frozen is still flushing. Reads check: active → frozen-1 → frozen-2 → SSTables.
2. **Write stall threshold:** If N frozen memtables accumulate (default N=3), writes are rejected with backpressure error. 3 frozen = 192 MB pinned.

---

## 10. Compaction Pipeline

### 10.1 Compaction Triggers

| Trigger | Condition | Action |
|---------|-----------|--------|
| L0 overflow | `len(L0) > 4` | Compact all L0 → overlapping range in L1 |
| L1 overflow | `total_size(L1) > 256 MB` | Pick one L1 SSTable, compact with overlapping L2 range |
| L2 overflow | `total_size(L2) > 2.56 GB` | Pick one L2 SSTable, compact with overlapping L3 range |
| Tombstone TTL | Periodic timer (1 hour) | Compact bottom-level SSTables with expired tombstones |

**Level size rationale:** 10x size ratio. L0 triggers at 4 files (~256 MB). L1 = 256 MB. L2 = 2.56 GB. L3 = 25.6 GB. Worst-case write amplification ~10 per level.

**Write stalling when L0 is full:**

1. **L0 soft limit (4 files):** Compaction triggered. Writes proceed at full speed.
2. **L0 slow limit (8 files):** Write throughput throttled. Each write sleeps `(l0_count - soft_limit) * 1ms`.
3. **L0 hard limit (12 files):** Writes fully stalled with `RESOURCE_EXHAUSTED` and `retry-after` hint.

**Space Amplification Goal (SAG):** Configurable (1.0–2.0, default 1.75). When the second-largest tier reaches half the size of the largest tier, a cross-tier compaction is triggered.

### 10.2 SSTable Run Fragments

Instead of monolithic SSTables, compaction produces **SSTable runs** — sequences of smaller, non-overlapping fragments (~1 GB each for L1+, ~64 MB for compaction output):

```
sstables/L1/run-01JKPV2ABC/frag-0000.sst
sstables/L1/run-01JKPV2ABC/frag-0001.sst
sstables/L1/run-01JKPV2ABC/frag-0002.sst
```

**Benefits:**
- Delete each input fragment as soon as its key range is fully written. Max temporary space = `2 * fragment_size` instead of `2 * total_run_size`.
- A run is logically one SSTable for read purposes.

**Trivial move optimization:** When an L(N) fragment's key range does not overlap with any fragment in L(N+1), the fragment is "moved" by manifest-only update — no S3 I/O. For time-ordered keys, the majority of compactions are trivial moves.

**Partial range compaction:** Only overlapping fragments participate in the merge. Non-overlapping L(N+1) fragments are left in place.

### 10.3 Compaction Merge Process

```
1. Open iterators on all input SSTables
   - Fetch index blocks and bloom filters into DRAM
   - Data blocks fetched on demand

2. Merge-sort by composite key:
   - Duplicate keys: keep highest sequence_number
   - Point tombstones: if TTL expired AND bottom level, drop both
   - Range tombstones: propagate to output if they still cover keys in lower levels

3. Build output SSTable fragments:
   - Split into new fragment at size boundaries
   - Each fragment gets its own bloom filter, index, footer
   - Upload each fragment to S3 as completed

4. Update manifest (CAS):
   - Add: new run to target level
   - Remove: input SSTables from source levels
   - Validate compactor_epoch

5. Delete old SSTable objects from S3:
   - Deferred deletion via reference tracking (Section 14.2)
```

### 10.4 Tombstone Lifecycle

```
Write: Client DeleteItems → WAL → Memtable → L0 SSTable
  │
  ▼
L1 (compaction): Tombstone merged. Covered PUTs dropped.
                 Tombstone MUST survive — older PUTs may exist in L2/L3.
  │
  ▼
L2 (compaction): Same — tombstone propagated, covered entries dropped.
  │
  ▼
L3 (bottom level): Tombstone's TTL checked.
                    If expired: tombstone dropped. If not: persists.
```

**Point tombstone TTL:** Default 7 days. Jitter applied at compaction evaluation time (not write time). Per-tombstone random delay of 0-24 hours, seeded from hash of composite key (deterministic, stable across retries).

**Range tombstone watermark protocol:**

1. **Watermark definition:** `tombstone_compaction_watermarks[Li]` = the timestamp such that all range tombstones created before this timestamp have been fully propagated through level `Li`.
2. **Watermark update:** When compaction at level `Li` completes, update to `min(oldest_tombstone_timestamp_in_compaction_input)`.
3. **Deletion eligibility:** A range tombstone at `Li` is eligible only when `tombstone_compaction_watermarks[Lj] >= tombstone.created_at` for ALL levels `Lj > Li`.
4. **Partial coverage:** Tombstone copied into each compaction output that overlaps its range.
5. **Safety invariant:** Never deleted at `Li` if any SSTable at `Li+1...Ln` has overlapping keys with lower sequence numbers.

**Delete-only compaction:** If an SSTable's entire key range is covered by a range tombstone with a higher sequence number, remove it from the manifest without reading — pure metadata operation.

---

## 11. Read Path

### 11.1 Merge-Read Pattern

Reads merge across layers, from newest to oldest:

```
1. Active memtable (newest writes)
2. Frozen memtable(s) (pending flush)
3. L0 SSTables (most recent flushes, may overlap)
4. L1 SSTables (non-overlapping)
5. L2 SSTables (non-overlapping)
6. L3 SSTables (non-overlapping)
```

### 11.2 Point Read

1. Search active memtable, then frozen memtable — return immediately if found
2. Issue bloom/ribbon filter checks for all candidate SSTables concurrently (filters are in DRAM)
3. For SSTables passing the filter, issue concurrent S3 byte-range GETs for index blocks (L0 checked in parallel)
4. Issue concurrent S3 byte-range GETs for target data blocks
5. Merge results by sequence number — if it's a tombstone, return not-found

### 11.3 Range Read

1. Open iterators on all layers with matching record ID (bloom filter eliminates non-matching SSTables)
2. Merge-sort iterators by item key, applying tombstone filtering
3. **Dual-buffer prefetch:** consume buffer A while async-filling buffer B from S3, then swap
4. Accumulate results until byte-based page budget exhausted or range end reached
5. Return results plus a page token encoding the last key emitted

**Full record read** (`match_all`): Same as range read with start = MIN_KEY, end = MAX_KEY.

### 11.4 S3 GET Reduction Strategies

1. **Persistent index block cache:** Index blocks and filter blocks pinned in DRAM for SSTable lifetime. Typically <1% of SSTable size.
2. **Coalesced block fetches:** Adjacent block requests merged into a single byte-range GET.
3. **Speculative data block fetch:** For point reads near block boundaries, fetch both candidate blocks in one GET.
4. **SSTable-level GET budget:** Configurable max S3 GET count per read (default 8). Excess deferred with staleness flag.

### 11.5 S3 Byte-Range Read Protocol

For a point read missing all caches:

```
Step 1: Footer read (if not cached)
        GET Range: bytes=-80 → parse footer

Step 2: Bloom filter check (if not cached)
        GET Range: bytes={bloom_offset}-{bloom_end} → check record_id

Step 3: Index block read (if not cached)
        GET Range: bytes={index_offset}-{index_end} → binary search

Step 4: Data block read (if not cached)
        GET Range: bytes={block_offset}-{block_end}
        → Decompress, scan for key, cache in DRAM
```

**Optimization:** For small SSTables, footer + bloom + index are contiguous at end of file. A single ~200 KB byte-range GET fetches all metadata.

### 11.6 Parallel S3 GETs for L0

L0 SSTables overlap, so all must be checked:

```
1. Bloom filter check ALL L0 SSTables (in-memory, ~microseconds)
   → candidate list [sst-A, sst-C, sst-D]

2. Issue data block GETs for ALL candidates CONCURRENTLY
   → effective latency ≈ 1 × S3_latency

3. Merge by sequence number (highest wins)
```

### 11.7 NVMe Tier Prefetch

Range reads hitting the NVMe cache tier use readahead:

1. When a sequential scan pattern is detected (3+ consecutive block reads), prefetch next 4 blocks
2. Prefetched blocks inserted into DRAM window cache (not main cache) for quick eviction if scan abandoned
3. Prefetch disabled for point reads and short scans

---

## 12. Three-Tier Cache Architecture

All cache misses going directly to S3 (50-200ms) is unacceptable for read-heavy workloads.

| Tier | Latency | Content |
|------|---------|---------|
| DRAM | < 1 ms | Hot data blocks, bloom/ribbon filters, sparse indexes, deserialized KV pairs |
| Local NVMe | < 5 ms | Warm SSTable blocks evicted from DRAM |
| S3 | 50-200 ms | Cold/authoritative data |

**Cache key:** `(sstable_id, block_offset)`.

**Cache design principles:**
- **Logical cache:** Cache deserialized KV pairs, not raw bytes. No re-parsing on cache hit. Bypass OS page cache (`O_DIRECT`) for SSTable reads so compaction I/O doesn't evict hot user data.
- **Per-shard LRU:** Each CPU core owns its own LRU list (consistent with shard-per-core). Eviction granularity is individual data blocks (4 KB).
- **Continuity tracking:** Track which key ranges are complete in cache. If `(record_id, [a..z])` was fully read and cached, a miss for `(record_id, m)` means the key doesn't exist — skip S3 GET. Intervals tagged with manifest version; invalidated when compaction produces new manifest for affected key ranges.
- **Compaction-aware eviction:** After compaction invalidates old SSTables, asynchronously evict cached blocks from those SSTables. On eviction to NVMe, skip insertion if SSTable has been compacted.

**Scan-resistant admission policy (W-TinyLFU):**

1. **Window cache (1% of DRAM tier):** New entries admitted to small LRU unconditionally
2. **Frequency sketch (Count-Min Sketch):** Tracks access frequency, ~8 bytes per tracked key
3. **Main cache (99% of DRAM tier):** Segmented LRU (80% protected, 20% probation). Window evictions admitted to main only if frequency exceeds eviction candidate's frequency
4. **Reset:** Frequency sketch halved periodically to adapt to shifting patterns

The NVMe tier uses the same admission check — blocks evicted from DRAM written to NVMe only if they pass the frequency threshold.

---

## 13. Byte-Based Pagination

Pagination uses byte budgets rather than row counts. Clients specify a target page size in bytes (default 2MB) and an optional item limit.

**Adaptive Pagination:**
1. First request: estimate item count from cached average item size for the namespace
2. Read until byte budget met. If too few items fetched, issue additional reads.
3. Actual average item size stored in page token for subsequent pages
4. Server-side cache of average item size per namespace continuously updated

**SLO-Aware Early Return:** If accumulating items approaches the request's latency SLO (configured per namespace), stop early and return a partial page with a page token. If gRPC deadline is nearly exhausted, stop issuing further storage reads.

For cross-partition queries, the page token encodes per-partition cursors so each partition resumes independently. A metadata-only mode (`exclude_values`) allows listing keys without transferring value payloads.

---

## 14. Large Value Handling

### 14.1 Tiered Strategy

| Value Size | Storage Strategy |
|-----------|-----------------|
| < separation threshold | Inline in SSTables |
| separation threshold – 4 MB | Separated into blob objects |
| >= 4 MB | Chunked into 4 MB parts as separate S3 objects |

**Adaptive separation threshold:** Default 32 KB, adjusted per namespace:

1. **Write amplification tracking:** Compaction tracks effective write amplification ratio over a sliding 1-hour window
2. **Threshold adjustment:** If WA > 10x and avg value size > 8 KB, lower threshold (min 8 KB). If WA < 3x and blob-pointer-chase rate > 20% of reads, raise threshold (max 64 KB).
3. **Hysteresis:** Changes require 10 minutes persistence, move in 8 KB steps
4. **Per-namespace override:** Operators can pin to a fixed value

### 14.2 Value Separation (32 KB – 4 MB)

Values in this range cause significant write amplification during compaction. With value separation:

- Values >= 32 KB stored as separate S3 blob objects
- SSTable stores only a `(blob_object_id, offset, size)` pointer
- Compaction rewrites only keys + pointers, never value data
- Write amplification drops from 10-30x to ~1x for separated values

**Blob file format:**

```
┌──────────────────────────────────────┐
│ Header                               │
│   magic: 0x424C4F42 ("BLOB")        │
│   version: u16                       │
│   entry_count: u32                   │
├──────────────────────────────────────┤
│ Entry 0                              │
│   composite_key_len: varint          │
│   composite_key: bytes               │  ← stored for GC validation
│   value_len: varint                  │
│   value: bytes                       │
│   crc32: u32                         │
├──────────────────────────────────────┤
│ Entry 1 ...                          │
├──────────────────────────────────────┤
│ Footer                               │
│   index: [(key_hash, offset, len)]   │
│   entry_count: u32                   │
│   crc32: u32                         │
│   magic: 0x424C4F42                  │
└──────────────────────────────────────┘
```

**Blob pointer in SSTable:**

```
BlobPointer {
    blob_file_id: ULID,
    offset: u64,
    size: u32,
    value_crc32: u32,
}
```

**Blob GC:**
1. Sample blob files, compute live/dead ratio
2. If dead ratio > 50%, rewrite with live entries only
3. Track references via **blob reference log** — append-only S3 object recording refcount deltas per manifest version
4. GC process periodically materializes refcount map from last snapshot + recent deltas
5. Blobs with refcount = 0 confirmed zero against latest manifest before deletion

**Blob GC deletion protocol:**
1. Rewrite live entries from `B_old` to `B_new`, upload `B_new`
2. Record `(B_old, rewritten_at_manifest_version)` in blob GC queue
3. On next compaction touching SSTables referencing `B_old`: rewrite blob pointers to `B_new` (pointer migration)
4. After pointer migration: `B_old` enters deferred deletion — deleted only when `min_active_manifest_version > rewritten_at_manifest_version`
5. Safety fallback: 30-minute timeout for stuck readers

**In-flight read safety:** If a read encounters a 404 for a blob pointer, it re-reads the SSTable index entry from the current manifest for the updated blob pointer and retries once.

**Parallel value reads:** During range scans, value GETs issued concurrently from a thread pool (up to 32 concurrent GETs).

### 14.3 Chunking (>= 4 MB)

Values at or above 4 MB split into 4 MB chunks:

```
chunks/{namespace}/{chunk_group_id}/chunk-0000    (4 MB)
chunks/{namespace}/{chunk_group_id}/chunk-0001    (4 MB)
chunks/{namespace}/{chunk_group_id}/chunk-0002    (remainder)
```

SSTable stores `ChunkMetadata { chunk_group_id, chunk_count, total_size, content_type }`.

**Read:** Fetch all chunks in parallel (up to 32 concurrent GETs). Effective latency ≈ 1 round-trip.

**GC:** Background task lists chunk groups not referenced by any live manifest and deletes them. Grace period of 4 hours (covers worst-case flush times). Active flush registry markers prevent premature deletion of in-progress flushes.

---

## 15. Garbage Collection

### 15.1 What Needs GC

| Object Type | When Garbage | Method |
|-------------|-------------|--------|
| Old SSTables | After compaction removes from manifest | Reference tracking + deferred DELETE |
| Orphaned SSTables | Crash during flush | List S3 objects not in manifest, delete after 1 hour |
| Old manifests | After 100 newer manifests | Delete beyond retention count |
| Blob files | After live ratio drops below threshold | Rewrite live entries, delete old |
| Chunk groups | After referencing SSTable entry removed | Scan, check references, delete orphans |
| Incomplete multipart uploads | Crash during upload | S3 lifecycle rule (24 hours) |

### 15.2 SSTable GC Protocol

```
1. Each active reader holds a reference to the manifest version it started with
   (atomic counter per manifest version).

2. After compaction commits new manifest (removing input SSTable IDs):
   Record (sst_id, removed_in_manifest_version) in GC queue.

3. GC worker checks queue every 60 seconds:
   min_active_version = minimum manifest version referenced by any active reader
   For each entry where removed_in_manifest_version < min_active_version:
       S3 DELETE, remove from queue.

4. Safety fallback: 30-minute timeout for stuck readers.
```

**Long-running read protection:**
1. **Heartbeat extension:** Long-running reads heartbeat their manifest snapshot every 5 minutes. GC forced deletion only applies to non-heartbeated snapshots.
2. **Pre-deletion notification:** GC worker publishes `sstable_gc_pending` event. Active readers can heartbeat or checkpoint.
3. **Absolute ceiling:** `max_snapshot_ttl` (default 2 hours). Reads exceeding this are terminated with retriable error.

### 15.3 Orphan Detection

Run periodically (every 6 hours):

1. List all SSTable objects in S3 for the namespace
2. Load current manifest, collect all referenced SSTable IDs
3. For each S3 object not in the manifest and >= 1 hour old: delete
4. Same for chunk groups and blob files

---

## 16. Cluster Coordination

### 16.1 Design Philosophy

The coordination layer exists for one reason: protecting the unflushed window — the gap between when a write is ACK'd (after WAL + quorum replication) and when it reaches S3 (after memtable flush). S3 handles everything else.

### 16.2 Partition Ownership

Each partition has exactly one owner node for all reads and writes. Ownership determined by consistent hashing ring with virtual nodes. Virtual partitions (power-of-2 count, configured per namespace) are mapped to physical nodes via the ring.

### 16.3 User-Defined Partition Keys

Four strategies per namespace:

- **Simple** — record ID is partition key directly
- **Composite** — multiple fields via delimiter (e.g., tenant + region)
- **Prefix** — first N characters of record ID
- **Custom hash** — named hash function applied before partition assignment

Partition key schemas are immutable after namespace creation. Partition count must be a power of 2.

### 16.4 S3-Based Distributed Leases

Partition ownership tracked via versioned S3 lease keys using `If-None-Match: *` conditional writes. No external coordination service required.

**Lease key scheme:**
```
leases/partition-{id}/lease-{version:020d}.json
```

Highest lexicographic version = current lease. Contains: `{owner, epoch, expiration_time, previous_version}`.

**Acquisition protocol:**
1. List all lease keys, identify current (highest version)
2. If no lease or expired, compute `next_version = current_version + 1`
3. PUT with `If-None-Match: *`
4. On 412: re-list and retry
5. On success: record as `held_lease_version`

**Renewal protocol:**
1. Create new lease key with `next_version = held_lease_version + 1`
2. PUT with `If-None-Match: *`
3. On success, delete lease keys older than `current_version - 5`

30-second TTL, 10-second renewal intervals (three chances before expiry).

**Lease discovery by non-owners:** Propagated via gossip. Coordinators route based on gossip-advertised ownership. During convergence window after failover, stale routing returns `NOT_PARTITION_OWNER` — coordinator re-reads lease from S3 as fallback.

**Lease version cap:** Max gap of 100 between current and lowest surviving lease version. If exceeded, prioritize deleting old keys before creating new ones. If deletion impossible, extend logical TTL via gossip.

**Lease renewal resilience:**
1. **Adaptive renewal interval:** If S3 PUT p99 > 500ms, decrease interval to 5 seconds
2. **Lease extension on slow renewal:** Immediate follow-up renewal if PUT took > 5 seconds
3. **Grace period before yielding:** `LEASE_ENDANGERED` state — continue reads, pause write ACKs, retry aggressively. Yield only after 3 consecutive failures.
4. **Jittered renewal timing:** ±2 seconds across partitions

### 16.5 Gossip-Based Membership (SWIM)

Node discovery and failure detection use the SWIM protocol. Nodes discover each other via seed nodes (config or DNS) and exchange health/metadata over UDP gossip. Each node's gossip payload (~200 bytes) includes: ID, gRPC address, ring version, owned partitions, per-partition manifest versions, lifecycle status (JOINING, ACTIVE, LEAVING, DEAD). Failure detection ~2-3 seconds.

### 16.6 Metadata Cache with Gossip Invalidation

All S3 metadata (partition schemas, ring state, manifests, leases) cached in-memory. Gossip-based invalidation eliminates S3 polling during steady state. Only recurring S3 writes are lease renewals (one PUT per partition per 10 seconds).

### 16.7 WAL Replication

Before ACKing, the partition owner replicates WAL entries to W-1 followers. Followers store in a dedicated follower WAL (no memtable).

**Follower catchup protocol:**
1. Follower tracks last sequence number per partition
2. On gap detection: `CatchupRequest(partition_id, last_sequence_number)` to owner
3. Owner streams WAL entries from requested sequence. If already truncated: `CatchupFromManifest(manifest_version)`
4. Follower >1 full flush behind marked `CATCHING_UP`, excluded from quorum

Three consistency levels per namespace:
- **ONE** — owner-only, fastest, risk of data loss
- **QUORUM** — majority ACK (default), survives minority failures
- **ALL** — all replicas, strongest, slowest

Reads always go to partition owner (R=1). **Follower reads** available for flushed data via `allow_follower_read: bool` flag — consistent because flushed data is immutable in S3.

### 16.8 Failover

When a node dies (gossip detects in 2-3 seconds), surviving followers race to acquire the orphaned partition's S3 lease.

**Failover timeline:**
- Gossip failure detection: 2-3 seconds
- Lease acquisition + manifest fetch: ~1 second
- WAL reconciliation: 0-3 seconds (configurable `failover_reconciliation_timeout`)
- Write barrier: ~5 seconds (lease-expired path)
- **Total time to serve reads:** ~3-4 seconds
- **Total time to serve writes:** ~8-12 seconds

**WAL reconciliation:**
1. New owner reads local follower WAL, identifies sequence range beyond last flushed manifest
2. Broadcasts `WALReconcile(partition_id, sequence_range)` to all surviving followers
3. Each follower responds with entries the new owner is missing
4. New owner merges all entries (deduplicated by sequence number), replays into memtable
5. If no other followers reachable and local WAL has gaps: log as `PARTITION_DATA_LOSS`, emit metric, serve with available entries

**Configurable grace period:** New owner waits `failover_reconciliation_timeout` (default 3s, max 10s). Completes early if all followers respond. Logs specific missing sequence ranges as `PARTITION_WAL_GAP` events on timeout.

### 16.9 Rebalancing

When a node joins or leaves, only partitions on the affected ring segment move. S3 stores the authoritative ring state. Gossip propagates ring version changes.

---

## 17. Ordered Key Generation

The system generates 12-byte ordered keys: 8 bytes of millisecond timestamp, 2 bytes of node ID, and 2 bytes of per-node sequence. Monotonically increasing, naturally sorted by time. Supports ~65K keys per millisecond per node with up to 65,536 nodes. Node IDs assigned from a monotonic counter stored at S3 key `cluster/node-id-counter` using `If-None-Match: *`.

---

## 18. Tombstone-Based Deletes

Deletes are writes. Two granularities:
- Single tombstone for an entire record (`match_all`)
- Per-key tombstones with TTL + random jitter to prevent compaction storms

Background compaction garbage-collects expired tombstones.

---

## 19. API

Four operations, all scoped to a namespace and record ID. Defined as gRPC services.

### 19.1 PutItems — Write Items to a Record

```
PutItemsRequest {
  idempotency_token: IdempotencyToken
  namespace:         string
  id:                string          // record ID
  items:             List<Item>
}

PutItemsResponse {
  version:           OrderedKey      // system-generated version
}

IdempotencyToken {
  generation_time:   uint64          // client monotonic timestamp (ms)
  token:             bytes           // UUID v7 nonce (128-bit)
}
```

Rejects tokens with clock drift exceeding configurable threshold (default 5 seconds).

### 19.2 GetItems — Read Items from a Record

```
GetItemsRequest {
  namespace:         string
  id:                string
  predicate:         Predicate
  selection:         Selection
  signals:           Map<string, bytes>
}

GetItemsResponse {
  items:             List<Item>
  next_page_token:   optional<bytes>
}

Predicate {
  oneof {
    match_keys:      List<bytes>     // specific item keys
    match_range:     Range           // start/end with inclusive/exclusive
    match_all:       bool            // all items in the record
  }
}

Range {
  start_key:         bytes
  end_key:           bytes
  start_inclusive:    bool            // default true
  end_inclusive:      bool            // default false
}

Selection {
  page_size_bytes:   uint32          // byte budget (default 2MB)
  item_limit:        uint32          // max items (0 = unlimited)
  exclude_values:    bool            // metadata-only mode
  page_token:        optional<bytes>
}
```

### 19.3 DeleteItems — Delete Items from a Record

```
DeleteItemsRequest {
  idempotency_token: IdempotencyToken
  namespace:         string
  id:                string
  predicate:         Predicate
}

DeleteItemsResponse {
  version:           OrderedKey
}
```

Delete behavior by predicate:
- **match_all** — single record-level tombstone, constant latency
- **match_range** — single range tombstone covering `[start, end)`
- **match_keys** — per-item tombstones with TTL + random jitter

### 19.4 ScanItems — Streaming Read

Server-side streaming variant of GetItems:

```
ScanItemsRequest {
  namespace:         string
  id:                string
  predicate:         Predicate
  signals:           Map<string, bytes>
}

// Server streams:
ScanItemsResponse {
  items:             List<Item>      // batch per stream message
}
```

### 19.5 Idempotency

**Dedup retention window:**
1. **In-memory dedup (hot path):** Active + frozen memtables store tokens inline. Covers retries within seconds.
2. **Persisted dedup index:** Flushed into a dedicated **dedup block** in the SSTable — compact hash set of 128-bit token hashes (~16 bytes per token).
3. **Dedup block caching:** L0/L1 dedup blocks pinned in DRAM. L2/L3 loaded on demand.
4. **Retention TTL:** Tokens older than `idempotency_retention_ttl` (default 10 minutes) accepted unconditionally.
5. **Compaction cleanup:** Expired tokens dropped from dedup blocks during compaction.

### 19.6 Cross-Partition Queries

Coordinator fans out to relevant partition owners in parallel, merge-sorts, applies byte budget, returns unified response with composite page token.

**Partition pruning strategies:**
1. **Partition bloom filters:** ~100KB per partition, over contained record IDs. Reduces point query fan-out to typically 1 partition.
2. **Progressive fan-out with early termination:** Batches of 16 partitions. Remaining cancelled when byte budget satisfied.
3. **Partition affinity caching:** LRU cache of `record_id → partition_id` (100K entries).
4. **Fan-out budget:** Configurable `max_fan_out` per namespace (default 64).

### 19.7 Client-Server Signaling

**Server → Client signals** (delivered via handshake, refreshed periodically):
- Target/max latency SLOs, supported compression codecs, max page size, feature flags

**Client → Server signals** (sent with each request):
- Compression capability, chunking support flag, client version

---

## 20. Multi-Tenancy

Namespaces provide tenant isolation. Each namespace has its own partition key schema, S3 path prefix, partition set, manifest, and compaction lifecycle.

```
NamespaceConfig {
  name:                    string
  partition_key_strategy:  SIMPLE | COMPOSITE | PREFIX | CUSTOM_HASH
  partition_count:         uint32       // power of 2
  s3_path_prefix:          string

  // Storage configuration
  persistence: List<StorageLayer> {
    id:               string
    type:             S3 | CACHE
    config: {
      consistency_scope:   LOCAL | GLOBAL
      consistency_target:  READ_YOUR_WRITES | EVENTUAL
      default_ttl:         optional<duration>
    }
  }

  // Performance tuning
  memtable_size_threshold: uint64      // bytes, default 64MB
  compaction_strategy:     LEVELED
  bloom_filter_fp_rate:    float       // default 0.01
  default_page_size_bytes: uint32      // default 2MB
  max_page_size_bytes:     uint32      // default 8MB
  target_latency_slo:      duration    // e.g., 10ms for p99
  max_latency_slo:         duration    // e.g., 500ms

  // Replication
  write_consistency:       ONE | QUORUM | ALL
  replication_factor:      uint32      // default 3
}
```

Partition-related fields immutable after creation. Performance tuning fields updatable at runtime via signaling.

---

## 21. Change Data Capture (CDC)

### 21.1 Design Principle

Fire and forget. No delivery guarantees. No consumer tracking. No backpressure to writers. flushdb is a storage engine, not a message broker.

### 21.2 Emitter

A bounded ring buffer (default 64K events) with a background drain loop. Batches of up to 256 events dispatched on a 100ms timer. If buffer full, oldest events silently overwritten.

**Gap detection:** Every event carries a monotonically increasing `cdc_sequence_number` per partition. The emitter periodically (every 1 second) emits a `CDCCheckpoint` heartbeat containing:
- `dropped_count: uint64`
- `dropped_record_ids: List<string>` (from a bounded 10K-entry LRU set of affected record IDs)
- `dropped_set_overflow: bool`

Consumers can detect gaps, know how many events were lost, which records were affected (for targeted re-reads), and fall back to full partition re-read only when `dropped_set_overflow` is true.

### 21.3 Sink Model

Pluggable. Initial implementation: Kafka (async producer, no ack waiting). Events for the same record ID routed to the same Kafka partition for per-record ordering. Topic: `flushdb.{namespace}.changes`. Multiple sinks can be active simultaneously.

---

## 22. Recovery

On startup, a node:

1. Fetches the manifest from S3 (source of truth for flushed state)
2. Rebuilds in-memory indexes from manifest metadata
3. Replays local WAL entries with sequence numbers beyond the manifest's `last_flushed_sequence`
4. Resumes normal operation

Zero data loss for any ACK'd write, assuming WAL (or follower WAL) survived the crash.

---

## 23. S3 Object Layout

All persistent state organized under namespace-scoped S3 prefix with hash-based prefix sharding:

```
s3://{bucket}/{hash(object_id) % 128}/flushdb/{namespace}/
  ├── manifests/
  │   ├── manifest-00000000000000000001.json
  │   ├── manifest-00000000000000000002.json
  │   └── ...
  ├── sstables/
  │   ├── L0/
  │   │   ├── {ulid}.sst
  │   │   └── ...
  │   ├── L1/
  │   │   ├── {run-id}/frag-0000.sst
  │   │   ├── {run-id}/frag-0001.sst
  │   │   └── ...
  │   ├── L2/ ...
  │   └── L3/ ...
  ├── blobs/
  │   ├── {blob-id}.blob
  │   └── ...
  ├── chunks/
  │   ├── {chunk-id}.chunk
  │   └── ...
  ├── blob-refs/
  │   ├── blob-refcount-snapshot-{version}.json
  │   └── ...
  └── leases/
      ├── partition-{id}/
      │   ├── lease-00000000000000000001.json
      │   ├── lease-00000000000000000002.json
      │   └── ...
      └── ...
```

**S3 Prefix Sharding:** 128 prefixes yields up to 448K PUTs/s and 704K GETs/s before throttling.

**SSTable IDs:** ULIDs — sortable by creation time AND distribute uniformly across hash prefixes.

---

## 24. End-to-End Data Flow

### 24.1 Write Flow

```
Client PutItems(namespace, record_id, items, idempotency_token)
  │
  ├─► Coordinator routes to partition owner (via ring + lease)
  │
  ├─► Owner: Append WALEntry to local WAL (fsync)
  ├─► Owner: Replicate WALEntry to W-1 followers
  ├─► Owner: Insert into memtable (composite key sort order)
  ├─► Owner: Push to CDC ring buffer (async, non-blocking)
  ├─► Owner: ACK to client with system-generated OrderedKey version
  │
  └─► Background: When memtable exceeds threshold
        ├─► Freeze memtable, swap in new one
        ├─► Flush frozen memtable as SSTable to S3 (L0)
        ├─► Update manifest on S3
        ├─► Truncate WAL entries covered by this flush
        └─► Background compaction merges L0 → L1 → L2 → L3
```

### 24.2 Read Flow

```
Client GetItems(namespace, record_id, predicate, selection)
  │
  ├─► Coordinator routes to partition owner
  │
  ├─► Owner: Merge-read across layers (memtable → L0 → L1 → L2 → L3)
  │     ├─► Bloom filter check on record_id per SSTable
  │     ├─► Seek to (record_id, predicate.start_key) in each layer
  │     ├─► Merge-sort iterators by item_key
  │     ├─► Apply tombstone filtering
  │     └─► Accumulate until byte budget exhausted
  │
  └─► Return items + page_token (if more data)
```

---

## 25. Sequence Diagrams

### 25.1 Concurrent Flush + Compaction

```
Time ──────────────────────────────────────────────────────►

Flusher                          Compactor
   │                                │
   │  Read manifest v5              │  Read manifest v5
   │  (L0: [A,B,C,D])              │  (L0: [A,B,C,D])
   │                                │
   │  Build SSTable E               │  Begin merging [A,B,C,D] into L1
   │  Upload E to S3                │  ... (slow, reading from S3)
   │                                │
   │  CAS: write v6 (add E to L0)  │
   │  → SUCCESS                     │
   │  v6: L0=[A,B,C,D,E]           │
   │                                │
   │                                │  Finish merge, output [F,G] for L1
   │                                │  Upload F,G to S3
   │                                │
   │                                │  CAS: write v7 (expected prev=v5)
   │                                │  → FAIL (v6 exists)
   │                                │
   │                                │  Re-read v6. [A,B,C,D] still in L0.
   │                                │  Recompute: remove [A,B,C,D], add [F,G]
   │                                │  CAS: write v7
   │                                │  (L0=[E], L1=[F,G])
   │                                │  → SUCCESS
```

### 25.2 Zombie Writer Fencing

```
Time ──────────────────────────────────────────────────────►

Node A (original owner)           Node B (new owner)
   │                                │
   │  Owns partition P              │
   │  writer_epoch = 5              │
   │  Begins flush...               │
   │                                │
   │  ── Network partition ──       │
   │                                │  Detects A is dead (gossip)
   │                                │  Acquires lease for P (S3 CAS)
   │                                │  CAS: manifest with epoch = 6
   │                                │  → SUCCESS. B is now the writer.
   │                                │  Replays follower WAL, begins serving.
   │                                │
   │  Flush completes               │
   │  Upload SSTable to S3 (ok)     │
   │  Read manifest for CAS...      │
   │  Sees writer_epoch = 6         │
   │  6 > my epoch (5)              │
   │  ⚠ ZOMBIE. HALT.              │
   │  Abandon SSTable (orphan GC)   │
```

---

## 26. Key Metrics

### Storage Engine
- Write throughput (ops/sec, bytes/sec)
- Read latency (p50, p95, p99)
- Memtable flush frequency and duration
- Compaction throughput and space amplification
- SSTable cache hit rate

### Cluster
- Partitions owned/followed per node
- WAL replication latency to followers
- Failover time (gossip detection through serving resumed)
- Rebalance duration on node join/leave
- Gossip propagation delay
- Lease renewal success/failure rate
- Metadata cache hit rate and S3 fetch count

### CDC
- Events enqueued vs dropped (buffer overflow indicator)
- Sink send latency and error rate
- Buffer utilization percentage

---

## 27. Non-Goals

- flushdb is not a relational database. No SQL, no joins, no transactions spanning multiple records.
- flushdb does not provide exactly-once CDC delivery. Sinks are responsible for their own delivery semantics.
- flushdb does not support schema migration for partition keys. Schemas are immutable after namespace creation.
- flushdb does not manage external sink infrastructure (Kafka clusters, webhook endpoints, etc.).

---

## 28. Open Questions and Future Work

### 28.1 Manifest Per-Partition vs Per-Namespace

Current design: one manifest per namespace. All partitions share one manifest, and flushes/compactions contend on the same CAS.

**Alternative:** One manifest per partition. Zero contention between partitions. But: more S3 objects, more manifest reads on startup.

**Recommendation:** Start with per-namespace. If CAS contention becomes a bottleneck (>10 retries average), switch to per-partition.

### 28.2 Remote Stateless Compaction

Since all SSTables are on S3, compaction workers don't need local state:

1. Coordinator picks compaction job
2. Sends job descriptor to a stateless worker (Lambda, ECS task, etc.)
3. Worker downloads inputs from S3, merges, uploads outputs
4. Worker returns output metadata
5. Coordinator updates manifest via CAS

This decouples compaction throughput from serving node resources.

### 28.3 Deterministic Simulation Testing

The manifest CAS protocol, epoch fencing, and zombie writer detection are complex distributed protocols. A deterministic simulation framework (FoundationDB-style) that abstracts S3 calls, time, and randomness would allow injecting failures and reproducing any scenario from a seed. Essential before production deployment.
