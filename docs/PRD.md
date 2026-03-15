# flushdb — Product Requirements Document

## 1. Overview

flushdb is a distributed key-value database that uses S3 as its durable source of truth while maintaining fast local writes through an LSM-tree architecture. It draws from Cassandra's storage model and Netflix's KV Data Abstraction Layer patterns to provide a system where data durability is delegated to object storage, and the coordination layer exists only to protect in-flight writes that haven't yet reached S3.

---

## 2. Problem Statement

Traditional distributed KV stores (Cassandra, DynamoDB) couple storage durability with the database cluster itself — replication, disk management, and failure recovery are all internal concerns. This creates operational overhead: disk provisioning, replica repair, anti-entropy, and complex multi-node consistency protocols.

S3 offers 11 nines of durability, strong read-after-write consistency, and effectively unlimited capacity — but it has high latency (~50ms per request) and no support for point writes. flushdb bridges this gap: fast local writes with WAL-backed durability, background flushes to S3 for permanent storage, and a thin coordination layer that only protects the unflushed window.

---

## 3. Target Users

- Teams that need a KV store with S3-tier durability without managing disk replication
- Applications with write-heavy workloads that can tolerate eventual flush to object storage
- Multi-tenant platforms that need namespace-level isolation with per-tenant partitioning strategies
- Systems that need CDC event streams from their KV layer without running a separate change tracking system

---

## 4. Data Model

### 4.1 Two-Level Map

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

### 4.2 Composite Storage Key

Internally, the storage engine operates on a single composite key:

```
composite_key = record_id + separator + item_key
```

This composite key is what gets written to the WAL, stored in memtable, and persisted in SSTables. Because SSTables sort by composite key, all items for a given record are physically co-located and sorted by item key. This means:

- **Point lookup** of a single item: binary search for exact `(record_id, item_key)`
- **Range scan** within a record: seek to `(record_id, start_key)`, scan forward until `record_id` changes or `end_key` is reached
- **Full record read**: seek to `(record_id, MIN_KEY)`, scan until `record_id` changes

### 4.3 Supported Data Patterns

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

This means application developers choose a data pattern, not a database — flushdb handles the storage.

### 4.4 Why Sorted Values Matter

1. **Range queries are first-class** — `match_range(start, end)` resolves to a contiguous scan within a single partition. No scatter-gather, no secondary indexes. Time-series, pagination over event histories, prefix-based config lookups — all reduce to range scans on sorted bytes.

2. **Single tombstone for range deletes** — Deleting all items in a range writes one range tombstone marker rather than N individual tombstones. This directly addresses Cassandra's tombstone compaction problem.

3. **Predictable read amplification** — Because items within a record are co-located in SSTables, reading a record touches at most `L0_count + 1` SSTables (one per level after L0). Bloom filters on record IDs eliminate SSTables that don't contain the target record.

4. **Natural merge during compaction** — Items for the same record from different SSTables merge together during compaction, deduplicating updates and dropping expired tombstones in a single pass.

5. **Efficient byte-based pagination** — Sorted order means the page token is just the last item key seen. Resume by seeking to `(record_id, last_key + 1)`.

---

## 5. Core Architecture Decisions

### 5.1 S3 as Source of Truth

All durable state lives in S3. Local storage serves two purposes: the WAL (write-ahead log) for crash recovery and a three-tier cache for read performance. On crash recovery, the system rebuilds entirely from S3 manifests plus local WAL replay. This means nodes are stateless from a durability perspective — any node can recover any partition given access to S3.

#### Three-Tier Cache Architecture

All cache misses going directly to S3 (50-200ms) is unacceptable for read-heavy workloads. The storage engine maintains a three-tier cache hierarchy:

| Tier | Latency | Content |
|------|---------|---------|
| DRAM | < 1 ms | Hot data blocks, bloom/ribbon filters, sparse indexes |
| Local NVMe | < 5 ms | Warm SSTable blocks evicted from DRAM |
| S3 | 50-200 ms | Cold/authoritative data |

**Cache design principles:**
- **Logical cache:** Cache deserialized KV pairs, not raw bytes. No re-parsing on cache hit. Bypass OS page cache (`O_DIRECT`) for SSTable reads so compaction I/O doesn't evict hot user data.
- **Continuity tracking:** Track which key ranges are complete in cache. If `(record_id, [a..z])` was fully read from S3 and cached, a subsequent miss for `(record_id, m)` means the key doesn't exist — skip the S3 GET. Continuity intervals are tagged with the manifest version at the time they were populated. When compaction completes and produces a new manifest, all continuity intervals whose manifest version is older than the compaction's input manifest are invalidated for the affected key ranges (derived from the compaction's input SSTable key boundaries). This prevents stale negatives when tombstone expiration during compaction reveals previously-shadowed keys.
- **Row-granularity eviction:** A single eviction unit is one item entry, not an entire record.
- **Compaction-aware eviction:** After compaction invalidates old SSTables, asynchronously evict all cached blocks from those SSTables. On eviction to secondary cache, skip insertion if the SSTable has been compacted.

**Scan-resistant admission policy (W-TinyLFU):** A single full-record scan or bulk read must not evict hot data from the cache. The cache uses a Window-TinyLFU admission policy:

1. **Window cache (1% of DRAM tier):** New entries are admitted to a small LRU window unconditionally
2. **Frequency sketch (Count-Min Sketch):** Tracks access frequency for all keys seen, using ~8 bytes per tracked key
3. **Main cache (99% of DRAM tier):** Segmented LRU (80% protected, 20% probation). When a window entry is evicted, it is admitted to the main cache only if its frequency exceeds the frequency of the main cache's eviction candidate
4. **Reset:** The frequency sketch is halved periodically (every N accesses) to adapt to shifting access patterns

This ensures one-hit scan data stays in the small window and is evicted without polluting the main cache. Hot data that is accessed repeatedly accumulates frequency and is protected. The NVMe tier uses the same admission check — blocks evicted from DRAM are written to NVMe only if they pass the frequency threshold, preventing scan pollution of the SSD tier as well.

### 5.2 LSM-Tree Write Path

Writes follow the classic LSM-tree pattern: append to WAL, insert into in-memory memtable, ACK to client. When the memtable reaches a size threshold (default 64MB) or a time-based trigger fires (default 5 minutes, see Section 5.4), it is frozen, a new active memtable takes over, and a background process flushes the frozen memtable as an SSTable to S3. The WAL is truncated only after the corresponding SSTable is confirmed on S3.

#### Group Commit (WAL Batching)

Individual fsync + replication per write is prohibitively expensive. The write path uses group commit to amortize these costs:

1. **Buffering phase:** Incoming writes are appended to an in-memory WAL buffer and inserted into the memtable immediately. The write is not yet ACK'd
2. **Commit phase:** Every 200μs (configurable) or when the buffer reaches 256KB (whichever comes first), the buffer is fsynced as a single batch and replicated to followers as a single batch message
3. **ACK phase:** All writes in the committed batch are ACK'd to their respective clients simultaneously

This reduces fsync calls from N/sec (one per write) to ~5,000/sec (one per batch), and follower replication from N round-trips to one round-trip per batch. Under sustained load, hundreds of writes share a single fsync + replication round-trip, yielding 5-10x throughput improvement.

**Commit ordering and partial failure:** The group commit phases execute in strict order — fsync must complete before replication begins, and replication quorum must be met before any write in the batch is ACK'd. This means:

1. **Owner crashes after fsync, before replication:** The batch is in the owner's local WAL but no follower has it. On failover, these entries exist only on the dead node's disk. Since no ACK was sent to clients, no data loss from the client's perspective — clients will timeout and retry. However, if the dead node's disk is recoverable, the recovery process (Section 10) replays these entries. To prevent duplicate writes if clients retried to the new owner, idempotency tokens deduplicate the replayed entries against any retries the new owner already accepted.

2. **Owner crashes after partial replication (some followers ACK'd, quorum not met):** Same as case 1 — no client ACK was sent. Some followers have the batch in their follower WAL. On failover, the WAL reconciliation protocol (Section 6.9) collects these partial entries from surviving followers and merges them. Since clients were not ACK'd, these writes may or may not be visible after failover depending on reconciliation — this is acceptable because the client's retry will re-establish the write via idempotency dedup.

3. **Owner crashes after quorum replication, before ACK:** The batch is durable (owner WAL + follower WALs). Clients did not receive an ACK, so they will retry. The new owner recovers the batch via WAL reconciliation. Client retries are deduplicated via idempotency tokens. No data loss, no duplicates.

The key invariant: **a write is only considered durable when the client receives an ACK, and an ACK is only sent after fsync + quorum replication**. Partial failures before ACK are invisible to clients and resolved by retry + idempotency.

**Latency trade-off:** Group commit adds up to 200μs of buffering latency to each write. For latency-sensitive namespaces, the commit interval is configurable down to 50μs (trading throughput for latency). A `flush_immediate: bool` flag on PutItems bypasses batching entirely for individual writes that need minimum latency.

### 5.3 Shard-Per-Core Architecture

The storage engine pins each partition's data structures to a specific CPU core, eliminating lock contention from the hot path entirely.

**Design:**
- Each core owns its own: active memtable, frozen memtable list, WAL buffer, compaction state, LRU cache region
- Zero lock acquisitions on the write or read hot path — data is partitioned, not shared
- Cross-core communication via lock-free SPSC (single-producer single-consumer) queues
- Priority-based cooperative scheduling with preemption (see below)

**Partition-to-core mapping:** Virtual partitions (vnodes) are mapped to cores using `vnode_id % num_cores`. Cross-partition queries post tasks to target cores via SPSC queues.

**Priority-based task scheduling:** Each core runs a priority scheduler with three tiers:

| Priority | Tasks | Preemption |
|----------|-------|------------|
| P0 (critical) | Read requests, write path (memtable insert + WAL buffer append) | Immediate — preempts P1/P2 |
| P1 (normal) | Memtable flush, WAL group commit flush | Yields to P0 within 10μs |
| P2 (background) | Compaction, bloom/ribbon filter construction, blob GC | Yields to P0/P1 within 50μs |

Compaction tasks check a per-core preemption flag every 50μs. When a read or write arrives, the scheduler sets the flag, and the compaction task yields at its next check point. This bounds compaction-induced read latency to 50μs worst case, down from the 500μs cooperative yield interval. Compaction progress is not significantly impacted because its throughput is I/O-bound (S3 reads/writes), not CPU-bound — the yield points occur during CPU-intensive merge-sort phases that are a small fraction of total compaction time.

### 5.4 Memtable Structure

The memtable is a skip list sorted by composite key `(record_id, item_key)`, owned by a single core (no concurrent access from other cores). This provides:

- O(log N) inserts and lookups
- Zero-lock writes within the owning core's shard
- Natural iteration in sorted order for flush to SSTable

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

For range deletes, a special sentinel entry is written with `entry_type = RANGE_DELETE` and the start/end keys encoded in the value. During reads, the memtable checks for range tombstones that cover the target key.

The memtable tracks its approximate size in bytes and the timestamp of its first write. A memtable is frozen and swapped when **either** condition is met:

1. **Size threshold** (default 64MB) — the memtable has accumulated enough data for an efficient SSTable
2. **Time threshold** (default 5 minutes) — the memtable has been active too long, regardless of size

The time-based trigger bounds the unflushed window for low-throughput partitions. Without it, a partition writing 1KB/s would take ~18 hours to reach 64MB — during which the WAL grows unboundedly, follower WALs grow, and the data-at-risk window widens. The 5-minute default ensures WAL size stays bounded and failover replay is fast even for cold partitions.

**WAL size backpressure:** As a safety net against S3 outages preventing flushes, the system monitors WAL size per partition. If the WAL exceeds `max_wal_size` (default 256MB — 4x the memtable threshold), writes to that partition are stalled with `RESOURCE_EXHAUSTED` until a flush completes. This prevents local disk exhaustion during prolonged S3 unavailability.

The frozen memtable remains available for reads until its SSTable is flushed to S3.

### 5.5 Read Path

Reads follow a merge-read pattern across multiple layers, from newest to oldest:

```
1. Active memtable (newest writes)
2. Frozen memtable(s) (pending flush)
3. L0 SSTables (most recent flushes, may overlap)
4. L1 SSTables (non-overlapping)
5. L2 SSTables (non-overlapping)
6. L3 SSTables (non-overlapping)
```

**Point read** (`match_keys` with a single key):
1. Search active memtable, then frozen memtable — return immediately if found
2. Issue bloom/ribbon filter checks for all candidate SSTables concurrently (filters are in DRAM — effectively free)
3. For SSTables passing the filter, issue concurrent S3 byte-range GETs for index blocks (L0 SSTables checked in parallel, not sequentially)
4. Issue concurrent S3 byte-range GETs for target data blocks
5. Merge results by sequence number (newest write wins) — if it's a tombstone, return not-found

**Range read** (`match_range` within a record):
1. Open iterators on all layers that could contain items for this record ID (bloom filter eliminates non-matching SSTables)
2. Merge-sort iterators by item key, applying tombstone filtering
3. **Dual-buffer prefetch:** consume buffer A while async-filling buffer B from S3, then swap — hides S3 latency during sequential scans
4. Accumulate results until the byte-based page budget is exhausted or the range end is reached
5. Return results plus a page token encoding the last key emitted

**Async parallel S3 reads:** All S3 I/O on the read path uses async operations. Point reads across multiple SSTables issue concurrent byte-range GETs rather than sequential requests. This reduces multi-SSTable lookup latency from `N * 50-200ms` (serial) to `max(50-200ms)` (parallel). Range scans overlap data consumption with prefetch I/O.

**S3 GET reduction strategies:** Each S3 GET has both latency and dollar cost. The system minimizes GET count through:

1. **Persistent index block cache:** SSTable index blocks and filter blocks are pinned in DRAM for the lifetime of the SSTable. These are small (typically <1% of SSTable size) and eliminate the most common S3 GETs — without pinning, every read would issue at least one GET for the index block before it can locate the data block
2. **Coalesced block fetches:** When a read needs multiple data blocks from the same SSTable (common for range scans), adjacent block requests are coalesced into a single byte-range GET spanning the contiguous range. For example, blocks at offsets [100KB-104KB] and [104KB-108KB] become a single GET for [100KB-108KB]
3. **Speculative data block fetch:** For point reads, if the index block lookup identifies a single candidate data block, the system fetches it immediately. If the index identifies two adjacent candidate blocks (common near block boundaries), both are fetched in a single coalesced GET rather than two sequential GETs
4. **SSTable-level GET budget:** Each read operation has a configurable maximum S3 GET count (default 8). If the read path would exceed this budget (e.g., too many L0 SSTables passing bloom filters), lower-priority SSTables are deferred and the result is returned with a flag indicating potential staleness

**NVMe tier prefetch for sequential scans:** Range reads and full-record reads hitting the NVMe cache tier use readahead to hide per-block latency:

1. When a sequential scan pattern is detected (3+ consecutive block reads within the same SSTable), the system issues prefetch reads for the next 4 blocks ahead of the current read position
2. Prefetched blocks are inserted into the DRAM window cache (not the main cache) so they are evicted quickly if the scan is abandoned early
3. Prefetch is disabled for point reads and for scans that are expected to complete within 2 blocks (based on the byte budget and estimated item size)

**Full record read** (`match_all`):
- Same as range read with start = MIN_KEY, end = MAX_KEY for the record

### 5.6 Bloom Filters and Ribbon Filters

Each SSTable contains a bloom filter over the **record IDs** (not item keys) present in that SSTable. This allows the read path to skip entire SSTables that definitely don't contain any items for a given record.

- Default false positive rate: 1%
- Filter size: ~10 bits per record ID
- Stored as a separate section in the SSTable, loaded into memory when the SSTable is opened
- For L0 (overlapping SSTables), bloom filters are critical — without them, every L0 SSTable would need to be checked for every read

Bloom filters are not used for item-level filtering for typical records because items within a record are accessed via sorted range scans, not random point lookups.

**Large-record optimization:** For records with a high item count (exceeding a configurable threshold, default 10,000 items), record-level bloom filters cause excessive read amplification — every SSTable containing any item for the record passes the filter, even for point lookups of a single item. To mitigate this:

1. **Per-block min/max key fences:** Each data block in the SSTable stores the minimum and maximum composite key it contains. After the bloom filter identifies candidate SSTables, the sparse index's per-block key fences are checked to skip blocks that don't overlap the target item key range. This is effectively free since index blocks are cached in DRAM
2. **Prefix bloom filters for hot records:** When compaction detects a record exceeding the large-record threshold, it builds an additional prefix bloom filter keyed on `(record_id, item_key_prefix)` using the first 4 bytes of the item key. This narrows the candidate block set for point lookups within large records from O(blocks_per_record) to O(1) expected, at a cost of ~4 additional bits per item key in the filter section

**Ribbon filters:** For SSTables produced by compaction (not memtable flushes), the system uses ribbon filters instead of standard bloom filters. Ribbon filters achieve the same 1% FPR at ~7 bits/key instead of ~10 bits/key — 30% smaller. This means more filters fit in DRAM (fewer S3 GETs to load filter sections) and smaller filter sections for faster S3 range-GETs. Construction cost is higher (~230 bits/key temporary memory vs. ~75 for bloom) but this is a one-time cost at compaction time, not on the read path.

**Partitioned filters:** Filters are split into small filter blocks by key range. Individual filter blocks are cache-friendly (fit in one DRAM cache entry). For S3, this enables fetching only the relevant filter shard via a byte-range request rather than the entire filter.

### 5.7 Ordered Key Generation

The system generates 12-byte ordered keys: 8 bytes of millisecond timestamp, 2 bytes of node ID, and 2 bytes of per-node sequence. These keys are monotonically increasing and naturally sorted by time, supporting approximately 65K keys per millisecond per node with up to 65,536 nodes in the cluster. Clients receive these system-generated keys as version identifiers for every write. Node IDs are assigned from a monotonic counter stored as a well-known S3 key (`cluster/node-id-counter`) using `If-None-Match: *` on versioned keys to prevent duplicates.

### 5.8 Tombstone-Based Deletes

Deletes are writes. A delete operation writes a tombstone entry through the same write path as a put. Two granularities are supported: a single tombstone for an entire record (match_all), or individual per-key tombstones each with a TTL plus random jitter to prevent compaction storms from synchronized expirations. Background compaction is responsible for garbage-collecting expired tombstones.

### 5.9 Leveled Compaction with Incremental SSTable Runs

SSTables are organized into levels. L0 tolerates up to 4 overlapping files (recent flushes). L1 holds up to 10 non-overlapping SSTables (640MB total), L2 up to 100 (6.4GB), and L3 is unbounded. When a level exceeds its threshold, a background merge-sort compacts entries into the next level, deduplicating keys and dropping expired tombstones. New SSTables are uploaded to S3 and the manifest is atomically updated before old files are deleted.

**Write stalling when L0 is full:** If flushes outpace compaction, unbounded L0 growth degrades every read (each read must check all L0 SSTables). The system applies backpressure from L0 to the write path:

1. **L0 soft limit (4 files):** Compaction is triggered. Writes proceed at full speed
2. **L0 slow limit (8 files):** Write throughput is artificially throttled. Each write sleeps for `(l0_count - soft_limit) * 1ms` before ACKing, giving compaction time to drain L0. This progressive delay avoids a hard cliff
3. **L0 hard limit (12 files):** Writes are fully stalled — the write path blocks until at least one L0 compaction completes. The client receives a backpressure signal (gRPC `RESOURCE_EXHAUSTED` with a `retry-after` hint)

These thresholds are configurable per namespace. The `l0_stall_count` metric tracks how often each threshold is hit.

#### SSTable Runs and Fragment-Based Compaction

Standard leveled compaction requires temporary S3 storage equal to the total input size — both input and output SSTables coexist during the compaction window, doubling S3 storage cost.

**SSTable runs** solve this: instead of writing one monolithic SSTable per compaction output, the system writes a sequence of smaller, non-overlapping **fragments** (~1 GB each). During compaction, each input fragment is deleted as soon as its data is confirmed written to the output fragment. Maximum temporary overhead = `2 * fragment_size` instead of `2 * total_run_size`.

**Trivial move optimization:** When an L(N) fragment's key range does not overlap with any fragment in L(N+1), the fragment is "moved" to L(N+1) by updating the manifest only — no data is read from or written to S3. This is a metadata-only operation that completes in the time of a single manifest CAS. For workloads with time-ordered keys (e.g., event streams), the majority of compactions are trivial moves because older data at L(N+1) has non-overlapping key ranges with newer L(N) flushes.

**Partial range compaction:** When an L(N) fragment overlaps with only a subset of L(N+1) fragments, only the overlapping fragments participate in the merge. Non-overlapping L(N+1) fragments are left in place. This avoids re-reading and re-uploading cold data that hasn't changed, reducing compaction I/O from `O(total_level_size)` to `O(overlapping_range_size)`. The manifest tracks per-fragment key boundaries to enable precise overlap detection.

S3 layout for runs:
```
sstables/L1/{run-id}/frag-0000.sst
sstables/L1/{run-id}/frag-0001.sst
sstables/L1/{run-id}/frag-0002.sst
```

The manifest tracks `run_id` + list of fragment IDs per SSTable run. On compaction completion, input fragments are deleted incrementally and the manifest fragment list is updated.

**Space Amplification Goal (SAG):** Configurable parameter (1.0–2.0, default 1.75). When the second-largest tier reaches half the size of the largest tier, a cross-tier compaction is triggered, making the largest tier non-overlapping. This bounds worst-case space amplification.

### 5.10 Byte-Based Pagination and Adaptive Tuning

Pagination uses byte budgets rather than row counts. Clients specify a target page size in bytes (default 2MB) and an optional item limit. This provides predictable memory and network usage per page regardless of value sizes. For cross-partition queries, the page token encodes per-partition cursors so each partition resumes independently. A metadata-only mode (exclude_values) allows listing keys without transferring value payloads.

**Adaptive Pagination** — The storage engine doesn't natively paginate by bytes; it reads items one at a time. To minimize read amplification:

1. On the first request, the server estimates how many items fit in the byte budget using a cached average item size for the namespace
2. Items are read until the byte budget is met. If too few items were fetched, additional reads are issued. If too many, excess items are discarded and a page token is generated
3. The actual average item size observed is stored in the page token, so subsequent pages use a better estimate
4. The server-side cache of average item size per namespace is continuously updated from recent queries

**SLO-Aware Early Return** — If the server detects that accumulating items is approaching the request's latency SLO (configured per namespace), it stops early and returns a partial page with a page token. This ensures clients get predictable progress even on records with thousands of small items. If the client is a gRPC caller with a deadline, the server avoids issuing further storage reads when the deadline is nearly exhausted.

### 5.11 Value Separation and Large Value Handling

The system uses a tiered strategy for values based on size, with an adaptive separation threshold:

| Value Size | Storage Strategy |
|-----------|-----------------|
| < separation threshold | Inline in SSTables |
| separation threshold – 4 MB | Separated into blob objects (value separation) |
| >= 4 MB | Chunked into 4 MB parts as separate S3 objects |

**Adaptive separation threshold:** The default separation threshold is 32 KB, but it is adjusted per namespace based on observed workload characteristics:

1. **Write amplification tracking:** The compaction subsystem tracks the effective write amplification ratio (total bytes written to S3 / total bytes received from clients) over a sliding 1-hour window
2. **Threshold adjustment:** If write amplification exceeds 10x and the average value size is above 8 KB, the threshold is lowered (minimum 8 KB) to separate more values and reduce compaction rewrite cost. If write amplification is below 3x and the read path's blob-pointer-chase rate is above 20% of reads, the threshold is raised (maximum 64 KB) to reduce indirection overhead
3. **Hysteresis:** Threshold changes require the triggering condition to persist for 10 minutes before taking effect, and the threshold moves in steps of 8 KB to prevent oscillation
4. **Per-namespace override:** Operators can pin the threshold to a fixed value via namespace configuration, disabling adaptive behavior

#### Value Separation (32 KB – 4 MB)

Values in this range cause significant write amplification during compaction — every compaction re-reads and re-writes all values even when only keys need reordering. With value separation:

- Values >= 32 KB are stored as separate S3 blob objects
- The SSTable stores only a `(blob_object_id, offset, size)` pointer alongside the key
- Compaction rewrites only keys + pointers, never value data
- Write amplification drops from 10-30x to ~1x for separated values

**Blob object format:**
1. Sequential `(key, value)` pairs (key stored for GC validation)
2. Meta block with file properties
3. Footer with offsets and checksums

**Garbage collection:** Sample blob objects for discard ratio. If dead bytes > 50%, rewrite the blob object copying only live values. Track which blob objects are referenced by which SSTables during compaction.

**Blob GC and read coordination:** Blob GC must not delete a blob object while an in-flight read holds a pointer to it. The system uses manifest-tracked reference counting:

**Blob reference tracking:** Rather than storing a full `blob_id → refcount` map inside the manifest (which would bloat every manifest version), blob references are tracked via a separate **blob reference log** — an append-only S3 object that records refcount deltas:

```
blob-refs/{namespace}/blob-reflog-{version:020d}.json
```

Each manifest version records only the *delta* in the manifest body: `blob_ref_deltas: List<{blob_id, delta: +1|-1}>`. The full materialized refcount state is computed by the blob GC process, not carried in every manifest.

**Refcount materialization:** The blob GC process periodically (default every 10 minutes) reads all manifest versions since its last checkpoint, applies the deltas to build the current `blob_id → refcount` map, and writes a **blob refcount snapshot** to S3:

```
blob-refs/{namespace}/blob-refcount-snapshot-{version}.json
```

This snapshot is the authoritative refcount state. Subsequent GC runs only need to read manifests newer than the snapshot, keeping the work incremental. The snapshot is typically small even for large namespaces because it only contains blobs with `refcount > 0` — zero-refcount blobs are deleted and removed from the snapshot.

**Manifest impact:** Each manifest version carries only the delta list, not the full map. A typical compaction touches 10-50 blobs, so the delta list is a few hundred bytes — negligible compared to the SSTable metadata already in the manifest.

**Deletion protocol:**
1. The GC process materializes the current refcount map from its last snapshot + recent manifest deltas
2. Identifies blobs with `refcount = 0`
3. Before deleting, re-reads the latest manifest to apply any in-flight deltas that may have incremented the refcount since step 1
4. Deletes confirmed zero-refcount blobs from S3. If the delete fails, it is retried on the next GC cycle — orphaned blobs waste space but do not cause correctness issues
5. Writes a new refcount snapshot excluding the deleted blobs

**In-flight read safety:** A read that resolves a blob pointer is safe as long as the blob exists in S3. Since blobs are only deleted after the refcount reaches 0 (meaning no SSTable references the blob) *and* a re-confirmation check against the latest manifest, the window for a 404 is limited to the gap between re-confirmation and the DELETE. As a safety net, if a read encounters a 404 for a blob pointer, it re-reads the SSTable index entry from the current manifest to obtain the updated blob pointer and retries once. This fallback handles the rare race condition without relying on arbitrary time-based grace periods.

**Parallel value reads:** During range scans, keys are read sequentially from the SSTable but value GETs are issued concurrently from a thread pool (up to 32 concurrent GETs). Effective latency = max(individual GET latency), not sum.

#### Large Value Chunking (>= 4 MB)

Values at or above 4 MB are split into 4 MB chunks stored as separate S3 objects. A single S3 GET can efficiently fetch up to ~4 MB in one round-trip; below that, the overhead of managing multiple chunk objects exceeds the cost of a single blob read. The SSTable entry retains only the key and metadata (chunk count, content type). On read, chunks are fetched in parallel from S3 and reassembled.

---

## 6. Cluster Coordination

### 6.1 Design Philosophy

The coordination layer exists for one reason: protecting the unflushed window — the gap between when a write is ACK'd to the client (after WAL + quorum replication) and when it reaches S3 (after memtable flush). S3 handles everything else. This means the cluster layer is thin by design.

### 6.2 Partition Ownership

Each partition has exactly one owner node that handles all reads and writes for that partition. Ownership is determined by a consistent hashing ring with virtual nodes (vnodes). Virtual partitions (power-of-2 count, configured per namespace) are mapped to physical nodes via the ring, with each physical node owning multiple vnodes for even distribution.

### 6.3 User-Defined Partition Keys

Users define partition key schemas per namespace at creation time. Four strategies are supported:

- **Simple** — the record ID is the partition key directly
- **Composite** — multiple fields extracted from the record ID via a delimiter (e.g., tenant + region)
- **Prefix** — the first N characters of the record ID (useful for time-series data)
- **Custom hash** — a named hash function applied to the ID before partition assignment

Partition key schemas are immutable after namespace creation. The partition count must be a power of 2 and should be significantly larger than the expected node count to allow rebalancing without repartitioning. All records sharing a partition key land on the same node and share the same WAL, memtable, and SSTables.

### 6.4 S3-Based Distributed Leases

Partition ownership is tracked via versioned S3 lease keys using `If-None-Match: *` conditional writes. No external coordination service (ZooKeeper, etcd) is required.

**Lease key scheme:** Instead of overwriting a single lease object (which S3 cannot do atomically), each lease is a new immutable key with a monotonically increasing version:

```
leases/partition-{id}/lease-{version:020d}.json
```

The lease with the highest lexicographic version is the current lease. Each lease object contains: `{owner, epoch, expiration_time, previous_version}`.

**Acquisition protocol:**
1. List all lease keys for the partition, identify the current (highest version) lease
2. If no lease exists or the current lease has expired, compute `next_version = current_version + 1`
3. PUT `lease-{next_version}.json` with `If-None-Match: *` — this atomically fails if another node already created this version
4. On `412 Precondition Failed`, re-list and retry from step 1
5. On success, the node is now the owner. It records the new lease version locally as its `held_lease_version`

**Renewal protocol:**
1. The owner creates a new lease key with `next_version = held_lease_version + 1`, containing a refreshed expiration time and the same owner/epoch
2. PUT with `If-None-Match: *` — if another node has already written a higher version (indicating a takeover), the renewal fails and the owner detects it has been fenced
3. On success, the owner deletes lease keys older than `current_version - 5` to bound key growth (retaining a few for crash-recovery auditing)

This guarantees exactly one owner per partition at any time: two nodes racing for the same version will have exactly one succeed, and S3's strong read-after-write consistency ensures the winner is immediately visible.

**Lease discovery by non-owner nodes:** Non-owner nodes (coordinators routing requests) do not poll S3 lease keys to find the current owner. Instead, lease ownership is propagated via gossip (Section 6.6). When a node acquires or renews a lease, it updates its gossip payload with `{partition_id → lease_version, expiration_time}`. Coordinators route requests based on gossip-advertised ownership, which converges within 1-2 gossip rounds (~1-2 seconds). During the convergence window after a failover, a coordinator may route to the old owner — the old owner rejects the request with a `NOT_PARTITION_OWNER` error containing the last known lease version, and the coordinator re-reads the lease from S3 as a fallback, caches the result, and retries. This fallback path is only exercised during the brief gossip convergence window after ownership changes.

**Lease version cap:** To prevent unbounded lease key accumulation during S3 brownouts where renewals succeed but old-key deletions fail, the system enforces a maximum lease version gap. If the current lease version exceeds the lowest surviving lease version by more than 100 (configurable via `max_lease_version_gap`), the renewal process prioritizes deleting old keys before creating new ones. If deletion is impossible (S3 DELETE failures), the owner extends the existing lease's logical TTL via gossip (advertising a later expiration to peers) without creating a new S3 key, buying time until deletions can proceed. This caps lease key accumulation at ~100 keys per partition worst case.

Leases have a 30-second TTL with 10-second renewal intervals (three chances before expiry).

**Lease renewal resilience:** S3 latency spikes (500ms+) during brownouts could cause renewal failures that trigger false failovers. To mitigate this:

1. **Adaptive renewal interval:** The base renewal interval is 10 seconds, but the system tracks S3 PUT latency p99 over a sliding 60-second window. If p99 exceeds 500ms, the renewal interval decreases to 5 seconds (6 chances before expiry instead of 3)
2. **Lease extension on slow renewal:** If a renewal PUT succeeds but took longer than 5 seconds, the owner immediately issues a follow-up renewal to reset the TTL clock, since a significant portion of the lease window was consumed by the slow PUT
3. **Grace period before yielding:** When a lease renewal fails, the owner does not immediately stop serving. It enters a `LEASE_ENDANGERED` state where it continues serving reads (which are safe — the owner still has the most recent data) but pauses new write ACKs. It retries renewal aggressively (every 2 seconds). Only after 3 consecutive renewal failures does the owner yield the partition and stop serving entirely
4. **Jittered renewal timing:** Renewal times are jittered ±2 seconds across partitions to avoid thundering-herd renewal storms during S3 brownouts

### 6.5 Epoch-Based Fencing

The versioned lease protocol (Section 6.4) handles lease acquisition but doesn't address **zombie writers** — nodes that lose their lease but continue writing SSTables and updating manifests due to network delays or GC pauses.

**Writer epoch:** The manifest contains a monotonically increasing `writer_epoch: u64`. A new partition owner increments the epoch on startup and writes an "epoch SST" with a higher ID than any existing SST. Running writers detect fencing when they encounter a higher epoch during manifest CAS and halt immediately.

**Compactor epoch:** A separate `compactor_epoch: u64` prevents two compactors from both committing results for the same compaction job. The compactor increments its epoch before starting work and verifies it hasn't been superseded before committing the output manifest.

**Startup protocol:**
1. Acquire the partition lease (Section 6.4) — this establishes ownership
2. Read current manifest, extract current `writer_epoch`
3. Increment epoch: `new_epoch = writer_epoch + 1`
4. Write epoch SST with ID higher than any existing SST
5. Write new manifest with `writer_epoch = new_epoch` via CAS
6. If CAS fails, re-read the latest manifest. If the `writer_epoch` in the latest manifest is >= `new_epoch`, another writer won — back off and retry from step 2. If the epoch is still the old value, a concurrent flush from the old owner landed between steps 2 and 5 — merge the old owner's flush into the new manifest and retry the CAS with the merged state
7. **Write barrier:** After the CAS succeeds, the new owner must wait before serving writes to ensure the old owner has stopped. The barrier duration depends on how the ownership transition occurred:
   - **Lease expired (old owner presumed dead):** The old owner's lease already expired before acquisition, meaning it has been unable to renew for at least one full TTL. The barrier is reduced to `old_owner_max_inflight_time` (default 5 seconds) — enough for any in-flight S3 uploads from the old owner to complete or timeout. The new owner's epoch in the manifest will fence any late-arriving manifest CAS from the old owner
   - **Lease preempted (forced takeover during rebalance):** The old owner may still be alive and actively writing. The barrier is `lease_ttl / 2` (default 15 seconds) to ensure the old owner detects fencing via its next renewal attempt or gossip
   - During the barrier, the new owner serves reads from the flushed manifest state plus any WAL entries recovered during reconciliation
8. After the barrier, begin serving writes. Any manifest CAS from the old owner will now fail because the new owner's manifest contains a higher `writer_epoch`

**Old owner detection:** The old owner detects fencing when any of its operations encounter a higher epoch — manifest CAS failure, lease renewal failure, or a gossip message advertising a new owner for its partition. On detection, the old owner immediately stops all writes, drains in-flight S3 uploads (allowing them to complete but not committing their manifest entries), and yields the partition.

### 6.6 Gossip-Based Membership (SWIM)

Node discovery and failure detection use the SWIM protocol — zero external infrastructure. Nodes discover each other via seed nodes (config or DNS) and exchange health and metadata over UDP gossip. Each node's gossip payload (~200 bytes) includes its ID, gRPC address, ring version, owned partitions, per-partition manifest versions, and lifecycle status (JOINING, ACTIVE, LEAVING, DEAD). Failure detection takes approximately 2-3 seconds via the ping, indirect-ping, suspect, dead protocol.

### 6.7 Metadata Cache with Gossip Invalidation

All S3 metadata (partition schemas, ring state, partition manifests, lease records) is cached in-memory on each node. Gossip-based invalidation eliminates S3 polling entirely during steady state. Partition schemas are immutable and loaded once. Ring state is refreshed from S3 only when a peer's gossip advertises a newer ring version. Partition manifests are invalidated lazily — the owner bumps a version counter in gossip after flushing, and other nodes only fetch from S3 when they actually need that partition's data. The only recurring S3 writes during steady state are lease renewals (one PUT per partition per 10 seconds).

### 6.8 WAL Replication

Before ACKing a write, the partition owner replicates the WAL entry to W-1 follower nodes (where W is the write quorum). Followers store entries in a dedicated follower WAL — they do not maintain a memtable. The follower WAL exists purely as a backup. If the owner fails, the follower that wins the lease replays its follower WAL into a fresh memtable to recover the unflushed window.

**Follower catchup protocol:** A follower that falls behind (network partition, restart, or temporary overload) must catch up before it can participate in quorum writes or be eligible for failover. The catchup mechanism works as follows:

1. Each follower tracks the last sequence number it received per partition
2. When a follower reconnects or detects a gap (missing sequence numbers in incoming replication), it sends a `CatchupRequest(partition_id, last_sequence_number)` to the current owner
3. The owner streams all WAL entries from the requested sequence number forward. If those entries have already been truncated from the owner's WAL (because the corresponding SSTable was flushed to S3), the owner responds with a `CatchupFromManifest(manifest_version)` directive — the follower discards its stale follower WAL and resets its baseline to the flushed manifest
4. A follower that is more than one full memtable flush behind is marked `CATCHING_UP` in gossip and is excluded from quorum counting until it reaches the owner's current sequence number

This ensures followers are never silently incomplete. The owner tracks follower replication high-water marks and only counts a follower toward quorum if it is within the current unflushed window.

Three consistency levels are supported per namespace:

- **ONE** — owner-only write, fastest, risk of data loss if owner dies before flush
- **QUORUM** — majority of replicas must ACK (default), survives minority failures
- **ALL** — all replicas must ACK, strongest durability, slowest writes

Reads always go to the partition owner (R=1), which always has the latest data.

**Follower reads for availability:** Followers can serve read requests for data that has been flushed to S3 (i.e., data covered by the partition manifest). Since flushed data is immutable and authoritative in S3, follower reads of flushed state are consistent — not stale. Only the unflushed window (memtable + pending WAL entries) is exclusive to the owner. Clients opt into follower reads via a `allow_follower_read: bool` flag on GetItems. The response includes a `served_by: OWNER | FOLLOWER` field and a `freshness_lag: duration` indicating the age of the latest flushed manifest the follower has cached. This provides a read availability path when the owner is under heavy write/compaction load, without sacrificing consistency for flushed data.

### 6.9 Failover

When a node dies, gossip detects it within 2-3 seconds. Surviving follower nodes race to acquire the orphaned partition's S3 lease via CAS. The winner fetches the partition manifest from S3, replays its follower WAL for entries beyond the last flushed state, rebuilds the memtable, and begins serving.

**Failover timeline:**
- Gossip failure detection: 2-3 seconds
- Lease acquisition + manifest fetch: ~1 second
- WAL reconciliation: 0-3 seconds (configurable `failover_reconciliation_timeout`, default 3s)
- Write barrier: ~5 seconds (lease-expired path — the old owner is dead, so the short barrier applies)
- **Total time to serve reads:** ~3-4 seconds (reads are served after manifest fetch + WAL replay, before the write barrier completes)
- **Total time to serve writes:** ~8-12 seconds

**Follower WAL completeness verification:** The winning follower's WAL may be incomplete — with QUORUM writes (W-1 followers), not every follower receives every entry. Before serving, the new owner must reconcile:

1. The new owner reads its local follower WAL and identifies the range of sequence numbers it holds beyond the last flushed manifest
2. It broadcasts a `WALReconcile(partition_id, sequence_range)` request to all other surviving followers for that partition
3. Each follower responds with any WAL entries it holds that the new owner is missing
4. The new owner merges all received entries (deduplicated by sequence number), replays the complete set into its memtable, and only then begins serving
5. If no other followers are reachable and the local follower WAL has gaps (detected via non-contiguous sequence numbers), the new owner logs the gap as a data loss event, emits a `PARTITION_DATA_LOSS` metric, and begins serving with the entries it has

This reconciliation adds ~500ms to failover but ensures the new owner has the most complete view of the unflushed window possible across all surviving followers.

**Configurable failover grace period:** To reduce the risk of serving with an incomplete WAL, the new owner waits for a configurable `failover_reconciliation_timeout` (default 3 seconds, max 10 seconds) before declaring reconciliation complete. During this window:

1. The new owner broadcasts `WALReconcile` to all known followers and waits for responses
2. As followers respond, the new owner merges their entries and tracks which sequence number ranges are now covered
3. If all expected followers respond before the timeout, reconciliation completes early
4. If the timeout expires with gaps still present, the new owner logs the specific missing sequence ranges as `PARTITION_WAL_GAP` events (not just a boolean data loss flag), begins serving, and emits a `wal_gap_sequences` metric with the count of missing entries

Operators can tune this per namespace: latency-sensitive namespaces use a shorter timeout (accepting higher data loss risk), while durability-sensitive namespaces use a longer timeout. Setting `failover_reconciliation_timeout = 0` restores the fast-failover behavior with no waiting.

### 6.10 Rebalancing

When a node joins or leaves, only partitions on the affected ring segment move. S3 stores the authoritative ring state. Gossip propagates ring version changes so all nodes know when to refresh their cached view. Because partition count is much larger than node count, rebalancing transfers a proportional fraction of partitions without requiring data repartitioning.

---

## 7. API

Four operations, all scoped to a namespace and record ID. The API is defined as gRPC services.

### 7.1 PutItems — Write Items to a Record

Upsert operation. Inserts new items or updates existing items in the sorted map.

```
PutItemsRequest {
  idempotency_token: IdempotencyToken
  namespace:         string
  id:                string          // record ID
  items:             List<Item>      // one or more items to write
}

PutItemsResponse {
  version:           OrderedKey      // system-generated version for this write
}

IdempotencyToken {
  generation_time:   uint64          // client monotonic timestamp (ms)
  token:             bytes           // UUID v7 nonce (128-bit)
}
```

The system rejects tokens with clock drift exceeding a configurable threshold (default 5 seconds) to prevent silent write discard (timestamp far in past) or immutable doomstones (timestamp far in future).

### 7.2 GetItems — Read Items from a Record

```
GetItemsRequest {
  namespace:         string
  id:                string
  predicate:         Predicate       // which items to match
  selection:         Selection       // pagination and filtering
  signals:           Map<string, bytes>  // client capability flags
}

GetItemsResponse {
  items:             List<Item>
  next_page_token:   optional<bytes> // present if more pages remain
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
  page_size_bytes:   uint32          // byte budget per page (default 2MB)
  item_limit:        uint32          // max total items across all pages (0 = unlimited)
  exclude_values:    bool            // metadata-only mode — keys without payloads
  page_token:        optional<bytes> // resume from previous page
}
```

### 7.3 DeleteItems — Delete Items from a Record

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

- **match_all** — single record-level tombstone, constant latency regardless of item count
- **match_range** — single range tombstone covering `[start, end)`
- **match_keys** — per-item tombstones with TTL + random jitter (avoids compaction storms)

### 7.4 ScanItems — Streaming Read

Server-side streaming variant of GetItems. Instead of paginated request-response, the server streams items to the client as they are read from storage. Useful for bulk reads where the client wants all items without managing page tokens.

```
ScanItemsRequest {
  namespace:         string
  id:                string
  predicate:         Predicate
  signals:           Map<string, bytes>
}

// Server streams:
ScanItemsResponse {
  items:             List<Item>      // batch of items per stream message
}
```

### 7.5 Predicates

Three query filters, all operating on the **item key** within a record:

- **match_keys** — specific set of item keys (multi-get)
- **match_range** — contiguous range of item keys with inclusive/exclusive bounds
- **match_all** — all items in a record

Because item keys are sorted, `match_range` resolves to a contiguous scan — no random I/O within the record.

### 7.6 Idempotency

Write and delete operations require an idempotency token composed of a client monotonic timestamp and a UUID v7 nonce. The system rejects tokens with excessive clock drift. This allows safe retries and hedged requests without duplicate writes.

The server uses the token for two purposes:
1. **Deduplication** — repeated requests with the same token are idempotent
2. **Ordering** — in last-write-wins resolution, the `generation_time` determines which write survives. This ensures correct ordering even with hedged requests or retries.

**Dedup retention window:** Idempotency tokens are retained for a bounded window to prevent unbounded storage growth while still covering the realistic retry period:

1. **In-memory dedup (hot path):** The active memtable and frozen memtable(s) store idempotency tokens inline with each entry. Duplicate detection for recent writes is a memtable lookup — effectively free. This covers the common case: retries arriving within seconds of the original write
2. **Persisted dedup index:** When a memtable is flushed to an SSTable, the idempotency tokens are written into a dedicated **dedup block** in the SSTable (separate from the data blocks). This block is a compact hash set of `(token_hash_128bit)` values — ~16 bytes per token, no values stored. For a 64MB memtable with 100K entries, the dedup block is ~1.6MB
3. **Dedup block caching:** Dedup blocks for L0 and L1 SSTables are pinned in DRAM (they are small relative to data blocks). For L2/L3, dedup blocks are loaded on demand during duplicate checks
4. **Retention TTL:** Tokens older than `idempotency_retention_ttl` (default 10 minutes, configurable per namespace) are not checked during dedup. The dedup check first compares the token's `generation_time` against `now - retention_ttl` — if the token is older than the window, it is accepted unconditionally. This bounds the number of SSTables that need dedup block lookups to those flushed within the retention window
5. **Compaction cleanup:** When compaction merges SSTables, tokens older than `idempotency_retention_ttl` are dropped from the dedup block entirely. The dedup block shrinks over time as old tokens expire

The 10-minute default covers: client retries (typically <30 seconds), hedged requests (immediate), and failover replay (WAL reconciliation completes within `failover_reconciliation_timeout`, default 3 seconds). Operators can increase the TTL for namespaces with long-lived client retry policies.

### 7.7 Cross-Partition Queries

When a query spans multiple partitions (e.g., `match_all` on a record whose partition key maps to multiple partitions, or `match_range` crossing partition boundaries), the coordinator fans out to relevant partition owners in parallel, merge-sorts results by item key, applies the byte-based page budget, and returns a unified response with a composite page token encoding per-partition cursors.

**Partition pruning:** Hash-based partitioning prevents range-based pruning, so the coordinator uses the following strategies to minimize fan-out:

1. **Partition bloom filters:** Each partition maintains a lightweight in-memory bloom filter (configurable, default 100KB per partition) over the record IDs it contains. Before fan-out, the coordinator checks the bloom filter for each candidate partition and skips partitions that definitely don't contain the target record. For point queries by record ID, this reduces fan-out from all partitions to typically 1 partition
2. **Progressive fan-out with early termination:** For `match_all` or `match_range` queries, the coordinator does not fan out to all partitions simultaneously. It issues requests in batches (default batch size: 16 partitions). If the byte budget is satisfied before all batches complete, remaining batches are cancelled. The page token records which partitions were not yet queried so subsequent pages resume from where the previous page stopped
3. **Partition affinity caching:** The coordinator caches a mapping of `record_id → partition_id` for recently accessed records (LRU, 100K entries). Subsequent queries for the same record skip the fan-out entirely and go directly to the known partition. Cache entries are invalidated on rebalance events
4. **Fan-out budget:** A configurable `max_fan_out` per namespace (default 64) caps the number of concurrent partition queries. If the query would exceed this, the coordinator returns an error rather than executing a scatter-gather storm that would degrade cluster-wide latency

### 7.8 Client-Server Signaling

Signaling enables dynamic configuration exchange between client and server without redeployment.

**Server → Client signals** (delivered via handshake on client initialization, refreshed periodically):
- Target and maximum latency SLOs per namespace
- Supported compression codecs
- Maximum page size
- Feature flags (chunking support, etc.)

**Client → Server signals** (sent with each request in the `signals` map):
- Client compression capability and preferred codec
- Chunking support flag
- Client version (for backward compatibility)

This allows the client to dynamically adjust timeouts, hedging policies, and serialization behavior without code changes.

---

## 8. Multi-Tenancy

Namespaces provide tenant isolation. Each namespace has its own partition key schema, S3 path prefix, partition set, manifest, and compaction lifecycle. Namespaces are fully independent — operations on one namespace cannot affect another.

### 8.1 Namespace Configuration

```
NamespaceConfig {
  name:                    string
  partition_key_strategy:  SIMPLE | COMPOSITE | PREFIX | CUSTOM_HASH
  partition_count:         uint32       // power of 2
  s3_path_prefix:          string       // e.g., "flushdb/namespaces/{name}/"

  // Storage configuration
  persistence: List<StorageLayer> {
    id:               string           // e.g., "PRIMARY", "CACHE"
    type:             S3 | CACHE       // durable vs ephemeral
    config: {
      consistency_scope:   LOCAL | GLOBAL
      consistency_target:  READ_YOUR_WRITES | EVENTUAL
      default_ttl:         optional<duration>
    }
  }

  // Performance tuning
  memtable_size_threshold: uint64      // bytes, default 64MB
  compaction_strategy:     LEVELED     // only leveled for now
  bloom_filter_fp_rate:    float       // default 0.01
  default_page_size_bytes: uint32      // default 2MB
  max_page_size_bytes:     uint32      // default 8MB
  target_latency_slo:      duration    // e.g., 10ms for p99
  max_latency_slo:         duration    // e.g., 500ms absolute cutoff

  // Replication
  write_consistency:       ONE | QUORUM | ALL
  replication_factor:      uint32      // default 3
}
```

Namespace configuration is immutable after creation for partition-related fields (strategy, count). Performance tuning fields can be updated at runtime and propagated via signaling.

---

## 9. Change Data Capture (CDC)

### 9.1 Design Principle

Fire and forget. No delivery guarantees. No consumer tracking. No backpressure to writers.

Every write is already a self-contained event in the WAL. CDC captures these events at write time and pushes them to user-configured external sinks asynchronously. flushdb does not manage consumers, offsets, or redelivery — that is the sink's responsibility.

### 9.2 Emitter

A bounded ring buffer (default 64K events) with a background drain loop. Batches of up to 256 events are dispatched to all registered sinks on a 100ms timer. If the buffer is full, oldest events are silently overwritten. The write path is never blocked by CDC.

**Gap detection:** Every CDC event carries a monotonically increasing `cdc_sequence_number: uint64` per partition. When the ring buffer overwrites events, a gap appears in the sequence. Consumers detect discontinuities by tracking the last sequence number they processed. Additionally, the emitter periodically (every 1 second) emits a `CDCCheckpoint` heartbeat event containing the current sequence number and the number of events dropped since the last checkpoint.

**Dropped record tracking:** Alongside the ring buffer, the emitter maintains a bounded set (capped at 10K entries, LRU-evicted) of `record_id` values whose CDC events were overwritten before being drained. When an event is overwritten, its `record_id` is inserted into this set. The `CDCCheckpoint` heartbeat includes:
- `dropped_count: uint64` — number of events lost since the last checkpoint
- `dropped_record_ids: List<string>` — the record IDs from the dropped set, flushed and reset on each checkpoint emission
- `dropped_set_overflow: bool` — true if more than 10K distinct record IDs were dropped (the set overflowed and the list is incomplete)

This allows consumers to:
1. Detect that events were lost (sequence gap)
2. Know how many events were lost (checkpoint drop count)
3. Know *which records* were affected and issue targeted re-reads from S3 for just those records, rather than a full partition scan
4. Fall back to a full partition re-read only when `dropped_set_overflow` is true (a rare case indicating sustained overload)

### 9.3 Sink Model

Sinks are pluggable. The initial implementation supports Kafka (async producer, no ack waiting). Events for the same record ID are routed to the same Kafka partition to preserve per-record ordering. Topic naming follows the pattern `flushdb.{namespace}.changes`. Multiple sinks can be active simultaneously.

### 9.4 Rationale

flushdb is a storage engine, not a message broker. Adding at-least-once delivery to CDC would require persisting offsets on the write path, retry queues, backpressure mechanics, and dead letter storage — all of which Kafka already provides natively. flushdb just needs to get events to the sink fast.

---

## 10. Recovery

On startup, a node:

1. Fetches the manifest from S3 (source of truth for flushed state)
2. Rebuilds in-memory indexes from manifest metadata
3. Replays local WAL entries that were ACK'd but not yet flushed (sequence numbers beyond the manifest's last recorded timestamp)
4. Resumes normal operation

This protocol ensures zero data loss for any write that was ACK'd to a client, assuming the WAL (or follower WAL) survived the crash.

---

## 11. SSTable Format

A custom binary format. Entries are stored by composite key `(record_id, item_key)`, so all items for a record are contiguous and sorted:

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
│ Footer                           │
│   dedup_block_offset: uint64     │
│   bloom_filter_offset: uint64    │
│   index_block_offset: uint64     │
│   entry_count: uint64            │
│   min_key: composite_key         │
│   max_key: composite_key         │
│   crc32: uint32                  │
│   magic: 0x464C4442              │
└──────────────────────────────────┘
```

Key properties:
- **Sorted by composite key** — enables binary search via the sparse index and contiguous range scans within a record
- **Bloom/ribbon filter on record IDs** — eliminates SSTables that don't contain the target record before any I/O (ribbon filters used for compaction-produced SSTables, 30% smaller than bloom)
- **S3 byte-range reads** — individual data blocks can be fetched without downloading the entire file (footer and index block are read first, then targeted blocks)
- **Compression per block** — each data block is independently compressed, allowing random access without decompressing the entire file

---

## 12. S3 Object Layout

All persistent state is organized under a namespace-scoped S3 prefix with hash-based prefix sharding to avoid S3 rate limits:

```
s3://{bucket}/{hash(object_id) % 128}/flushdb/{namespace}/
  ├── manifests/
  │   ├── manifest-00000000000000000001.json   # zero-padded 20-digit versioned manifests
  │   ├── manifest-00000000000000000002.json
  │   └── ...
  ├── sstables/
  │   ├── L0/
  │   │   ├── {ulid}.sst                       # ULIDs: sortable + uniformly distributed
  │   │   └── ...
  │   ├── L1/
  │   │   ├── {run-id}/frag-0000.sst           # SSTable run fragments (~1 GB each)
  │   │   ├── {run-id}/frag-0001.sst
  │   │   └── ...
  │   ├── L2/
  │   │   └── ...
  │   └── L3/
  │       └── ...
  ├── blobs/
  │   ├── {blob-id}.blob               # value-separated large values (32 KB – 1 MB)
  │   └── ...
  ├── chunks/
  │   ├── {chunk-id}.chunk             # large value chunks (>= 1 MB)
  │   └── ...
  ├── blob-refs/
  │   ├── blob-refcount-snapshot-{version}.json  # materialized refcounts
  │   └── ...
  └── leases/
      ├── partition-{id}/
      │   ├── lease-00000000000000000001.json  # versioned lease keys
      │   ├── lease-00000000000000000002.json  # highest version = current
      │   └── ...
      └── ...
```

#### S3 Prefix Sharding

S3 rate limits are per-prefix: 3,500 PUT/s and 5,500 GET/s. Without sharding, all L0 SSTables under a single prefix would hit throttling under heavy flush load. Hash-based prefix sharding distributes objects across 128 prefixes, yielding up to 448K PUTs/s and 704K GETs/s before throttling.

#### Manifest CAS Protocol

S3 supports conditional writes via `If-None-Match: *` (PUT succeeds only if the key does not already exist). The manifest CAS protocol uses monotonically increasing zero-padded 20-digit IDs (e.g., `manifest-00000000000000000042.json`). The "current" manifest is always the one with the highest lexicographic ID.

**Write protocol:**
1. Writer reads the current latest manifest (e.g., `manifest-00000000000000000042.json`)
2. Writer computes the next ID: `manifest-00000000000000000043.json`
3. Writer issues a PUT with `If-None-Match: *` — this atomically fails if another writer already created manifest-43
4. On `412 Precondition Failed`, the writer re-reads the latest manifest and retries from step 1

This eliminates the TOCTOU race inherent in read-then-write schemes. Two concurrent writers both targeting manifest-43 will have exactly one succeed and one receive a 412. No data is silently overwritten.

The manifest contains `writer_epoch` and `compactor_epoch` fields for fencing (see Section 6.5). Old manifests are retained for rollback (subject to the compaction policy below).

#### Manifest Compaction

The manifest grows with every flush and compaction — each creates a new versioned manifest key. For busy namespaces, this can reach thousands of manifest versions containing increasingly large SSTable metadata (run IDs, fragment lists, key boundaries, blob reference maps).

**Manifest snapshots:** Every 100 manifest versions (configurable via `manifest_snapshot_interval`), the system writes a self-contained **snapshot manifest** that includes the full materialized state (all SSTable metadata, blob refcounts, epochs) rather than just the delta from the previous version. The snapshot is tagged with a `snapshot: true` field in its JSON body. On startup or failover, the node reads only the latest snapshot manifest and applies any subsequent non-snapshot manifests on top of it — avoiding the need to read and merge hundreds of incremental manifests.

**Old manifest pruning:** Manifest versions older than the second-most-recent snapshot are eligible for deletion. The system retains two snapshots (not one) to allow rollback if the latest snapshot is discovered to be corrupt. A background task deletes pruning-eligible manifests in batches of 50, rate-limited to avoid S3 DELETE throttling. Pruning is best-effort — orphaned old manifests waste negligible space and do not affect correctness.

**Manifest size budget:** If a single manifest exceeds `max_manifest_size` (default 16MB), the system logs a `MANIFEST_SIZE_WARNING` and forces a snapshot on the next write. This acts as a safety net against unbounded growth in pathological cases (e.g., thousands of tiny SSTables from rapid low-volume flushes).

#### SSTable Naming with ULIDs

SSTable IDs use ULIDs (Universally Unique Lexicographically Sortable Identifiers). ULIDs are sortable by creation time AND distribute uniformly across hash-based prefixes due to their random component, naturally avoiding prefix hotspots.

---

## 13. Non-Goals

- flushdb is not a relational database. No SQL, no joins, no transactions spanning multiple records.
- flushdb does not provide exactly-once CDC delivery. Sinks are responsible for their own delivery semantics.
- flushdb does not support schema migration for partition keys. Schemas are immutable after namespace creation.
- flushdb does not manage external sink infrastructure (Kafka clusters, webhook endpoints, etc.).

---

## 14. WAL Format

The write-ahead log is an append-only file on local disk. Each entry is self-contained:

```
WALEntry {
  sequence_number:   uint64           // monotonically increasing
  entry_type:        PUT | DELETE | RANGE_DELETE
  namespace:         string
  record_id:         string
  item_key:          bytes
  item_value:        bytes            // empty for deletes
  item_metadata:     bytes
  idempotency_token: IdempotencyToken
  crc32:             uint32           // integrity check
}
```

WAL properties:
- Each entry is length-prefixed and CRC-protected for crash recovery
- The WAL is fsynced before ACKing to the client
- WAL is per-partition (all records in a partition share one WAL)
- Truncation occurs only after the corresponding SSTable is confirmed on S3
- On recovery, entries with sequence numbers beyond the last flushed manifest version are replayed into a fresh memtable

---

## 15. End-to-End Data Flow

### 15.1 Write Flow

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

### 15.2 Read Flow

```
Client GetItems(namespace, record_id, predicate, selection)
  │
  ├─► Coordinator routes to partition owner
  │
  ├─► Owner: Merge-read across layers (memtable → L0 → L1 → L2 → L3)
  │     ├─► Bloom filter check on record_id per SSTable
  │     ├─► Seek to (record_id, predicate.start_key) in each layer
  │     ├─► Merge-sort iterators by item_key
  │     ├─► Apply tombstone filtering (skip deleted items)
  │     └─► Accumulate until byte budget exhausted
  │
  └─► Return items + page_token (if more data)
```

---

## 16. Key Metrics

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
