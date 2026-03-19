# Design Decisions

This page explains the major design choices behind flushdb and the research that influenced them. It is written for engineers evaluating the system or contributing to it, and focuses on the reasoning behind each decision rather than the implementation details.

---

## Why S3 as Source of Truth

Traditional distributed key-value stores couple durability with the cluster itself. Replication, disk management, failure recovery, anti-entropy, and consistency protocols are all internal concerns. This creates significant operational overhead: disk provisioning across nodes, replica repair after failures, and complex multi-node consistency protocols that must be tested against every possible failure mode.

Amazon S3 offers a different set of trade-offs. It provides eleven nines of durability (99.999999999%), strong read-after-write consistency (since December 2020), effectively unlimited capacity, and zero operational overhead for storage management. The downside is latency: approximately 50ms per request, and no support for point writes (you write entire objects, not individual bytes).

flushdb accepts this trade-off by separating the write path from durable storage. Writes go to a local write-ahead log and in-memory memtable for sub-millisecond acknowledgment. A background flush pipeline asynchronously moves data to S3 as immutable SSTable files. This decouples storage durability from compute: nodes become stateless from a durability perspective and are fully replaceable. If a node dies, a replacement fetches the current manifest from S3 and replays any WAL entries that had not yet been flushed.

The coordination layer exists solely to protect the "unflushed window" -- the gap between when a write is acknowledged to the client and when it reaches S3.

---

## Why LSM-Tree Architecture

Log-structured merge-trees are write-optimized data structures: writes append sequentially to a write-ahead log and insert into an in-memory sorted structure (the memtable), while background compaction merges and sorts data at rest. This aligns naturally with the S3 storage model for several reasons:

- **SSTables are immutable.** Once written to S3, an SSTable file is never modified. This matches S3's object model perfectly -- you write an object once and read it many times.
- **Compaction produces new objects.** Merging SSTables creates new S3 objects and removes old ones via manifest updates. There is no in-place mutation.
- **Strong read-after-write consistency.** A newly flushed SSTable is immediately readable from S3 by any node, without any replication delay or cache invalidation protocol.
- **Sequential I/O patterns.** Both flush and compaction produce large sequential writes, which maximize S3 throughput. Reads use byte-range requests to fetch individual data blocks without downloading entire files.

The leveled compaction strategy (L0 through L3 with a 10x size ratio per level) bounds space amplification while keeping write amplification predictable. L0 SSTables may overlap in key range; L1 and below are non-overlapping, enabling efficient binary search during reads.

---

## Why Shard-Per-Core

Inspired by ScyllaDB and the Seastar framework, flushdb uses a shard-per-core architecture where each CPU core owns its partition's data structures outright. There are no shared mutable structures on the hot path.

The traditional approach to concurrent access -- shared data structures protected by mutexes -- creates contention under high throughput. Even well-optimized lock-free data structures incur cache-line bouncing across cores. Under load, this produces unpredictable latency spikes that are difficult to diagnose and impossible to eliminate.

Shard-per-core eliminates this entirely:

- Each partition's memtable, WAL buffer, compaction state, and cache region are owned by exactly one core.
- No lock acquisitions occur on the read or write hot path. Inserts are plain pointer writes; reads are direct traversals.
- Cross-core communication (for queries that need data from another partition) uses lock-free single-producer single-consumer (SPSC) queues.
- A priority-based cooperative scheduler ensures that reads and writes preempt background tasks like compaction within bounded time (50 microseconds worst case).

The result is predictable latency under all load conditions, because there are no lock contention spikes to absorb.

---

## Why Epoch-Based Fencing

Inspired by SlateDB, epoch-based fencing solves the zombie writer problem in distributed systems with S3-based state.

The scenario: Node A owns a partition and begins flushing a memtable to S3. During the flush, Node A's lease expires. Node B acquires the lease and begins accepting writes. Node A's flush completes, and it attempts to update the manifest -- if it succeeds, it could overwrite Node B's state and cause data loss for writes that Node B accepted.

Epoch-based fencing prevents this with a simple invariant: every writer carries a monotonically increasing epoch number, and the manifest rejects writes from outdated epochs. When Node B acquires the lease, it increments the epoch in the manifest via a compare-and-swap operation. When Node A later tries to update the manifest, it sees that the epoch has advanced beyond its own and halts immediately, abandoning its SSTable as an orphan for garbage collection to clean up.

This approach is simple to reason about, requires no external coordination, and makes the correctness argument straightforward: a write is only accepted if the writer's epoch matches the manifest's epoch.

---

## Why SSTable Run Fragments

Inspired by ScyllaDB's Incremental Compaction Strategy (ICS), flushdb produces compaction output as sequences of smaller, non-overlapping fragments rather than monolithic SSTable files.

Monolithic compaction has a storage cost problem: during a compaction that merges N gigabytes of input SSTables into N gigabytes of output, both input and output exist simultaneously on S3. For a large compaction, this doubles the storage cost for the duration of the operation.

Run fragments solve this by splitting compaction output into smaller pieces (approximately 64MB each). As each output fragment is written, the input fragments whose key ranges are fully covered can be immediately deleted. The maximum temporary storage overhead drops from the total input size to approximately twice the fragment size.

Fragments within a run are logically one SSTable for read purposes. A trivial move optimization further reduces compaction cost: when a fragment's key range does not overlap with any fragment in the next level, it is "moved" by a manifest-only update with no S3 I/O at all. For time-ordered key patterns, the majority of compactions reduce to trivial moves.

---

## Why Manifest CAS via S3 Conditional Writes

Inspired by SlateDB, flushdb uses S3's own conditional write support for atomic manifest updates, eliminating the need for any external coordination service.

Many distributed systems rely on ZooKeeper, etcd, or Consul for coordinating state changes. Each of these is an additional operational dependency with its own failure modes, capacity planning requirements, and upgrade procedures. Since flushdb already depends on S3 for all durable state, using S3 for coordination as well removes an entire category of operational concerns.

S3 conditional writes (the `If-None-Match: *` header, available since August 2024) reject a PutObject request if the target key already exists. flushdb exploits this by giving each manifest version a monotonically increasing numeric ID. To update the manifest, a writer reads the current version N, computes the new state, and writes it as version N+1 with the conditional header. If two writers race, exactly one succeeds and the other receives an HTTP 412 response, prompting it to re-read, re-validate, and retry.

This provides optimistic concurrency control with no external dependencies beyond S3 itself.

---

## Why W-TinyLFU Cache Admission

Standard LRU caches are vulnerable to scan pollution. A single large range scan can evict all frequently accessed data, causing a burst of cache misses for the hot working set. In a system where cache misses cost 50-200ms (the S3 round-trip), this is unacceptable.

W-TinyLFU (Window Tiny Least Frequently Used), as implemented in the Caffeine caching library, addresses this with frequency-based admission. The cache is split into a small window region (1% of capacity) and a main region (99% of capacity). New entries are admitted unconditionally to the window region. When an entry is evicted from the window, it is only promoted to the main region if its access frequency -- tracked by a compact Count-Min Sketch -- exceeds that of the main region's eviction candidate.

This means a one-time range scan fills the window region but never displaces hot data in the main region. The frequency sketch is periodically halved to adapt to shifting access patterns, preventing stale frequency counts from permanently blocking new hot keys.

---

## Why Byte-Based Pagination

Row-count pagination (e.g., "give me the next 100 items") works poorly when item sizes vary dramatically. A page of 100 items might be 100KB of small metadata entries or 100MB of large documents. This unpredictability makes it difficult to set meaningful latency SLOs, size gRPC response buffers, or provide consistent client experience.

Byte-based pagination solves this by letting the client specify a target page size in bytes (default 2MB). The server accumulates items until the byte budget is exhausted or the range end is reached, then returns the results with a page token encoding the last key emitted. Response sizes are predictable, latency is controllable, and SLO-aware early return can stop issuing storage reads when the request's deadline is approaching.

For workloads where the average item size is unknown, the server maintains a running average per namespace and uses it to estimate how many items to prefetch for the first page.

---

## Research Influences

The following table summarizes the external systems and papers that informed specific design choices in flushdb.

| Design | Source | Key Insight |
|--------|--------|-------------|
| Epoch-based fencing | SlateDB | Monotonic epochs prevent zombie writer corruption without external coordination |
| Manifest CAS | SlateDB | S3 conditional writes eliminate the need for ZooKeeper or etcd |
| Shard-per-core | ScyllaDB / Seastar | Partitioned data structures outperform shared ones for throughput and tail latency |
| SSTable run fragments | ScyllaDB ICS | Fragment-based compaction reduces temporary storage overhead during merges |
| Three-tier cache | RocksDB / SlateDB | DRAM, NVMe, and S3 tiers hide object storage latency for hot and warm data |
| Value separation | WiscKey / Titan / Pebble | Storing large values separately from SSTables reduces write amplification |
| S3 prefix sharding | RisingWave | Hash-based prefixes distribute requests across S3 partitions to avoid rate limits |
| W-TinyLFU | Caffeine | Frequency-based admission policy resists scan pollution in caches |
| Ribbon filters | RocksDB | 30% smaller than bloom filters at the same false positive rate |
| Delete-only compaction | Pebble | Drop SSTables fully covered by range tombstones without reading their contents |
| Dirty segment tracking | ScyllaDB | Safe WAL truncation when segments contain entries from multiple memtable generations |
| Parallel S3 GETs | RocksDB MultiGet | Concurrent block fetches across overlapping L0 SSTables hide per-GET latency |
| Streaming multipart upload | RisingWave | Overlap SSTable construction with S3 upload to reduce flush latency |

---

## Non-Goals

flushdb makes deliberate choices about what it does not do. These are not missing features -- they are scope boundaries that keep the system simple, correct, and focused.

- **No SQL, joins, or multi-record transactions.** flushdb is a key-value store with a two-level sorted map data model. All operations are scoped to a single record within a single namespace. Cross-record atomicity is out of scope.
- **No exactly-once CDC delivery.** Change data capture is fire-and-forget. The emitter provides gap detection via sequence numbers and checkpoint heartbeats, but delivery guarantees are the responsibility of the downstream sink.
- **No schema migration for partition keys.** Partition key strategies are immutable after namespace creation. This avoids the complexity of online repartitioning, which would require coordinating data movement across all nodes while maintaining availability.
- **Not a message broker.** CDC exists to notify downstream systems of changes, not to provide durable ordered message delivery. The ring buffer has a fixed capacity and silently drops old events when full.
- **No management of external sink infrastructure.** flushdb emits change events but does not provision, monitor, or manage the downstream systems that consume them. Sink health and scaling are external concerns.
