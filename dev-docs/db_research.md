# Database Research: Designs to Incorporate into flushdb

Research from ScyllaDB, RocksDB, SlateDB, WiscKey, Neon, TiKV, Pebble, DynamoDB, and FoundationDB — mapped to flushdb's S3-backed LSM architecture.

---

## CRITICAL: Missing Correctness Guarantees

### 1. Epoch-Based Fencing (SlateDB)

**Problem:** Your S3 ETag-based CAS handles lease acquisition but doesn't address zombie writers — nodes that lose their lease but continue writing SSTables and updating manifests.

**Solution:**
- A monotonically increasing `writer_epoch` stored in the manifest
- New writers increment the epoch on startup and write it to an "epoch SST" with a higher ID than any existing SST
- Running writers detect fencing when they encounter a higher epoch and halt
- Same mechanism for compactors (`compactor_epoch`) to prevent two compactors from both committing results

**Action:** Add `writer_epoch: u64` and `compactor_epoch: u64` to manifest. On startup, read current epoch, increment, write "epoch SST", then proceed.

---

### 2. Manifest CAS Protocol for S3 (SlateDB)

**Problem:** PRD says "manifest updates are atomic" but S3 doesn't support native CAS.

**Solution (SlateDB):**
1. Write new versioned manifest with a monotonically increasing zero-padded 20-digit ID (e.g., `manifest-00000000000000000042.json`)
2. The "current" manifest is always the one with the highest lexicographic ID
3. Competing writers detect conflicts by checking if their expected previous manifest ID is still the latest
4. Old manifests retained for rollback (already in PRD)

**Action:** Change manifest naming from `manifest-{version}.json` to `manifest-{zero_padded_20_digit_version}.json`. Reader always picks highest ID. Writer CAS by checking the ID hasn't changed since last read.

---

## HIGH PRIORITY: Significant Performance Gains

### 3. Shard-Per-Core Architecture (ScyllaDB/Seastar)

**Problem:** Concurrent skip list requires fine-grained locking on writes.

**Design:**
- Pin each partition's data structures (memtable, WAL segment, SSTable cache, bloom filters) to a specific CPU core
- Zero lock acquisitions in the hot path — data is partitioned, not shared
- Cross-core communication via lock-free SPSC (single-producer single-consumer) queues
- Cooperative preemption: yield every ~500 microseconds during CPU-intensive operations (compaction, bloom filter construction)

**Implementation:**
- Map virtual partitions (vnodes) to cores using consistent hash → `vnode_id % num_cores`
- Each core owns its own: active memtable, frozen memtable list, WAL buffer, compaction state, LRU cache region
- Cross-partition queries post tasks to target cores via SPSC queues

---

### 4. Incremental Compaction Strategy / SSTable Runs (ScyllaDB ICS)

**Problem:** Leveled compaction requires temporary space = input size. For S3: you pay per GB-hour, so you're paying for both input and output during the entire compaction window.

**Design:**
- An "SSTable run" is a sequence of smaller, non-overlapping **fragments** (~1 GB each) instead of one monolithic SSTable
- During compaction, delete each input fragment as soon as its data is confirmed written to the output fragment
- Max temporary overhead = `2 * fragment_size` instead of `2 * total_run_size`

**S3 Layout Change:**
```
# Instead of:
sstables/L1/{sstable-id}.sst

# Use run fragments:
sstables/L1/{run-id}/frag-0000.sst
sstables/L1/{run-id}/frag-0001.sst
sstables/L1/{run-id}/frag-0002.sst
```

**Manifest change:** Track `run_id` + list of fragment IDs per SSTable run. On compaction completion, delete input fragments incrementally, update manifest fragment list.

**ICS 2.0 Space Amplification Goal (SAG):** Configurable parameter (1.0–2.0). When the second-largest tier reaches half the size of the largest tier, trigger a cross-tier compaction, making the largest tier non-overlapping (like LCS). Recommended starting SAG: 1.75.

---

### 5. Three-Tier Local Cache: DRAM → NVMe → S3 (RocksDB/SlateDB)

**Problem:** PRD mentions "LRU cache for recently-accessed SSTables" but doesn't detail the caching architecture. All cache misses go directly to S3 (50-200ms).

**Three-tier design:**

| Tier | Latency | Content |
|------|---------|---------|
| DRAM | < 1 ms | Hot data blocks, bloom filters, sparse indexes |
| Local NVMe | < 5 ms | Warm SSTable blocks evicted from DRAM |
| S3 | 50-200 ms | Cold/authoritative data |

**Implementation:** SlateDB uses [Foyer](https://github.com/foyer-rs/foyer) (Rust hybrid DRAM+disk cache). RocksDB's `SecondaryCache` interface provides the abstraction.

**Logical cache (ScyllaDB):** Cache deserialized KV pairs, not raw bytes. No re-parsing on hit. Bypass OS page cache (`O_DIRECT`) for SSTable reads so compaction I/O doesn't evict hot user data.

**Continuity tracking (ScyllaDB):** Track which key ranges are "complete" in cache. If `(record_id, [a..z])` was fully read from S3 and cached, a subsequent miss for `(record_id, m)` means the key **doesn't exist** — skip the S3 GET.

**Cache eviction unit:** Row-granularity LRU (not partition-granularity). A single eviction unit = one item entry, not an entire record. Single global LRU list per shard.

---

### 6. Value Separation for Large Values (WiscKey/Titan/Pebble)

**Problem:** Your chunking threshold is 1 MB but values 1 KB–1 MB still cause massive write amplification during compaction. Every compaction re-reads and re-writes all values.

**Design:**
- Values >= threshold (e.g., 32 KB) stored as separate S3 blob objects
- SSTable stores only `(blob_object_id, offset, size)` pointer — the key section
- Compaction rewrites only keys + pointers, never value data
- Write amplification: 10-30x → ~1x for large values

**S3 Layout:**
```
blobs/{blob-id}.blob      # large value storage (separate from SSTables)
```

**Blob object format (Titan design):**
1. Sequential `(key, value)` pairs (key stored for GC validation)
2. Meta block with file properties
3. Footer with offsets and checksums

**GC:** Sample blob objects for discard ratio. If dead bytes > 50%, rewrite the blob object copying only live values. Track which blob objects are referenced by which SSTables via a `BlobFileSizeCollector` during compaction.

**Parallel value reads (WiscKey):** During range scans, keys are read sequentially from the SSTable but value GETs are issued concurrently from a thread pool. Each value fetch = one S3 GET. Without parallelism, N keys = N serial S3 GETs. With 32 concurrent GETs, effective latency = max(latency) not sum(latency).

---

### 7. S3 Prefix Sharding (RisingWave)

**Problem:** S3 rate limits are per-prefix: 3,500 PUT/s and 5,500 GET/s. Your layout puts all L0 SSTables under `sstables/L0/`. Under heavy flush load, you'll hit throttling.

**Solution:**
```
# Instead of:
s3://bucket/flushdb/{namespace}/sstables/L0/{id}.sst

# Use hash-based prefixes:
s3://bucket/{hash(sst_id) % 128}/flushdb/{namespace}/L0/{id}.sst
```

With 128 prefixes: up to 448K PUTs/s and 704K GETs/s before throttling.

**Note:** SlateDB uses ULIDs for SST naming — ULIDs are sortable AND distribute uniformly across prefixes due to their random component, naturally avoiding prefix hotspots. Consider adopting ULIDs for SSTable IDs.

---

### 8. Async Parallel S3 GETs (RocksDB MultiGet)

**Problem:** Read path searches L0 SSTables sequentially. Each lookup = 50-200ms. N L0 files = N * 50-200ms serial latency.

**Design:**
1. Issue bloom filter checks for all candidate SSTables concurrently (bloom filters are in memory — free)
2. For SSTables passing the bloom filter, issue concurrent S3 byte-range GETs for index blocks
3. Issue concurrent S3 byte-range GETs for data blocks
4. Merge results by sequence number (newest wins)

**RocksDB implementation:** `ReadOptions::async_io = true` uses C++ coroutines. Each `TableReader::MultiGet` suspends after issuing async reads. Measured: 40% latency reduction for single-level, 61% for multi-level, 2x scan throughput.

**Range scan prefetch:** Dual-buffer prefetch — consume buffer A while async-filling buffer B, then swap.

---

### 9. Ribbon Filters (RocksDB)

**Problem:** Your bloom filters use ~10 bits/key at 1% FPR. Ribbon filters achieve the same FPR at ~7 bits/key — 30% smaller.

**Benefit for S3:**
- More filters fit in DRAM (fewer S3 GETs to load filter sections)
- Smaller filter sections = faster S3 Range-GETs
- Same false positive rate → same number of unnecessary SSTable lookups

**Trade-off:** Construction cost is higher (~230 bits/key temp memory vs. ~75 for Bloom) but this is a one-time cost at compaction time, not on the read path.

**Partitioned filters (RocksDB):** Split the filter into many small filter blocks by key range. Makes individual filter blocks cache-friendly (fit in one DRAM cache entry). For S3: fetch only the relevant filter shard via Range request, not the entire filter.

---

## MEDIUM PRIORITY: Operational Improvements

### 10. I/O Scheduling Groups with Token Buckets (ScyllaDB)

**Problem:** Background tasks (compaction, memtable flush, CDC drain) compete with foreground reads/writes for S3 bandwidth.

**Design:**

| Scheduling Group | Priority | S3 Operations |
|-----------------|----------|---------------|
| User reads | Highest | GETs |
| Memtable flush to S3 | High | PUTs (new SSTables) |
| Compaction uploads | Background | GETs + PUTs |
| CDC drain to Kafka | Lowest | N/A |

**ScyllaDB's capacity rover approach:** Two atomic counters (tail rover, head rover) analogous to TCP's sliding window. Dispatching increments tail; completion advances head. Dispatch when `tail - head < capacity`. No locks — shards form a FIFO dispatch queue implicitly.

**Integral controller:** Auto-tunes compaction/flush group quotas. When compaction backlog grows, increase its bandwidth share. When it catches up, reduce. When memtable flushes fall behind, increase flush priority.

**S3 cost model:** `cost = read_bw/max_read_bw + write_bw/max_write_bw + read_iops/max_read_iops + write_iops/max_write_iops <= 1`

---

### 11. Range Tombstones with Delete-Only Compaction (Pebble)

**PRD already has range tombstones.** Add Pebble's optimization:

**Delete-only compaction:** If an SSTable's entire key range is fully covered by a range tombstone, drop the SSTable object without reading it. For S3: `DELETE` call (free) instead of GET + filter + re-upload.

**Range key masking with block property filters:** Each data block stores min/max timestamp of its entries. When iterating over a key space covered by an MVCC range tombstone, entire data blocks can be skipped if their timestamp range is entirely below the tombstone's timestamp. "Reduces latency of scanning in a keyspace with an MVCC range tombstone by several orders of magnitude."

**Range tombstone storage:** Store range tombstones in a separate meta-block per SSTable (not inline with point keys). Fragment overlapping tombstones into non-overlapping segments at SSTable write time.

---

### 12. Remote Stateless Compaction (RocksDB-Cloud / CaaS-LSM)

**Design:** Since all SSTables are on S3, compaction workers are inherently stateless.

**Protocol:**
1. Coordinator sends compaction job descriptor: `{input_sst_keys: [...], level: N, namespace: "..."}`
2. Worker downloads inputs from S3
3. Worker merge-sorts, applies compaction filters, writes output SSTables to S3
4. Worker returns output SSTable metadata to coordinator
5. Coordinator updates manifest via CAS

**Separation of concerns:**
- **Compaction Scheduler:** Observes SST counts/sizes per level, decides which compactions to run
- **Compaction Executor:** Downloads, merges, uploads, reports back

**Results (CaaS-LSM, SIGMOD '24):** Up to 8x OPS improvement, 98% P99 latency reduction, 99% write stall reduction vs. local compaction in cross-datacenter deployments.

**Scaling:** Spin up workers during write spikes, scale to zero during quiet periods. Workers are ephemeral — only need S3 access.

---

### 13. Multipart Upload with Streaming (RisingWave)

**Design for memtable flush to S3:**
- Use S3 multipart upload with 16 MB parts (empirically optimal)
- **Stream:** Upload part N while constructing part N+1, overlapping construction and upload
- **Lazy init:** If the SSTable is small (< 16 MB), use single `PutObject` instead (multipart has overhead)
- **Lifecycle rule:** Set S3 lifecycle rule to abort incomplete multipart uploads after 1 day

**Memory benefit:** Peak memory drops from O(SSTable_size) to O(16 MB).

---

### 14. Compaction Rate Limiter / Token Bucket (RocksDB)

**Design:**
- Token bucket refilled at `rate_bytes_per_sec` with `refill_period_us` (default 100ms)
- Compaction gets `IO_LOW` priority tokens; flush gets `IO_HIGH` priority tokens
- Fairness parameter: low-priority gets 1/10 chance to proceed even when high-priority is waiting (prevents starvation)
- Auto-tuned: adjusts dynamically in range `[rate / 20, rate]` based on recent demand

**Why:** Unthrottled compaction saturates S3 outbound bandwidth and may hit PUTs-per-second limits. Rate limiting allows foreground reads to maintain steady GET bandwidth.

---

### 15. Compaction Filters for TTL GC (RocksDB)

**Design:** Invoke a user-defined filter per key during compaction. Can:
- Drop expired keys (TTL-based) returning `kRemove`
- Modify values
- Use `CompactionFilterFactory` for per-sub-compaction instances (thread-safe)

**For flushdb:** Tombstone GC is already tied to compaction. Add a filter that also drops items whose metadata-encoded TTL has passed. `periodic_compaction_seconds` forces SSTs through compaction after a time threshold, ensuring TTL filters eventually see all data.

**Why:** Compaction already reads every SST block from S3. Piggybacking TTL cleanup is free — no extra S3 GETs.

---

### 16. Merge Operators (RocksDB)

**Problem:** Update-heavy workloads (counters, append-to-list) currently require `Get` (S3 GET) then `Put`.

**Design:**
- Write the "delta" directly to the memtable as a merge operand — no read needed
- `PartialMerge`: combines two merge operands into one (reduces accumulation)
- `FullMerge`: applies all operands to a base value at read time or compaction
- Operands accumulate in SSTs, resolved lazily

**Use cases:** Counters, append logs, set unions, last-write-wins maps.

---

### 17. Compaction-Aware Cache Eviction (SAS-Cache, MSST '24)

**Problem:** After compaction invalidates old SSTables, their cached blocks become stale. Local cache fills with dead blocks, reducing hit rate.

**Design:**
- Track compacted SSTable IDs in a queue
- After compaction completes, asynchronously evict all blocks from invalidated SSTables from the local NVMe cache
- **Multi-level prefetching:** Prefetch blocks for the next level during sequential scans
- **LSM-Managed Cache Filter:** On eviction to secondary cache, check if SSTable has been compacted; skip insertion if so

**Results:** 36% throughput improvement, 20% latency reduction vs. naive secondary cache.

---

### 18. SSTable Encoding Optimizations (ScyllaDB mc/me formats)

**ScyllaDB saw 53% file size reduction for wide-row schemas.** For S3: smaller SSTables = lower storage costs + faster GETs.

**Optimizations:**
- **Varint encoding:** Small integers (timestamps, lengths) in 1-2 bytes instead of fixed 8 bytes
- **Delta-encoded timestamps:** Store base (minimum) timestamp once per SSTable; entries store only the delta (typically 1-2 bytes)
- **record_id deduplication within blocks:** record_id stored once per block as a prefix, not per entry. Since all items in a block for the same record share the record_id, this is significant savings for wide records
- **Row-level attributes:** Row-level metadata stored as first-class structures, not embedded in entries

---

### 19. WAL Dirty Segment Tracking (ScyllaDB)

**Problem:** WAL segments can't be deleted until all column families that wrote to them have been flushed. Without tracking, you either delete too early (data loss) or keep segments forever (disk bloat).

**ScyllaDB's approach:**
- Each WAL segment tracks `Map<memtable_generation_id → highest_allocated_position>`
- When a memtable is flushed to S3, it reports its highest WAL position to the segment manager
- A segment can be deleted only when all memtable generations that wrote to it have been confirmed flushed
- This handles the tricky case: one low-write-rate partition can pin segments containing writes for high-write-rate partitions

**Action:** Add dirty tracking map to WAL segment metadata.

---

## LOWER PRIORITY: Advanced/Future Designs

### 20. Tablets — Per-Sub-Range Independent LSM Trees (ScyllaDB 6.0)

**Design:**
- Each tablet covers a sub-range of the token ring for one namespace
- Each tablet has its own independent mini-LSM: memtable, SSTable files, compaction state
- Migration = flush tablet's memtable + copy SSTable files to new owner + update Raft metadata
- No data re-encoding or streaming needed for migration

**Benefits:**
- Independent compaction per range (no cross-range coordination)
- Migration is O(data_size) not O(in-flight_writes)
- Per-tablet load balancing without repartitioning

---

### 21. Delta Layers + Image Layers (Neon)

**For future point-in-time query support:**

- **Image layer:** Full snapshot of all keys in a range at one timestamp. Named `{key_range}__{timestamp}`
- **Delta layer:** WAL records for keys modified within a `(key_range, timestamp_range)` window
- **Page reconstruction:** Find nearest image layer ≤ target timestamp, apply delta layers forward

**GC policy:**
- Retain layers needed by active snapshots
- Delete layers fully superseded by newer image layers
- Never delete the last remaining layer for a key range

---

### 22. Raft Group 0 for Metadata Coordination (ScyllaDB 5.2+)

**Replace gossip-based metadata with Raft for:**
- Partition ownership assignment (currently S3 ETag CAS)
- Compaction plan scheduling (prevents two workers claiming the same SSTable)
- Schema/namespace changes
- Ring topology changes (node join/leave)

**Benefits:** Linearizable metadata, no split-brain, log-based recovery, no S3 polling for ring state.

---

### 23. Log Replicas for Fast Failover (DynamoDB)

**DynamoDB's design:**
- Specialized replicas that store only recent WAL entries (not full memtable state)
- During leader failure, a log replica can join a quorum immediately without rebuilding state
- Full data replicas eventually catch up from the log replica's WAL

**For flushdb:** Follower nodes already store a follower WAL. Consider designating some as "log-only replicas" that never build a memtable — they only serve WAL replay during failover, not reads. Faster to spin up than full replicas.

---

### 24. Trie-Based Partition Index (ScyllaDB ms format / BTI)

**Replace the sparse index with a trie:**
- **O(key_length) lookup** vs O(log n) binary search in sparse index
- **Prefix compression** in trie nodes reduces index size significantly
- **Partitioned index access:** Fetch only the relevant trie shard for a key prefix

**ScyllaDB's BTI format:**
- `Partitions.db`: trie-based index of partition keys → byte offset in data file
- `Rows.db`: trie-based index of clustering keys within partitions

---

### 25. L0 Sublevels with Flush Split Keys (Pebble)

**Problem:** All L0 SSTables overlap in key range, forcing wide L0→L1 compactions that must include all overlapping files.

**Design:**
- During memtable flush, split output into multiple smaller L0 SSTables at pre-defined split keys
- Non-overlapping L0 files can be compacted concurrently without coordination

**Pebble results:** 92% improvement in overall compaction time (550 min → 385 min with concurrent, further improved by sublevels).

---

### 26. Deterministic Simulation Testing (FoundationDB)

**Design:** Run the complete database software in a single process with all sources of non-determinism abstracted:
- Network (S3 calls)
- Disk (WAL)
- Time
- Random number generation

Inject faults: S3 throttling, partial GETs, S3 eventual consistency races, network partitions, node crashes. Every run reproducible by seed.

**This is essential** for testing correctness of your epoch/CAS/gossip logic. S3's failure modes (rate limiting, transient errors, read-after-write inconsistency for overwrites) are complex and hard to reproduce in integration tests.

---

### 27. Distributed Cache with Consistent Hashing (ClickHouse)

**For multi-tenant scaling:**
- Multiple cache nodes with consistent hashing to partition hot blocks
- Each node owns a portion of the key space, fetches from S3 on miss, stores on local NVMe
- Write-through: new SSTables flushed to S3 are simultaneously cached in the distributed cache
- Immutability: SSTables never change after write, so no cache invalidation — only eviction
- Compute nodes pull blocks from multiple cache nodes in parallel (100-250 µs vs. 50+ ms for S3)

---

## Summary: Priority Matrix

| Priority | Design | Source | Key Benefit |
|----------|--------|--------|-------------|
| **Critical** | Epoch-based fencing | SlateDB | Prevents zombie writer corruption |
| **Critical** | Manifest CAS protocol | SlateDB | Correct concurrent writes to manifest |
| **High** | Shard-per-core | ScyllaDB | Zero-lock write path |
| **High** | ICS SSTable runs + fragment deletion | ScyllaDB | Reduced S3 storage cost during compaction |
| **High** | Three-tier cache (DRAM→NVMe→S3) | RocksDB/SlateDB | 10-50x read latency improvement |
| **High** | Value separation (WiscKey/Titan) | WiscKey, TiKV, Pebble | Eliminates value rewrite during compaction |
| **High** | S3 prefix sharding (128 prefixes) | RisingWave | Avoids S3 rate limiting at scale |
| **High** | Async parallel S3 GETs | RocksDB MultiGet | Hides S3 per-GET latency |
| **High** | Ribbon filters | RocksDB | 30% smaller filters, same FPR |
| **Medium** | I/O scheduling groups + token buckets | ScyllaDB | Background work doesn't starve reads |
| **Medium** | Delete-only compaction for range tombstones | Pebble | Free bulk delete without S3 GETs |
| **Medium** | Remote stateless compaction | RocksDB-Cloud | Elastic compaction scaling |
| **Medium** | Multipart upload with streaming | RisingWave | Reduces peak memory, overlaps upload |
| **Medium** | Compaction rate limiter | RocksDB | Prevents S3 bandwidth saturation |
| **Medium** | Compaction filters for TTL GC | RocksDB | Piggybacks GC on existing compaction I/O |
| **Medium** | Merge operators | RocksDB | Eliminates read-before-write for updates |
| **Medium** | Compaction-aware cache eviction | SAS-Cache | Keeps local cache valid post-compaction |
| **Medium** | SSTable encoding optimizations (varint, delta-ts) | ScyllaDB | Smaller SSTables → lower S3 cost |
| **Medium** | WAL dirty segment tracking | ScyllaDB | Safe WAL truncation |
| **Lower** | Tablets (per-sub-range LSM) | ScyllaDB 6.0 | Fast migration via metadata update |
| **Lower** | Raft Group 0 for metadata | ScyllaDB 5.2+ | Linearizable coordination |
| **Lower** | Log replicas | DynamoDB | Faster failover without full state |
| **Lower** | Delta + image layers | Neon | Point-in-time query support |
| **Lower** | Trie-based partition index | ScyllaDB BTI | O(key_length) index lookup |
| **Lower** | L0 sublevels + flush split keys | Pebble | Concurrent non-overlapping L0 compactions |
| **Lower** | Deterministic simulation testing | FoundationDB | Reproducible fault injection |
| **Lower** | Distributed cache with consistent hashing | ClickHouse | Shared warm cache for multi-tenant |
