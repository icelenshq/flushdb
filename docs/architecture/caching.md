# Caching

Every cache miss that falls through to S3 costs 50-200ms. For read-heavy workloads, this latency is unacceptable. flushdb interposes two local cache tiers between the storage engine and S3 to keep hot and warm data close to the CPU.

---

## Three-Tier Architecture

| Tier | Latency | Content |
|------|---------|---------|
| DRAM | < 1 ms | Hot data blocks, bloom/ribbon filters, sparse indexes, deserialized KV pairs |
| NVMe | < 5 ms | Warm SSTable blocks evicted from DRAM |
| S3 | 50-200 ms | Cold and authoritative data |

```
  Read request
       |
       v
  +---------+     hit
  |  DRAM   | -----------> return
  |  cache  |
  +---------+
       | miss
       v
  +---------+     hit
  |  NVMe   | -----------> promote to DRAM, return
  |  cache   |
  +---------+
       | miss
       v
  +---------+
  |   S3    | -----------> admit to DRAM (subject to W-TinyLFU), return
  |         |
  +---------+
```

The cache key for all tiers is `(sstable_id, block_offset)`, which uniquely identifies a 4 KB data block within an SSTable.

---

## Design Principles

**Logical cache, not byte cache.** The DRAM tier stores deserialized key-value pairs, not raw compressed bytes. A cache hit returns a fully parsed structure with zero additional work. This avoids re-decompressing and re-parsing on every hit, which would otherwise dominate CPU time for hot keys.

**Bypass the OS page cache.** SSTable reads from local NVMe use O_DIRECT, bypassing the kernel's page cache entirely. Without this, compaction -- which reads and writes large volumes of data sequentially -- would evict hot user data from the page cache. O_DIRECT ensures that only the application-level cache controls what stays resident.

**Per-shard LRU.** flushdb uses a shard-per-core architecture where each CPU core owns its data structures exclusively. The cache follows the same model: each core owns its own LRU. No locks, no contention, no cross-core cache coherence traffic on the hot path.

**Eviction granularity.** Individual data blocks (4 KB). Evicting a single block does not disturb other blocks from the same SSTable.

---

## W-TinyLFU Admission Policy

A naive LRU cache is vulnerable to scan pollution: a single full-table scan can evict the entire working set of hot keys, leaving the cache cold for subsequent point reads. W-TinyLFU (Window Tiny Least Frequently Used) prevents this by gating admission to the main cache on access frequency.

The policy divides the DRAM tier into two regions:

```
DRAM Cache
+-------------------------------------------------------+
|                                                       |
|  Window Cache (1%)          Main Cache (99%)          |
|  +---------------+          +---------------------+   |
|  |               |          |                     |   |
|  |   Small LRU   |  admit   |  Segmented LRU      |   |
|  |               | -------> |                     |   |
|  | (unconditional|  only if |  Protected: 80%     |   |
|  |  admission)   |  freq >  |  Probation: 20%     |   |
|  |               |  victim  |                     |   |
|  +---------------+          +---------------------+   |
|                                                       |
|  Frequency Sketch (Count-Min Sketch)                  |
|  Tracks access counts, ~8 bytes per key               |
+-------------------------------------------------------+
```

**How it works:**

1. **Window cache (1% of DRAM).** Every new entry is admitted unconditionally into a small LRU. This gives genuinely new hot keys a chance to accumulate frequency before competing for the main cache.

2. **Frequency sketch.** A Count-Min Sketch data structure that tracks how often each key has been accessed. It uses approximately 8 bytes of memory per tracked key and supports O(1) frequency lookups.

3. **Main cache (99% of DRAM).** A segmented LRU with two segments:
   - **Protected segment (80%):** Entries that have proven their value through repeated access. Evictions from the protected segment demote entries to probation rather than discarding them.
   - **Probation segment (20%):** Entries on trial. If accessed again, they move to the protected segment. If not, they are the first to be evicted.

4. **Admission decision.** When an entry is evicted from the window cache, it competes against the eviction candidate from the main cache's probation segment. The frequency sketch is consulted for both. The entry with the higher access frequency wins the main cache slot; the loser is discarded.

5. **Periodic frequency reset.** The frequency sketch is halved at regular intervals. This prevents stale frequency counts from permanently blocking admission of keys whose access pattern has shifted. A key that was hot an hour ago but has gone cold will gradually lose its accumulated frequency, making room for currently active keys.

**NVMe admission.** The NVMe tier applies the same frequency check. When a block is evicted from DRAM, it is written to NVMe only if its frequency exceeds the threshold. One-time scan blocks are discarded entirely rather than polluting the NVMe tier.

---

## Continuity Tracking

When a full record read fetches and caches every item for a given record ID across the key range [a..z], the cache knows that this range is complete. If a subsequent point read for (record_id, m) misses the cache for that key, the engine can conclude that the key does not exist without issuing an S3 GET.

```
Cache state for record "user:42":

  Cached range:  [a]---[b]---[c]---...---[x]---[y]---[z]
                  |<--------- complete coverage -------->|

  Point read for (user:42, m):
    m falls within [a..z]
    Range is marked complete
    Cache miss means the key does not exist
    Skip S3 GET, return not-found immediately
```

These continuity intervals are tagged with the manifest version that was current when the range was cached. When compaction produces a new manifest that affects the key range of a tracked interval, that interval is invalidated. Subsequent reads for keys in the invalidated range will go to S3 to pick up any changes introduced by compaction.

---

## Compaction-Aware Eviction

Compaction replaces old SSTables with new ones. Cached blocks from old SSTables become stale the moment compaction commits a new manifest that removes those SSTables.

Two mechanisms keep the cache consistent:

1. **Asynchronous eviction.** After compaction commits, the engine walks the list of removed SSTable IDs and evicts any cached blocks belonging to those SSTables. This runs asynchronously to avoid blocking the compaction commit path.

2. **NVMe insertion guard.** When a block evicted from DRAM would normally be written to NVMe, the engine checks whether the block's SSTable has been removed by a recent compaction. If so, the block is discarded rather than written to NVMe. This prevents the NVMe tier from accumulating stale data.

Together, these ensure that reads never serve data from SSTables that are no longer part of the live manifest.

---

## Index and Filter Pinning

Index blocks and bloom/ribbon filter blocks are pinned in DRAM for the entire lifetime of their SSTable. They are loaded when the SSTable is first opened (either on startup from the manifest or after a flush/compaction produces a new SSTable) and released only when the SSTable is garbage collected.

These metadata structures are small relative to the data they index -- typically less than 1% of the SSTable's total size. A 64 MB SSTable might have a 125 KB bloom filter and a few KB of index data. Pinning them eliminates S3 GETs for metadata lookups entirely, reducing every point read to at most one data block fetch.

```
SSTable "sst-A" opened:
  +------------------+
  | Index block      |  pinned in DRAM  (few KB)
  | Bloom filter     |  pinned in DRAM  (~125 KB)
  +------------------+
  | Data block 0     |  fetched on demand from cache or S3
  | Data block 1     |  fetched on demand from cache or S3
  | ...              |
  +------------------+
```

---

## Coalesced Block Fetches

When a range scan or multi-key read requires several adjacent data blocks from the same SSTable, the engine merges those requests into a single S3 byte-range GET covering the full span.

```
Blocks needed: [3] [4] [5] [6]

Without coalescing:
  GET Range: bytes=12288-16383    (block 3)
  GET Range: bytes=16384-20479    (block 4)
  GET Range: bytes=20480-24575    (block 5)
  GET Range: bytes=24576-28671    (block 6)
  = 4 S3 GETs

With coalescing:
  GET Range: bytes=12288-28671    (blocks 3-6)
  = 1 S3 GET
```

This reduces the total number of S3 GET requests, which matters both for latency (fewer round-trips) and cost (S3 charges per request). The engine also performs speculative fetching for point reads near block boundaries: if the target key could fall in either of two adjacent blocks, both are fetched in a single request.

---

## GET Budget

Each read operation has a configurable maximum number of S3 GET requests it is allowed to issue (default: 8). This bounds the cost of pathological queries -- for example, a range scan across a record that spans hundreds of SSTables at L0 before compaction has caught up.

When a read exhausts its GET budget before completing:

- Results gathered so far are returned
- The response includes a staleness flag indicating that the result may be incomplete
- A page token allows the client to continue the read in a subsequent request

This prevents runaway S3 costs from individual queries and provides a predictable upper bound on per-read latency.

---

## Adaptive Pagination

Range reads and full record reads use byte-based pagination rather than row counts. The client specifies a byte budget (default 2 MB), and the engine accumulates items until the budget is met.

The challenge is estimating how many blocks to fetch from S3 to fill the byte budget without over-fetching (wasting GET requests) or under-fetching (requiring extra round-trips).

```
First page request:
  1. Look up cached average item size for the namespace
     (e.g., avg = 500 bytes, budget = 2 MB -> estimate ~4000 items)
  2. Fetch blocks until byte budget is met
  3. If too few items arrive, issue additional block reads
  4. Record actual average item size in the page token

Subsequent page requests:
  1. Read average item size from the page token
     (refined from actual data seen in previous pages)
  2. Estimate block count more accurately
  3. Fetch and accumulate
```

**SLO-aware early return.** Each namespace configures a target latency SLO (e.g., 10ms for p99). If accumulating items approaches the request's latency deadline, the engine stops issuing further S3 reads, returns a partial page with the items gathered so far, and includes a page token for continuation. This trades completeness for predictable latency, ensuring that one slow S3 response does not blow the SLO for the entire request.
