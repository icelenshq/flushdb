# Phase 6: Caching + Read Optimizations

**Complexity: L**
**Crate:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §12, §13

---

## Goal

Add a DRAM cache layer between the read path and StorageBackend to make repeated reads fast (<1ms) while remaining scan-resistant. After this phase, the read path intelligently caches hot data without letting full-record scans evict point-read hotspots.

---

## 1. Three-Tier Cache Architecture

All cache misses going directly to S3 (50-200ms) is unacceptable for read-heavy workloads. The cache sits between the read path and the StorageBackend:

| Tier | Latency | Content |
|------|---------|---------|
| **DRAM** (this phase) | < 1 ms | Hot data blocks, bloom/ribbon filters, sparse indexes, deserialized KV pairs |
| Local NVMe (future) | < 5 ms | Warm SSTable blocks evicted from DRAM |
| StorageBackend (S3) | 50-200 ms | Cold/authoritative data |

**Cache key:** `(sstable_id, block_offset)` — uniquely identifies a data block.

---

## 2. W-TinyLFU Admission Policy

The core cache algorithm, implemented via the `moka` crate. This is a scan-resistant admission policy that prevents full-record scans from evicting hot point-read data:

### Structure

```
┌──────────────────────────────────────────────────┐
│                 Incoming Entry                    │
│                      │                            │
│                      ▼                            │
│         ┌─────────────────────┐                  │
│         │ Window Cache (1%)    │ ← unconditional  │
│         │ Small LRU            │   admission       │
│         └─────────┬───────────┘                  │
│                   │ evicted from window           │
│                   ▼                               │
│         ┌─────────────────────┐                  │
│         │ Frequency Sketch     │ ← Count-Min      │
│         │ (tracks all access)  │   Sketch, ~8B/key│
│         └─────────┬───────────┘                  │
│                   │ frequency > eviction candidate │
│                   ▼                               │
│         ┌─────────────────────────────┐          │
│         │ Main Cache (99%)             │          │
│         │   Protected (80%)            │          │
│         │   Probation (20%)            │          │
│         └─────────────────────────────┘          │
└──────────────────────────────────────────────────┘
```

1. **Window cache (1% of DRAM tier):** New entries admitted to a small LRU unconditionally. This absorbs bursty access patterns.
2. **Frequency sketch (Count-Min Sketch):** Tracks access frequency across the entire keyspace, ~8 bytes per tracked key.
3. **Main cache (99% of DRAM tier):** Segmented LRU with 80% protected and 20% probation segments. Window evictions are admitted to main only if their frequency exceeds the eviction candidate's frequency.
4. **Periodic reset:** Frequency sketch halved periodically to adapt to shifting access patterns.

**Why this matters:** A full-record scan touching thousands of blocks once each would normally flush all hot data from a plain LRU. W-TinyLFU's frequency check means these one-hit-wonder scan blocks are never admitted to the main cache — they pass through the window and are evicted without displacing frequently-accessed point-read blocks.

---

## 3. Logical Cache

Cache deserialized KV pairs, not raw compressed bytes:
- No re-parsing or decompression on cache hit
- Cache entries are the already-decoded `MemtableEntry` (or equivalent read-path struct)
- This trades memory for CPU — cache entries are slightly larger than raw blocks, but hit latency is sub-microsecond

---

## 4. Index and Filter Pinning

Index blocks and bloom filter blocks are pinned in DRAM for the lifetime of the SSTable:
- Typically <1% of SSTable size (a 64MB SSTable has ~125KB bloom + ~50KB index)
- Never evicted by the W-TinyLFU policy — they're separate from the data cache
- Eliminates a StorageBackend round-trip on every read that needs to check or seek

---

## 5. Continuity Tracking

Track which key ranges are complete in cache:
- If `(record_id, [a..z])` was fully read and cached, a subsequent miss for `(record_id, m)` means the key doesn't exist — skip the StorageBackend GET entirely
- Intervals are tagged with the manifest version at the time they were cached
- **Invalidation on compaction:** When compaction produces a new manifest that touches the key range, the continuity interval is invalidated
- This is a powerful negative cache — saves StorageBackend round-trips for keys that don't exist within cached ranges

---

## 6. Compaction-Aware Eviction

After compaction invalidates old SSTables:
- Asynchronously evict all cached blocks from the compacted-away SSTables
- Use `(sstable_id, *)` as a batch eviction key
- Prevents serving stale data from SSTables that no longer exist in the manifest

---

## 7. Coalesced Block Fetches

When multiple adjacent data blocks need to be read:
- Detect adjacency (consecutive `block_offset` values)
- Merge into a single `get_range` call that spans all adjacent blocks
- Split the response back into individual blocks for caching
- Reduces StorageBackend round-trips for range scans from O(blocks) to O(1) for contiguous regions

---

## 8. SSTable GET Budget

Cap the number of StorageBackend GETs per read operation:
- Default budget: 8 GETs per read
- If more are needed, defer remaining blocks and return partial results with a staleness flag
- Prevents pathological cases (e.g., a record spread across hundreds of L0 SSTables) from causing unbounded latency

---

## 9. Adaptive Pagination

Improve pagination accuracy:
- **First page:** Estimate item count from cached average item size for the namespace
- **Subsequent pages:** Use actual average item size from the previous page (stored in the page token)
- **Continuous update:** Server-side cache of average item size per namespace, continuously updated from observed reads

---

## New Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| moka | 0.12 | W-TinyLFU cache implementation (DRAM tier) |

---

## Future Work Considerations

When building Phase 6, keep the following downstream dependencies in mind:

| What You're Building | Who Needs It Later | What To Watch For |
|---------------------|-------------------|-------------------|
| **W-TinyLFU cache** | Server read path (P7), NVMe tier (future) | The server dispatches reads through the cache. Design the cache as a layer that wraps `BlockFetcher` (or equivalent) — the server shouldn't know whether data came from cache or StorageBackend. Future NVMe tier sits between DRAM and S3 — design eviction to support a "demote to NVMe" callback rather than just dropping entries. |
| **Index/filter pinning** | Server lifecycle (P7), Ribbon filters (future) | The server manages SSTable lifecycles across partitions. Pinned filters must be released when an SSTable is removed from the manifest (after compaction). Future ribbon filters replace bloom for L1+ — the pinning mechanism should work with any filter type, not just bloom. |
| **Continuity tracking** | Server negative lookups (P7) | The server benefits from negative cache hits (key doesn't exist in a fully-cached range). Tag continuity intervals with manifest version so they're invalidated on compaction. The server should be able to query continuity status before issuing StorageBackend reads. |
| **Compaction-aware eviction** | Compaction integration (P5e) | Compaction produces a list of consumed SSTable IDs. The cache needs an efficient `evict_sstable(id)` bulk operation. Don't require per-block eviction — batch by SSTable ID. |
| **GET budget** | SLO-aware pagination (P7) | The server extends GET budgets with SLO awareness (deadline-based cutoff). Design the budget as a shared counter that the server can also decrement based on elapsed time, not just GET count. |
| **Adaptive pagination** | Server pagination (P7) | The server stores per-namespace average item sizes. The cache layer feeds observed sizes back to the server. Expose a hook for recording observed item sizes during reads so the server can update its running average. |
| **Coalesced block fetches** | S3 cost optimization (P7) | S3 charges per GET. Coalescing reduces cost significantly for range scans. The coalescing logic should be aware of S3 minimum charge sizes (~256 KB) — coalescing blocks smaller than the minimum charge size is always worth it even with slight over-fetching. |

---

## Done When

- Repeated point reads for the same key hit cache (<1ms latency after first read)
- Full-record scan does NOT evict hot point-read data — point reads remain fast after a scan completes
- Bloom filters and index blocks are pinned in memory, not evicted
- Compaction invalidates stale cache entries — no stale reads after compaction
- Continuity tracking: negative lookup within a fully-cached key range skips StorageBackend
- Coalesced fetches: range scan over N adjacent blocks results in fewer than N StorageBackend calls
- GET budget: a read exceeding 8 StorageBackend GETs is capped and returns partial results
- Adaptive pagination produces more accurate page sizes on second and subsequent pages
