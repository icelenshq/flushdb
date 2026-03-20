# Task 1: BlockCache + CacheConfig

**Crate:** `flushdb-engine`
**Files:** `src/cache/block_cache.rs`, `src/cache/mod.rs`
**Depends on:** Nothing
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §12 (Three-Tier Cache Architecture), Phase 6 §1–§3

---

## Goal

Build the core DRAM cache data structure using the `moka` crate's W-TinyLFU algorithm. This is the foundation every other caching task builds on. The cache stores deserialized `BlockEntry` vectors (logical cache) keyed by `(sstable_id, block_offset)`, so cache hits avoid both StorageBackend I/O and block decompression/parsing.

---

## What to Build

### 1.1 New Module Structure

Create `src/cache/mod.rs` as the cache module root. It re-exports all public types from sub-modules. All cache-related code lives under `src/cache/`.

### 1.2 BlockCacheKey

A composite key identifying a single data block:

```
BlockCacheKey {
    sst_id: String,       // SSTable ID from SSTableMeta.id
    block_offset: u64,    // Byte offset within the SSTable file
}
```

- Implements `Hash`, `Eq`, `Clone`, `Debug`
- The pair `(sst_id, block_offset)` uniquely identifies a block across the entire engine

### 1.3 CachedBlock

The cached value representing a single decoded data block:

```
CachedBlock {
    entries: Vec<BlockEntry>,   // Deserialized entries from the block
    size_bytes: u32,            // Approximate memory size for cache weigher
}
```

- `size_bytes` is computed once at construction as the sum of: each entry's `composite_key.as_bytes().len()` + value size + `metadata.len()` + fixed overhead per entry (40 bytes for struct fields). **Value size depends on `EntryValue` variant:** `EntryValue::Inline(bytes)` → `bytes.len()`, `EntryValue::BlobRef { blob_id, .. }` → `blob_id.len() + 12` (8 bytes offset + 4 bytes size).
- Implements `Clone`, `Debug`

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(entries: Vec<BlockEntry>) -> Self` | Computes `size_bytes` from entries, stores both |
| `entries` | `(&self) -> &[BlockEntry]` | Returns the decoded entries |
| `estimated_size` | `(&self) -> u32` | Returns the precomputed byte size |

### 1.4 BlockCache

Wraps a `moka::sync::Cache<BlockCacheKey, CachedBlock>` with W-TinyLFU admission:

```
BlockCache {
    inner: moka::sync::Cache<BlockCacheKey, CachedBlock>,
}
```

`moka` provides W-TinyLFU out of the box — window cache (1%), frequency sketch, and segmented main cache (99%) are configured automatically. The cache is sized by total weight (bytes), not entry count.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: &CacheConfig) -> Self` | Creates cache with `max_capacity` from config, custom weigher using `CachedBlock::estimated_size` |
| `get` | `(&self, key: &BlockCacheKey) -> Option<CachedBlock>` | Returns cached block if present; counts as access for frequency tracking |
| `insert` | `(&self, key: BlockCacheKey, block: CachedBlock)` | Inserts block; W-TinyLFU admission decides whether it enters main cache |
| `invalidate` | `(&self, key: &BlockCacheKey)` | Removes specific block |
| `invalidate_sst` | `(&self, sst_id: &str)` | Removes all cached blocks for the given SSTable ID. Uses `moka`'s `invalidate_entries_if` with a predicate matching `key.sst_id` |
| `entry_count` | `(&self) -> u64` | Number of entries currently in cache |
| `weighted_size` | `(&self) -> u64` | Total weight (bytes) of all cached entries |
| `stats` | `(&self) -> CacheStats` | Returns snapshot of hit/miss/eviction counters |

### 1.5 CacheConfig

Configuration for all cache components:

```
CacheConfig {
    block_cache_capacity_bytes: u64,    // Max DRAM for data block cache (default: 256 MB)
    pinned_metadata_capacity: usize,    // Max pinned SSTable metadata entries (default: 1000)
    enable_continuity_tracking: bool,   // Enable negative lookup cache (default: true)
    get_budget_per_read: u32,           // Max StorageBackend GETs per read (default: 8)
}
```

Implements `Clone`, `Debug`, `Default`.

Default values:

| Field | Default | Rationale |
|-------|---------|-----------|
| `block_cache_capacity_bytes` | 268_435_456 (256 MB) | Reasonable DRAM footprint for single-node deployment |
| `pinned_metadata_capacity` | 1000 | Supports ~1000 open SSTables before eviction |
| `enable_continuity_tracking` | true | Almost always beneficial |
| `get_budget_per_read` | 8 | Prevents pathological unbounded latency |

### 1.6 CacheStats

Snapshot of cache metrics:

```
CacheStats {
    hits: u64,
    misses: u64,
    insertions: u64,
    evictions: u64,
    weighted_size_bytes: u64,
    entry_count: u64,
}
```

Implements `Clone`, `Debug`, `Default`.

### 1.7 Design Decisions

- **`moka::sync::Cache` not `moka::future::Cache`:** The sync cache avoids async overhead on the hot path. Block fetching is async, but cache lookup/insert is synchronous and sub-microsecond. The sync cache is `Send + Sync` and safe for use across tokio tasks.
- **Logical cache (deserialized entries):** Cache stores `Vec<BlockEntry>` not raw `Bytes`. This trades slightly higher memory usage for zero CPU on cache hits — no decompression, no parsing. This is the right tradeoff because DRAM is cheaper than latency.
- **Weight-based sizing:** Cache capacity is in bytes, not entry count. A 4KB compressed block might expand to 20KB deserialized — the weigher tracks actual memory consumption.
- **`invalidate_sst` uses predicate scan:** This is O(n) over cache entries but runs asynchronously after compaction, not on the read hot path. Moka's `invalidate_entries_if` handles this efficiently.

### 1.8 Future-Proofing

- **NVMe tier (future):** The `BlockCache` should be designed so that a future NVMe tier can register an eviction listener via `moka`'s `eviction_listener`. When a block is evicted from DRAM, the listener can demote it to NVMe instead of dropping it. Do NOT implement the listener yet — just ensure the `moka::sync::Cache` builder uses `eviction_listener()` with a no-op placeholder that can be replaced later.
- **Per-shard caches (future):** The current design uses one shared cache. In Phase 7's shard-per-core architecture, each shard will own its own `BlockCache`. The API should not assume a singleton.

---

## Tests

**File:** `crates/flushdb-engine/tests/block_cache_tests.rs`

### Basic Operations
| Test | What It Validates |
|------|-------------------|
| `test_insert_and_get` | Insert a CachedBlock, get returns the same entries |
| `test_get_miss` | Get on a key that was never inserted returns None |
| `test_insert_overwrites` | Insert same key twice, get returns the latest |
| `test_invalidate_single` | Invalidate a key, subsequent get returns None |
| `test_invalidate_nonexistent` | Invalidate a key that doesn't exist — no panic |

### SSTable-Level Invalidation
| Test | What It Validates |
|------|-------------------|
| `test_invalidate_sst_removes_all_blocks` | Insert 10 blocks for SST "A", invalidate_sst("A"), all 10 return None |
| `test_invalidate_sst_preserves_other_ssts` | Insert blocks for SST "A" and "B", invalidate_sst("A"), "B" blocks remain |
| `test_invalidate_sst_nonexistent` | invalidate_sst for unknown SST — no panic, no side effects |

### W-TinyLFU Eviction Behavior
| Test | What It Validates |
|------|-------------------|
| `test_capacity_eviction` | Insert blocks exceeding capacity, total weighted_size stays within configured limit |
| `test_scan_resistance` | Insert N hot blocks, access each 10 times. Then insert M cold blocks (simulating scan). Verify hot blocks are still present — cold scan blocks were rejected by admission filter |
| `test_frequency_tracking` | Access key A 100 times, key B once. Fill cache to capacity. Key A should survive eviction, key B should not |

### CacheStats
| Test | What It Validates |
|------|-------------------|
| `test_stats_track_hits_and_misses` | Perform gets (some hits, some misses), verify stats counters match |
| `test_stats_track_insertions` | Insert N blocks, verify insertions counter is N |

### CachedBlock Sizing
| Test | What It Validates |
|------|-------------------|
| `test_cached_block_size_calculation` | Create CachedBlock with known entries, verify estimated_size matches expected calculation |
| `test_empty_block_size` | CachedBlock with empty entries vec has size 0 |

### CacheConfig Defaults
| Test | What It Validates |
|------|-------------------|
| `test_default_config` | Default CacheConfig has expected values (256 MB, 1000 pinned, budget 8) |

---

## Done When

- [ ] `BlockCache` wraps `moka::sync::Cache` with W-TinyLFU admission
- [ ] Cache is sized by weight (bytes), not entry count
- [ ] `invalidate_sst` removes all blocks for a given SSTable ID
- [ ] Scan-resistance test passes — hot blocks survive cold scan flood
- [ ] `CacheStats` tracks hits, misses, insertions, evictions
- [ ] `CacheConfig` has sensible defaults
- [ ] No-op eviction listener placeholder is wired for future NVMe tier
- [ ] All tests pass
