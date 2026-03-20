# Task 9: Engine Integration + Cache Lifecycle

**Crate:** `flushdb-engine`
**Files:** `src/engine.rs`, `src/sstable_handle.rs`, `src/read_path.rs`, `src/cache/mod.rs`, `src/lib.rs`
**Depends on:** All previous tasks (1–8)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §12, Phase 6 §1–§9

---

## Goal

Wire all cache components into the `Engine` struct so that the caching layer is transparent to callers. After this task, `Engine::get()`, `Engine::scan()`, and `Engine::multi_get()` automatically benefit from block caching, metadata pinning, continuity tracking, coalesced fetches, GET budgets, and adaptive pagination — with no API changes visible to the caller (except the new `is_partial` flag and improved pagination accuracy).

---

## What to Build

### 9.1 CacheConfig in EngineConfig

Add `CacheConfig` to the existing `EngineConfig`:

```
EngineConfig {
    pub memtable_config: MemtableConfig,
    pub wal_config: WalConfig,
    pub flush_config: FlushConfig,
    pub compaction_config: CompactionConfig,
    pub manifest_config: ManifestConfig,
    pub cache_config: CacheConfig,        // NEW
    pub namespace: String,
    pub local_dir: PathBuf,
}
```

`CacheConfig` gets a `Default` impl so existing code that constructs `EngineConfig` still compiles with `..Default::default()`.

### 9.2 Engine Struct Changes

Replace `DirectBlockFetcher` with the cached fetcher stack and add cache components:

```
Engine<B: StorageBackend> {
    memtable_list: MemtableList,
    wal_manager: WalManager,
    manifest_manager: ManifestManager<B>,
    flush_pipeline: FlushPipeline,
    compaction_scheduler: CompactionScheduler,
    compaction_executor: CompactionExecutor,
    levels: Vec<LevelState>,
    caching_fetcher: CachingBlockFetcher,            // NEW: wraps DirectBlockFetcher via Arc + BlockCache
    coalescing_fetcher: CoalescingFetcher,           // NEW: wraps clone of caching_fetcher
    block_cache: BlockCache,                          // NEW: shared (moka is internally Arc-based)
    pinned_metadata: PinnedMetadataCache,             // NEW: owned by Engine directly
    continuity_tracker: ContinuityTracker,            // NEW: negative lookup cache
    size_estimator: NamespaceSizeEstimator,           // NEW: adaptive pagination
    config: EngineConfig,
    next_sequence: u64,
    generation_counter: u64,
    pending_deletions: Vec<String>,
}
```

### 9.3 Engine::open() Changes

In `Engine::open()`, construct the cache stack:

1. Create `DirectBlockFetcher::new(backend.clone())`
2. Create `BlockCache::new(&config.cache_config)` — moka cache is internally `Arc`-based, cheap to clone
3. Create `CachingBlockFetcher::new(Arc::new(direct_fetcher), block_cache.clone())`
4. Create `CoalescingFetcher::new(caching_fetcher.clone())` — shares the same `BlockCache` and `Arc<DirectBlockFetcher>`
5. Create `PinnedMetadataCache::new(&config.cache_config)` — owned directly by Engine
6. Create `ContinuityTracker::new(&config.cache_config)`
7. Create `NamespaceSizeEstimator::new()`
8. Use `caching_fetcher` for recovery and SSTableHandle::open calls

**Ownership model:** `BlockCache` is cloned (cheap, Arc-based internally). `CachingBlockFetcher` is cloned into `CoalescingFetcher` (cheap — `Arc<dyn BlockFetcher>` + cloned `BlockCache`). `PinnedMetadataCache` is owned directly by `Engine` (not wrapped in `CacheEvictionHandler`). Cache eviction is done by calling `block_cache.invalidate_sst()` and `pinned_metadata.release()` directly from `Engine::compact()`.

### 9.4 SSTableHandle::open() with PinnedMetadataCache

Modify `SSTableHandle::open()` to accept an optional `&mut PinnedMetadataCache`:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `(meta: SSTableMeta, path: String, fetcher: &dyn BlockFetcher, pinned: Option<&mut PinnedMetadataCache>) -> FlushResult<Self>` | If `pinned` contains metadata for this SSTable ID, skip all 3 fetcher calls and use pinned data. Otherwise, fetch as before and pin the result. |

Alternatively, add a separate constructor to avoid changing the existing signature:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open_with_cache` | `(meta: SSTableMeta, path: String, fetcher: &dyn BlockFetcher, pinned: &mut PinnedMetadataCache) -> FlushResult<Self>` | Checks pinned cache first, falls back to fetcher, pins result. |

The `open()` method remains unchanged for callers that don't have a `PinnedMetadataCache`.

### 9.5 Read Path Changes

Cache-aware logic lives in `Engine` methods, NOT inside `ReadPath`. `ReadPath` remains a stateless helper that takes a fetcher reference. The `Engine` methods wrap `ReadPath` calls with cache checks before and after.

#### Engine::get() (point_read)

1. Check memtables via `ReadPath` (unchanged)
2. **Check continuity tracker:** `self.continuity_tracker.is_known_absent(record_id, item_key, manifest_version)` — if true, return `Ok(None)` immediately
3. Create `ReadBudget::new(self.config.cache_config.get_budget_per_read)`
4. Call `ReadPath::new(&self.caching_fetcher).point_read(...)` — the CachingBlockFetcher transparently handles cache hits/misses
5. Budget enforcement: pass `ReadBudget` to `fetch_block_budgeted` calls within the read path (requires threading budget through SSTableHandle::get)
6. If budget exhausted, return best result found so far

#### Engine::scan() (range_read)

1. Create `ReadBudget`
2. For SSTable sources, use `SSTableHandle::scan_coalesced()` with `self.coalescing_fetcher` instead of `scan()`
3. Track budget across all block fetches
4. If budget exhausted, set `is_partial = true`
5. After building result, call `self.size_estimator.record_items(namespace, total_bytes, entries.len())`
6. Build `PageToken` with `avg_item_size_bytes`
7. After a successful full range read (not partial), call `self.continuity_tracker.mark_range_complete(record_id, start_key, end_key, manifest_version)`

#### Engine::multi_get()

Same as get() per key, with a shared `ReadBudget` across all keys.

### 9.6 Write Path Changes

After each write (`put`, `delete`, `delete_range`):
- Call `continuity_tracker.invalidate_for_record(record_id)` — the write makes cached continuity intervals stale

### 9.7 Compaction Integration

In `Engine::compact()`, after the manifest is updated and levels rebuilt:
1. Call `cache_eviction.evict_result(&result)` — evicts blocks and pinned metadata for consumed SSTables
2. Call `continuity_tracker.invalidate_before_manifest(new_manifest_id)` — invalidates stale continuity intervals

### 9.8 rebuild_levels with PinnedMetadataCache

`Engine::rebuild_levels()` currently opens fresh SSTableHandles for all SSTables in the manifest. With the pinned metadata cache, SSTableHandles that were already opened will find their bloom/index/footer in the cache — only genuinely new SSTables (from compaction output) need StorageBackend fetches.

### 9.9 Engine::close() Changes

In `Engine::close()`, after flushing remaining data, no cache cleanup is needed — the cache drops with the Engine.

### 9.10 Public Cache Stats

Add methods to `Engine` for observability:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `cache_stats` | `(&self) -> CacheStats` | Returns BlockCache stats snapshot |
| `pinned_metadata_count` | `(&self) -> usize` | Number of pinned SSTable metadata entries |
| `continuity_tracked_records` | `(&self) -> usize` | Number of records with continuity intervals |

### 9.11 lib.rs Exports

Re-export from `src/lib.rs`:
- `CacheConfig`
- `CacheStats`
- `ReadBudget` (for advanced callers who want custom budgets)

---

## Tests

**File:** `crates/flushdb-engine/tests/engine_cache_integration_tests.rs`

All tests create a full `Engine` with `LocalFsBackend` and `tempdir`.

### Basic Cache Integration
| Test | What It Validates |
|------|-------------------|
| `test_repeated_get_uses_cache` | Put key, get twice — second get has faster block access (cache hit) |
| `test_scan_populates_cache` | Put 100 items, scan all, then get individual items — all hit cache |
| `test_cache_stats_track_hits` | Perform gets, verify cache_stats().hits > 0 |

### Compaction Eviction
| Test | What It Validates |
|------|-------------------|
| `test_compaction_evicts_stale_blocks` | Put items to trigger flush (L0 SSTables), trigger compaction, verify old L0 blocks evicted from cache |
| `test_compaction_preserves_valid_blocks` | Compact L0→L1, verify L1 blocks remain cached, L0 blocks evicted |

### Pinned Metadata
| Test | What It Validates |
|------|-------------------|
| `test_rebuild_levels_reuses_pinned_metadata` | Open engine, read (pins metadata), trigger compaction (rebuilds levels), verify pinned metadata is reused for surviving SSTables |
| `test_compaction_releases_consumed_metadata` | After compaction, consumed SSTable metadata is released from pinned cache |

### Continuity Tracking
| Test | What It Validates |
|------|-------------------|
| `test_negative_lookup_after_scan` | Scan record [a, z), then get("m") for non-existent key — returns None without StorageBackend call (continuity hit) |
| `test_write_invalidates_continuity` | Scan record, then put new item into record, then get for absent key — continuity invalidated, checks StorageBackend |
| `test_compaction_invalidates_continuity` | Scan record, trigger compaction, get for absent key — continuity invalidated |

### GET Budget
| Test | What It Validates |
|------|-------------------|
| `test_budget_caps_reads` | Create many L0 SSTables (15+), read with budget 8, verify read completes (may be partial) without touching all SSTables |
| `test_range_read_partial_flag` | Trigger budget exhaustion during range read, verify is_partial is true |

### Adaptive Pagination
| Test | What It Validates |
|------|-------------------|
| `test_page_token_carries_avg_item_size` | Scan first page, verify returned token has avg_item_size_bytes populated |
| `test_second_page_uses_prior_avg` | Scan two pages, verify second page's planning is influenced by first page's observed average |

### Coalesced Fetches
| Test | What It Validates |
|------|-------------------|
| `test_range_scan_coalesces_blocks` | Write large SSTable (>5 blocks), scan range — fewer StorageBackend calls than block count |

### End-to-End Correctness
| Test | What It Validates |
|------|-------------------|
| `test_cached_reads_match_uncached` | Run identical read workload with cache enabled vs disabled (cache capacity 0), verify same results |
| `test_full_lifecycle` | Put → flush → compact → scan → get → delete → scan again — all operations correct with caching enabled |

---

## Done When

- [ ] `EngineConfig` includes `CacheConfig`
- [ ] `Engine` creates full cache stack on open (BlockCache → CachingBlockFetcher → CoalescingFetcher)
- [ ] Read path transparently uses cache (callers unaware)
- [ ] Compaction triggers cache eviction for consumed SSTables
- [ ] Writes invalidate continuity tracking for affected records
- [ ] SSTableHandle::open reuses pinned metadata when available
- [ ] GET budget prevents unbounded latency
- [ ] Adaptive pagination tracks and uses per-namespace item size averages
- [ ] `RangeReadResult.is_partial` correctly set when budget exhausted
- [ ] Cache stats exposed via Engine methods
- [ ] All existing engine tests still pass (no behavioral regressions)
- [ ] All new integration tests pass
