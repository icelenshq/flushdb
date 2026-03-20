# Task 2: CachingBlockFetcher

**Crate:** `flushdb-engine`
**File:** `src/cache/caching_fetcher.rs`
**Depends on:** Task 1 (BlockCache, CachedBlock, BlockCacheKey)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §12 (Three-Tier Cache Architecture), Phase 6 §3 (Logical Cache)

---

## Goal

Implement a `BlockFetcher` that transparently caches decoded data blocks. Callers — `SSTableHandle::get()`, `SSTableHandle::scan()`, `ReadPath`, compaction — see the same `BlockFetcher` interface and don't know whether data came from cache or StorageBackend. This is the primary read-path optimization: cache hits skip StorageBackend I/O, decompression, and block decoding.

---

## What to Build

### 2.1 CachingBlockFetcher

A struct that implements the `BlockFetcher` trait by checking `BlockCache` before delegating to an inner fetcher:

```
CachingBlockFetcher {
    inner: Arc<dyn BlockFetcher>,   // Arc (not Box) — shared with CoalescingFetcher
    cache: BlockCache,              // moka Cache is internally Arc-based, cheap to clone
}
```

Using `Arc<dyn BlockFetcher>` (not `Box`) because `CoalescingFetcher` (Task 6) and `Engine` (Task 9) both need access to the fetcher. `Arc` without `Mutex` is permitted — `BlockFetcher` is `Send + Sync` and all methods take `&self`.

### 2.2 Construction

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(inner: Arc<dyn BlockFetcher>, cache: BlockCache) -> Self` | Wraps an existing fetcher (typically `DirectBlockFetcher`) with a cache layer |
| `cache` | `(&self) -> &BlockCache` | Returns a reference to the underlying BlockCache for stats/management |
| `inner` | `(&self) -> &dyn BlockFetcher` | Returns a reference to the inner fetcher |

### 2.3 BlockFetcher Implementation

#### `fetch_block(sst_path, offset, size, compression) -> FlushResult<Vec<BlockEntry>>`

1. Extract SSTable ID from `sst_path` (last path segment minus `.sst` extension)
2. Build `BlockCacheKey { sst_id, block_offset: offset }`
3. Check cache: `cache.get(&key)`
   - **Hit:** Return `cached_block.entries().to_vec()`, done
   - **Miss:** Continue to step 4
4. Call `inner.fetch_block(sst_path, offset, size, compression).await?`
5. Create `CachedBlock::new(entries.clone())`
6. Insert into cache: `cache.insert(key, cached_block)`
7. Return entries

**SSTable ID extraction:** The `sst_path` follows the pattern `flushdb/{namespace}/sstables/{level}/{id}.sst` or `flushdb/{namespace}/sstables/{level}/run-{run_id}/frag-{idx}.sst`. The ID is extracted as the filename minus `.sst` — this matches `SSTableMeta.id`.

Provide a helper function:

| Function | Signature | Behavior |
|----------|-----------|----------|
| `sst_id_from_path` | `(path: &str) -> &str` | Extracts filename stem from SSTable path. Splits on `/`, takes last segment, strips `.sst` suffix. |

#### `fetch_raw_block(sst_path, offset, size) -> FlushResult<Bytes>`

Pass through directly to `inner.fetch_raw_block()` — **do not cache raw blocks**. Raw blocks are used for metadata (bloom filters, index blocks, footers) which are pinned separately by `PinnedMetadataCache` (Task 3). Caching them here would double-count memory.

### 2.4 Design Decisions

- **Cache stores decoded entries, not raw bytes:** This is the "logical cache" design from STORAGE_DESIGN.md §12. Cache hits return `Vec<BlockEntry>` with zero decompression/parsing cost.
- **`fetch_raw_block` is NOT cached:** Raw block reads are used exclusively for metadata loading (bloom, index, footer). These are pinned by `PinnedMetadataCache` (Task 3) and should not compete with data block cache capacity.
- **Clone on cache hit:** `entries().to_vec()` clones the `Vec<BlockEntry>`. `BlockEntry` contains `CompositeKey` (backed by `Bytes` — reference-counted) and `Bytes` for value/metadata, so cloning is cheap pointer increments, not deep copies.
- **No deduplication of concurrent fetches:** If two reads request the same block simultaneously and both miss, both will fetch from StorageBackend and insert. The second insert overwrites the first — this is benign and avoids a coordination lock on the hot path. A future optimization could use `moka`'s `get_with()` for single-flight loading.

### 2.5 Trait Does NOT Include

- **No prefetching** — prefetching is handled by `CoalescingFetcher` (Task 6)
- **No budget tracking** — GET budget is handled by `ReadBudget` (Task 7)
- **No continuity tracking** — handled by `ContinuityTracker` (Task 5)

---

## Tests

**File:** `crates/flushdb-engine/tests/caching_fetcher_tests.rs`

All tests use `LocalFsBackend` with `tempdir` and write real SSTables for realistic block fetching.

### Cache Hit/Miss
| Test | What It Validates |
|------|-------------------|
| `test_first_fetch_misses_cache` | First fetch_block for a key calls inner fetcher, returns correct entries |
| `test_second_fetch_hits_cache` | Second fetch_block for same key returns same entries without calling inner fetcher |
| `test_different_blocks_independent` | Fetching block A then block B — both cache independently, both hit on second fetch |
| `test_different_ssts_independent` | Same block_offset in two different SSTables — cached independently |

### fetch_raw_block Passthrough
| Test | What It Validates |
|------|-------------------|
| `test_raw_block_not_cached` | Call fetch_raw_block, then call it again — inner fetcher called both times (no caching) |
| `test_raw_block_returns_correct_bytes` | fetch_raw_block returns exact bytes from StorageBackend |

### Cache Invalidation Integration
| Test | What It Validates |
|------|-------------------|
| `test_invalidate_sst_clears_cached_blocks` | Fetch blocks from SST "A", call cache.invalidate_sst("A"), subsequent fetches miss cache |
| `test_invalidate_does_not_affect_other_ssts` | Fetch from SST "A" and "B", invalidate "A", "B" blocks still hit cache |

### sst_id_from_path Helper
| Test | What It Validates |
|------|-------------------|
| `test_sst_id_from_l0_path` | `"flushdb/ns/sstables/L0/abc123.sst"` → `"abc123"` |
| `test_sst_id_from_fragment_path` | `"flushdb/ns/sstables/L1/run-xyz/frag-0001.sst"` → `"frag-0001"` |
| `test_sst_id_from_simple_filename` | `"data.sst"` → `"data"` |

### End-to-End with SSTableHandle
| Test | What It Validates |
|------|-------------------|
| `test_sstable_point_read_caches_block` | Write SSTable, open handle with CachingBlockFetcher, point read caches the data block, second read hits cache |
| `test_sstable_scan_caches_blocks` | Write SSTable with multiple blocks, scan caches all fetched blocks |

---

## Done When

- [ ] `CachingBlockFetcher` implements `BlockFetcher` trait
- [ ] `fetch_block` checks cache before calling inner fetcher
- [ ] `fetch_raw_block` passes through without caching
- [ ] Cache hits return correct decoded entries
- [ ] `sst_id_from_path` correctly extracts SSTable ID from all path formats
- [ ] SSTableHandle reads work transparently with CachingBlockFetcher
- [ ] All tests pass
