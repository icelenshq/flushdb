# Task 6: Coalesced Block Fetches

**Crate:** `flushdb-engine`
**File:** `src/cache/coalescing.rs`
**Depends on:** Task 2 (CachingBlockFetcher)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §12 (coalesced fetches), Phase 6 §7 (Coalesced Block Fetches)

---

## Goal

When a range scan needs to read multiple adjacent data blocks from the same SSTable, merge them into a single `get_range` StorageBackend call instead of issuing one call per block. This reduces round-trips from O(blocks) to O(contiguous_groups) for range scans. Each block in the coalesced response is individually decoded and cached, so subsequent point reads benefit from the prefetched data.

---

## What to Build

### 6.1 BlockRequest

Describes a single block to fetch:

```
BlockRequest {
    block_index: usize,        // Index in the SSTable's IndexBlock
    block_offset: u64,         // Byte offset in SSTable file
    block_size: u32,           // Compressed size in bytes
}
```

Implements `Clone`, `Debug`.

### 6.2 CoalescedFetchResult

Result of a coalesced fetch — one entry per requested block:

```
CoalescedFetchResult {
    blocks: Vec<(usize, Vec<BlockEntry>)>,  // (block_index, decoded entries)
}
```

### 6.3 Adjacency Detection

Two blocks are adjacent if the second block starts immediately after the first:

```
is_adjacent(a: &BlockRequest, b: &BlockRequest) -> bool {
    a.block_offset + a.block_size as u64 == b.block_offset
}
```

### 6.4 CoalescingFetcher

Wraps a `BlockFetcher` and provides a batch-fetch method:

```
CoalescingFetcher {
    inner: CachingBlockFetcher,  // Shared via clone — BlockCache and Arc<dyn BlockFetcher> are cheap to clone
    min_coalesce_size: u32,      // Minimum gap size to consider coalescing (0 = always coalesce adjacent)
}
```

`CachingBlockFetcher` is cheaply clonable because its `BlockCache` (moka) and `Arc<dyn BlockFetcher>` are reference-counted. `Engine` creates one `CachingBlockFetcher` and clones it into `CoalescingFetcher` — both share the same underlying cache and storage backend.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(inner: CachingBlockFetcher) -> Self` | Creates coalescing fetcher with default `min_coalesce_size: 0` |
| `fetch_blocks` | `(&self, sst_path: &str, requests: &[BlockRequest], compression: CompressionType) -> FlushResult<CoalescedFetchResult>` | Groups adjacent requests, issues coalesced reads, splits and decodes responses |

### 6.5 fetch_blocks Algorithm

1. **Sort** requests by `block_offset` (ascending)
2. **Check cache first:** For each request, check if the block is already in `CachingBlockFetcher`'s cache. Partition into cached hits and cache misses.
3. **Group misses into contiguous runs:** Walk the sorted misses. Start a new group whenever two consecutive blocks are NOT adjacent.
4. **For each contiguous group:**
   a. Compute merged range: `offset = first.block_offset`, `total_size = last.block_offset + last.block_size - first.block_offset`
   b. Call `inner.inner().fetch_raw_block(sst_path, offset, total_size).await?` — single StorageBackend GET
   c. **Split** the merged response: for each block in the group, slice `merged_bytes[relative_offset..relative_offset + block_size]`
   d. **Decode** each slice: `decode_block(&slice, compression)?`
   e. **Cache** each decoded block individually via the CachingBlockFetcher's cache
5. **Assemble result:** Combine cached hits and freshly fetched blocks, ordered by block_index.

### 6.6 SSTableHandle::scan_coalesced

Add a new method to `SSTableHandle` that uses `CoalescingFetcher` for range scans:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `scan_coalesced` | `(&self, start: &CompositeKey, end: Option<&CompositeKey>, fetcher: &CoalescingFetcher) -> FlushResult<Vec<BlockEntry>>` | Same semantics as `scan()`, but collects all needed `BlockRequest`s from the index, then calls `fetcher.fetch_blocks()` once instead of fetching blocks one-by-one |

The algorithm:
1. Find the starting block index via binary search (same as current `scan`)
2. Collect all `BlockRequest`s for blocks in the range (checking early termination by first_key)
3. Call `fetcher.fetch_blocks(sst_path, &requests, compression)`
4. Filter entries from the decoded blocks by the `[start, end)` range
5. Return filtered entries

### 6.7 Design Decisions

- **Coalescing happens at the block level, not byte level:** Each individual block is still cached separately. The coalescing is purely an I/O optimization — one large read instead of many small ones.
- **Cache check before coalescing:** If some blocks in a range are already cached, only the missing ones are fetched. This prevents re-fetching cached data.
- **No over-fetch padding:** The current design fetches exactly the bytes covered by the requested blocks. A future optimization could pad to S3's minimum charge size (256 KB) to amortize cost, but this is deferred.
- **`scan_coalesced` is additive:** The existing `scan()` method remains unchanged. `scan_coalesced` is a new method used when a `CoalescingFetcher` is available. Engine integration (Task 9) decides which to call.

---

## Tests

**File:** `crates/flushdb-engine/tests/coalesced_fetch_tests.rs`

All tests write real multi-block SSTables to `LocalFsBackend` with `tempdir`.

### Adjacency Detection
| Test | What It Validates |
|------|-------------------|
| `test_adjacent_blocks` | Blocks at offsets [0, 100) and [100, 200) are adjacent |
| `test_non_adjacent_blocks` | Blocks at offsets [0, 100) and [200, 300) are NOT adjacent |
| `test_single_block_no_coalescing` | Single block request — no coalescing needed, fetched individually |

### Coalesced Fetching
| Test | What It Validates |
|------|-------------------|
| `test_two_adjacent_blocks_one_read` | Write SSTable with 2 adjacent blocks, fetch_blocks issues 1 StorageBackend call, returns correct entries for both blocks |
| `test_three_adjacent_blocks_one_read` | Write SSTable with 3 adjacent blocks, all fetched in 1 call |
| `test_two_groups_two_reads` | Write SSTable with blocks [0,1,2] and [5,6], fetch all 5 — should issue 2 StorageBackend reads (2 contiguous groups) |
| `test_all_cached_no_reads` | Pre-populate cache for all requested blocks, fetch_blocks makes 0 StorageBackend calls |
| `test_partial_cache_hits` | Cache blocks 0 and 2, request blocks 0-3 — only uncached blocks fetched from StorageBackend |

### Block Splitting
| Test | What It Validates |
|------|-------------------|
| `test_split_merged_response` | Coalesced read of 3 blocks, each block decodes to correct entries (no cross-contamination) |
| `test_entries_match_individual_fetches` | Fetch blocks individually vs coalesced — same entries returned |

### scan_coalesced
| Test | What It Validates |
|------|-------------------|
| `test_scan_coalesced_full_range` | Scan entire SSTable with coalescing, results match non-coalesced scan |
| `test_scan_coalesced_partial_range` | Scan subset of blocks, results match non-coalesced scan for same range |
| `test_scan_coalesced_caches_blocks` | After scan_coalesced, individual block fetches hit cache |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_empty_request_list` | fetch_blocks with empty requests returns empty result |
| `test_single_entry_sstable` | SSTable with 1 block — coalescing degrades gracefully to individual fetch |

---

## Done When

- [ ] Adjacent blocks are detected and merged into single StorageBackend reads
- [ ] Coalesced response is correctly split back into individual blocks
- [ ] Each individual block is cached separately after coalesced fetch
- [ ] Already-cached blocks are not re-fetched
- [ ] `scan_coalesced` produces identical results to `scan` but with fewer StorageBackend calls
- [ ] All tests pass
