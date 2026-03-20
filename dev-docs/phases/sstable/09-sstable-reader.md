# Task 9: SSTableReader — End-to-End Reader

**Crate:** `flushdb-engine`
**File:** `src/sstable/reader.rs`
**Depends on:** Task 1 (types), Task 2 (footer/header), Task 4 (BlockReader), Task 5 (BloomFilter), Task 6 (DedupBlock), Task 7 (IndexBlock)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §7, Phase 4 §8

---

## Goal

Read an SSTable from `StorageBackend` with lazy metadata loading. The reader opens by reading the footer (last 80 bytes via `get_range`), then loads the bloom filter, index block, and dedup block on demand. Point lookups use bloom check → index binary search → single data block fetch → key scan. Range iteration seeks to the start block via index and iterates blocks forward. Block-level random access is exposed separately to enable future cache integration (Phase 6).

---

## What to Build

### 9.1 SSTableReader

```
SSTableReader<B: StorageBackend> {
    backend: B,
    path: String,
    file_size: u64,
    footer: SstFooter,
    filter: Option<FilterBlock>,
    index: Option<IndexBlock>,
    dedup: Option<DedupBlock>,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `async (backend: B, path: String, file_size: u64) -> FlushResult<Self>` | Reads footer (last 80 bytes via `backend.get_range(path, file_size - 80, 80)`). Validates magic and CRC. Sets `filter`, `index`, `dedup` to `None`. Does NOT load metadata yet. |
| `load_metadata` | `async (&mut self) -> FlushResult<()>` | Loads bloom filter, index block, and dedup block via three `get_range` calls. Parses all three. Dedup block offset computed from `footer.dedup_block_offset()`. |
| `get` | `async (&self, key: &CompositeKey) -> FlushResult<Option<BlockEntry>>` | Point lookup pipeline (§9.2). Returns `None` if key not found. Requires metadata loaded. |
| `get_block` | `async (&self, block_index: usize) -> FlushResult<Vec<BlockEntry>>` | Fetch and decode a single data block by index. Uses `index.get(block_index)` for offset/size, then `backend.get_range`. Exposed separately for cache integration. |
| `scan` | `async (&self, start: &CompositeKey, end: Option<&CompositeKey>) -> FlushResult<Vec<BlockEntry>>` | Range scan pipeline (§9.3). Returns entries in `[start, end)`. Requires metadata loaded. |
| `contains_record` | `(&self, record_id: &[u8]) -> FlushResult<bool>` | Bloom filter check. Returns `InvalidArgument` if metadata not loaded. |
| `check_dedup` | `(&self, token: &IdempotencyToken) -> FlushResult<bool>` | Dedup block check. Returns `InvalidArgument` if metadata not loaded. |
| `footer` | `(&self) -> &SstFooter` | Returns reference to parsed footer |
| `key_range` | `(&self) -> FlushResult<Option<(&CompositeKey, &CompositeKey)>>` | Delegates to `index.key_range()`. Returns `InvalidArgument` if metadata not loaded. |
| `entry_count` | `(&self) -> u64` | Returns `footer.entry_count` |
| `compression` | `(&self) -> CompressionType` | Returns `footer.compression_type` |
| `block_count` | `(&self) -> FlushResult<usize>` | Returns `index.block_count()`. Returns `InvalidArgument` if metadata not loaded. |

### 9.2 Point Lookup Pipeline

```
1. Check metadata loaded (filter and index must be Some)
2. Footer key range pre-filter:
   if !footer.may_contain_key(key) → return Ok(None)
3. Bloom filter check:
   if !filter.maybe_contains(key.record_id()) → return Ok(None)
4. Index binary search:
   index.find_block(key) → block_entry
   if None → return Ok(None)
5. Fetch data block:
   backend.get_range(path, block_entry.block_offset, block_entry.block_size) → raw_bytes
6. Decode block:
   decode_block(raw_bytes, footer.compression_type) → entries
7. Scan for exact key:
   entries.iter().find(|e| e.composite_key == *key) → return match or None
```

### 9.3 Range Scan Pipeline

```
1. Check metadata loaded
2. Find start block:
   index.find_block_index(start) → start_idx
   If None (start before all blocks), start_idx = 0
3. Iterate blocks from start_idx to index.block_count():
   a. Fetch block via get_block(block_idx) → entries
   b. For each entry:
      - Skip entries where entry.composite_key < start
      - If end is Some and entry.composite_key >= end → stop iteration
      - Otherwise collect entry
   c. Optimization: if first entry of next block has first_key >= end → stop
4. Return collected entries
```

### 9.4 SstableIterator

Full-table iterator that streams entries across blocks lazily:

```
SstableIterator<'a, B: StorageBackend> {
    reader: &'a SSTableReader<B>,
    current_block_idx: usize,
    current_entries: Vec<BlockEntry>,
    current_entry_idx: usize,
}
```

Exposes an async iteration pattern (since block fetches are async):

| Method | Signature | Behavior |
|--------|-----------|----------|
| `next` | `async (&mut self) -> FlushResult<Option<BlockEntry>>` | Returns next entry. When current block is exhausted, fetches and decodes the next block via `reader.get_block()`. Returns `None` after all blocks consumed. |

Note: Since `StorageBackend` methods are async, the iterator uses an explicit `async next()` method rather than implementing `std::iter::Iterator`. Callers use a `while let Some(entry) = iter.next().await?` loop.

### 9.5 Block-Level Access

`get_block(block_index)` is exposed as a public method separately from `get()` and `scan()` to enable Phase 6 cache integration. The cache layer will wrap `SSTableReader` and intercept `get_block` calls to serve from DRAM/NVMe when cached. The separation ensures the cache can intercept at the block granularity without modifying the point lookup or scan logic.

### 9.6 Error Handling

| Condition | Error |
|-----------|-------|
| Footer magic invalid | `FlushError::CorruptedData { message: "invalid SSTable magic" }` |
| Footer CRC mismatch | `FlushError::CrcMismatch { expected, actual }` |
| Metadata not loaded when calling get/scan/contains_record/check_dedup/key_range | `FlushError::InvalidArgument { message: "SSTable metadata not loaded — call load_metadata() first" }` |
| Block fetch failure | Propagated `FlushError` from `backend.get_range()` |
| Block CRC corruption | `FlushError::CrcMismatch` from block reader |
| Block index out of bounds | `FlushError::InvalidArgument { message: "block index {idx} out of range (0..{count})" }` |

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_reader_tests.rs`

All tests write SSTables using `SSTableWriter`, then read using `SSTableReader`. Use `LocalFsBackend` with `tempdir`. The file_size is obtained via `backend.get(path).await?.len()`.

### Write-Read Round-trip Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_read_single_entry` | Write 1 entry → open → load_metadata → get(key) → all fields match |
| `test_write_read_100_entries` | Write 100 sorted entries → iterate all → exact match on every entry |
| `test_write_read_all_entry_types` | Put, Delete, RangeDelete survive write → read with correct `entry_type` |
| `test_write_read_all_compression_types` | None, Snappy, Zstd: write → read → all entries match |

### Point Lookup Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_existing_key` | `get(key)` returns correct `BlockEntry` with matching composite_key, value, metadata |
| `test_get_nonexistent_key` | `get(absent_key)` returns `Ok(None)` |
| `test_get_bloom_eliminates_missing_record` | `get(key_with_absent_record_id)` returns `None` without fetching any data block (verify via entry count or by checking bloom directly) |
| `test_get_key_in_non_first_block` | Write entries spanning multiple blocks, get key in last block → correct entry |
| `test_get_first_and_last_key` | `get(min_key)` and `get(max_key)` both return correct entries |

### Range Scan Tests
| Test | What It Validates |
|------|-------------------|
| `test_scan_full_range` | `scan(min_key, None)` → returns all entries in sorted order |
| `test_scan_subset` | `scan(start, Some(end))` → only entries in `[start, end)` returned |
| `test_scan_single_record` | `scan` within one record_id → contiguous items for that record |
| `test_scan_across_blocks` | Range spanning 3+ data blocks → all entries from all blocks returned in order |
| `test_scan_empty_range` | `start >= end` → empty result |

### Metadata Tests
| Test | What It Validates |
|------|-------------------|
| `test_entry_count_matches` | `entry_count()` matches number of written entries |
| `test_key_range` | `key_range()` returns first_key of first block and first_key of last block |
| `test_contains_record_present` | `contains_record(present_record_id)` → `true` |
| `test_contains_record_absent` | `contains_record(absent_record_id)` → `false` |

### Dedup Tests
| Test | What It Validates |
|------|-------------------|
| `test_check_dedup_present_token` | Token from written entries → `true` |
| `test_check_dedup_absent_token` | Random token not in SSTable → `false` |

### Iterator Tests
| Test | What It Validates |
|------|-------------------|
| `test_iterator_all_entries_sorted` | `SstableIterator` produces all entries, each `composite_key >= previous` |
| `test_iterator_crosses_block_boundaries` | Iterator seamlessly transitions between blocks (no gaps, no duplicates) |

### Error Handling Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_without_metadata_errors` | Call `get()` before `load_metadata()` → `InvalidArgument` |

---

## Done When

- [ ] Writer → Reader round-trip preserves all entries exactly
- [ ] Point lookup returns correct entry for existing keys and `None` for absent keys
- [ ] Bloom filter eliminates lookups for absent record_ids
- [ ] Footer key range pre-filter works correctly
- [ ] Range scan returns correct subset in sorted order
- [ ] Range scan spanning multiple blocks works seamlessly
- [ ] `SstableIterator` yields all entries in sorted order across blocks
- [ ] Dedup block check works for present and absent tokens
- [ ] All three compression modes work end-to-end
- [ ] Metadata-not-loaded produces clear `InvalidArgument` error
- [ ] `get_block()` exposed for cache integration
- [ ] All tests pass
