# Task 8: Range Read Path & Pagination

**Crate:** `flushdb-engine`
**File:** `src/read_path.rs` (extends Task 7)
**Depends on:** Task 4 (BlockFetcher, SSTableHandle, LevelState), Task 5 (MergeIterator)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §11.3 (Range Read), §13 (Byte-Based Pagination), §6.5 (Range Tombstone Index)

---

## Goal

Implement range reads that merge-sort entries across all layers using the MergeIterator, apply tombstone filtering (both point and range tombstones), and support byte-based pagination with resumable page tokens. This enables GetItems with `match_range`, `match_all`, and `match_keys` predicates.

---

## What to Build

### 8.1 Range Read Method

Add to `ReadPath`:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `range_read` | `async (&self, record_id: &[u8], start_key: Option<&[u8]>, end_key: Option<&[u8]>, options: RangeReadOptions, memtable_list: &MemtableList, levels: &[LevelState]) -> FlushResult<RangeReadResult>` | Full range read across all layers with merge-sort, tombstone filtering, and pagination. |

### 8.2 RangeReadOptions

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `page_size_bytes` | `usize` | `2 * 1024 * 1024` | Byte budget for results (2 MB) |
| `item_limit` | `Option<usize>` | `None` | Max number of items to return |
| `resume_from` | `Option<PageToken>` | `None` | Resume from this page token |

### 8.3 PageToken

Encodes the position to resume a paginated read.

| Field | Type | Description |
|-------|------|-------------|
| `last_composite_key` | `CompositeKey` | The last key returned in the previous page |
| `last_sequence_number` | `u64` | Sequence number of the last entry (for tiebreaking) |

**Serialization:** Encode as binary: `[key_length: u32][key_bytes][sequence: u64]`. Expose as base64 string for API transport.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `encode` | `(&self) -> Bytes` | Binary-encodes the page token |
| `decode` | `(data: &[u8]) -> FlushResult<Self>` | Decodes from binary |
| `to_base64` | `(&self) -> String` | Base64 encoding for API response |
| `from_base64` | `(s: &str) -> FlushResult<Self>` | Decode from base64 |

### 8.4 RangeReadResult

| Field | Type | Description |
|-------|------|-------------|
| `entries` | `Vec<MergeEntry>` | Result entries (Put only — tombstones filtered out) |
| `next_page_token` | `Option<PageToken>` | Token for the next page, None if no more results |
| `total_bytes` | `usize` | Total bytes of returned entries |

### 8.5 Range Read Algorithm

**Step 1: Determine key range**
- If `start_key` is None: use `CompositeKey::min_key_for_record(record_id)`
- If `end_key` is None: use `CompositeKey::max_key_for_record(record_id)` (match_all pattern)
- If `resume_from` is Some: use `resume_from.last_composite_key` as the start (seek past it)
- Construct start/end as `CompositeKey::new(record_id, start_key)` and `CompositeKey::new(record_id, end_key)`

**Step 2: Collect sources for MergeIterator**
- Active memtable: `memtable_list.scan(start, end)` → convert to `VecSource` with source_id 0
- Frozen memtable(s): same scan → `VecSource` with incrementing source_ids
- For each level (L0-L3):
  - Find candidate SSTables via `level_state.find_candidates_for_range(start, end)`
  - For each candidate: `handle.scan(start, Some(end), fetcher)` to get block entries
  - Convert to `VecSource` with incrementing source_ids

**Step 3: Create MergeIterator and iterate**
- Create `MergeIterator::new(sources)`
- Call `next_deduped()` repeatedly
- For each entry:
  - Skip if not within the target record (defensive — bloom filter may have false positives)
  - Skip if it's a tombstone (Delete/RangeDelete)
  - Check range tombstone coverage: if any range tombstone from any layer covers this key with a higher sequence → skip
  - Accumulate entry into results
  - Track `total_bytes` (value.len() + metadata.len() + key.len())
  - Stop if `total_bytes >= page_size_bytes` or `entries.len() >= item_limit`

**Step 4: Build page token**
- If iterator is not exhausted after stopping: create `PageToken` from the last returned entry
- If iterator is exhausted: `next_page_token = None`

### 8.6 Range Tombstone Filtering

During range reads, range tombstones must be checked from multiple sources:

1. **Memtable range tombstones:** Check `memtable.range_tombstones().covers(record_id, item_key, entry_sequence)`
2. **Frozen memtable range tombstones:** Check each frozen memtable's `RangeTombstoneIndex`
3. **SSTable range tombstones:** Range tombstone entries appear in the merge stream as `EntryType::RangeDelete`. When encountered:
   - Don't emit them as results
   - Track them in a `RangeTombstoneCollector` so subsequent entries can be checked against them

### 8.7 RangeTombstoneCollector

A temporary structure used during a single range read to collect range tombstones discovered across all sources:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty collector |
| `add_from_memtables` | `(&mut self, memtable_list: &MemtableList)` | Adds all range tombstones from active + frozen memtables |
| `add` | `(&mut self, record_id: &[u8], start_key: &[u8], end_key: &[u8], sequence: u64)` | Adds a range tombstone discovered during merge |
| `covers` | `(&self, record_id: &[u8], item_key: &[u8], entry_sequence: u64) -> bool` | Returns true if any tombstone with higher sequence covers this key |

### 8.8 Match Keys (Multi-Get)

For `match_keys` predicate (fetch specific keys by name within a record):

| Method | Signature | Behavior |
|--------|-----------|----------|
| `multi_get` | `async (&self, record_id: &[u8], keys: &[&[u8]], memtable_list: &MemtableList, levels: &[LevelState]) -> FlushResult<Vec<Option<MergeEntry>>>` | Performs point reads for each key. Returns results in same order as input keys. Uses bloom filters to batch-eliminate SSTables. |

Optimization: check bloom filter once per SSTable for the record_id, then only scan matching SSTables for all requested keys.

---

## Tests

**File:** `crates/flushdb-engine/tests/range_read_tests.rs`

### Basic Range Tests
| Test | What It Validates |
|------|-------------------|
| `test_range_read_single_record` | All items in a record returned in sorted order |
| `test_range_read_bounded_range` | Only items within [start, end] returned |
| `test_range_read_empty_record` | No items for record_id → empty result |
| `test_range_read_match_all` | No start/end → all items in record |

### Cross-Layer Tests
| Test | What It Validates |
|------|-------------------|
| `test_range_read_merges_memtable_and_sstable` | Items from both memtable and SSTable merged correctly |
| `test_range_read_dedup_across_layers` | Same key in memtable and SSTable → newest wins |
| `test_range_read_memtable_update_overrides_sstable` | Updated value in memtable replaces SSTable value |

### Tombstone Filtering Tests
| Test | What It Validates |
|------|-------------------|
| `test_range_read_point_tombstone_filters_entry` | Deleted key excluded from results |
| `test_range_read_range_tombstone_filters_entries` | Range tombstone shadows covered keys |
| `test_range_read_range_tombstone_from_memtable` | Range tombstone in memtable shadows SSTable entries |
| `test_range_read_tombstone_sequence_ordering` | Put with seq > tombstone seq → Put survives |
| `test_range_read_entries_not_covered_survive` | Keys outside tombstone range unaffected |

### Pagination Tests
| Test | What It Validates |
|------|-------------------|
| `test_pagination_byte_budget` | Stops at byte budget, returns page token |
| `test_pagination_resume` | Resume with page token → next page starts after last key |
| `test_pagination_no_gaps` | Concatenating all pages produces complete result set |
| `test_pagination_no_duplicates` | No entry appears in two pages |
| `test_pagination_item_limit` | `item_limit` caps results per page |
| `test_pagination_last_page_no_token` | Final page returns `next_page_token = None` |
| `test_pagination_empty_subsequent_page` | Resume past all data → empty result, no token |

### Page Token Tests
| Test | What It Validates |
|------|-------------------|
| `test_page_token_encode_decode_roundtrip` | Binary encode/decode preserves key and sequence |
| `test_page_token_base64_roundtrip` | Base64 encode/decode roundtrip |
| `test_page_token_invalid_data` | Invalid bytes → error |

### Multi-Get Tests
| Test | What It Validates |
|------|-------------------|
| `test_multi_get_all_found` | All requested keys exist → all returned |
| `test_multi_get_some_missing` | Missing keys → None in corresponding positions |
| `test_multi_get_tombstoned_key` | Deleted key → None |
| `test_multi_get_preserves_input_order` | Results in same order as input keys |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_range_read_single_item` | Record with one item → returned correctly |
| `test_range_read_large_result` | 10K items across multiple SSTables → all merged correctly |
| `test_range_read_cross_block_boundaries` | Items spanning multiple SSTable blocks → seamless |

---

## Done When

- [ ] Range reads merge-sort across memtable + frozen + L0-L3 correctly
- [ ] Point tombstones filter out deleted keys
- [ ] Range tombstones shadow covered keys with proper sequence number ordering
- [ ] Byte-based pagination stops at budget and produces correct page tokens
- [ ] Page token resume produces no gaps and no duplicates
- [ ] `match_all` pattern (no start/end) returns full record
- [ ] `match_keys` multi-get returns results in input order
- [ ] RangeTombstoneCollector aggregates tombstones from all sources
- [ ] All tests pass
