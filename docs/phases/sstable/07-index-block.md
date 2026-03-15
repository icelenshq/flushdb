# Task 7: IndexBlock — Sparse Block Index

**Crate:** `flushdb-engine`
**File:** `src/sstable/index_block.rs`
**Depends on:** Task 1 (varint, constants)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §7.4, Phase 4 §5

---

## Goal

Build and query the sparse index that maps each data block's first composite key to its byte offset and size in the SSTable file. The index enables binary search for point lookups (narrowing to a single data block) and sequential iteration for range scans. Cached in DRAM by Phase 6 and used by compaction (Phase 5) to determine key range overlaps between levels.

---

## What to Build

### 7.1 IndexEntry Struct

```
IndexEntry {
    first_key: CompositeKey,
    block_offset: u64,
    block_size: u32,
    uncompressed_size: u32,
}
```

**Derives:** `Debug`, `Clone`

### 7.2 IndexBlockBuilder

```
IndexBlockBuilder {
    entries: Vec<IndexEntry>,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty builder |
| `add` | `(&mut self, first_key: CompositeKey, block_offset: u64, block_size: u32, uncompressed_size: u32)` | Appends index entry. Entries MUST be added in sorted key order (caller responsibility — the SSTableWriter iterates in sorted order). |
| `build` | `(self) -> IndexBlock` | Returns `IndexBlock` wrapping the collected entries |
| `entry_count` | `(&self) -> usize` | Number of entries added so far |

### 7.3 IndexBlock

```
IndexBlock {
    entries: Vec<IndexEntry>,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `find_block` | `(&self, key: &CompositeKey) -> Option<&IndexEntry>` | Binary search for the block that could contain `key`: find the **last** entry whose `first_key <= key`. Returns `None` if key is before all blocks (key < entries[0].first_key). |
| `find_block_index` | `(&self, key: &CompositeKey) -> Option<usize>` | Same as `find_block` but returns the index into the entries vec |
| `get` | `(&self, index: usize) -> Option<&IndexEntry>` | Get entry by index. Returns `None` if out of bounds. Used for sequential block iteration in range scans. |
| `block_count` | `(&self) -> usize` | Number of data blocks (= entries.len()) |
| `key_range` | `(&self) -> Option<(&CompositeKey, &CompositeKey)>` | Returns `(first_key of first block, first_key of last block)`. `None` if empty. Used by compaction for overlap detection. |
| `overlaps` | `(&self, start: &CompositeKey, end: &CompositeKey) -> bool` | Returns `true` if any block's key range could overlap `[start, end]`. Returns `false` if the index's key range is entirely before `start` or entirely after `end`. Used by compaction for level overlap queries. |
| `serialize` | `(&self) -> Bytes` | Encodes the entire index block (see §7.5) |
| `deserialize` | `(data: &[u8]) -> FlushResult<Self>` | Parses and reconstructs the index block |
| `iter` | `(&self) -> impl Iterator<Item = &IndexEntry>` | Iterate entries in sorted order |

### 7.4 Binary Search Details

`find_block` locates the candidate block for a given key:

1. Compute `partition_point = entries.partition_point(|e| e.first_key.as_bytes() <= key.as_bytes())`
2. If `partition_point == 0`, no block has `first_key <= key` → return `None`
3. Otherwise return `entries[partition_point - 1]`

This finds the last block whose first_key is <= the target key. That block is the only candidate for containing the key (since blocks are sorted and non-overlapping within an SSTable).

### 7.5 Serialization Format

```
[entry_count: u32 LE]
For each entry:
  [key_len: varint]
  [key_bytes: bytes]               // CompositeKey::as_bytes()
  [block_offset: u64 LE]
  [block_size: u32 LE]
  [uncompressed_size: u32 LE]
```

### 7.6 Validation on Deserialize

| Check | Error |
|-------|-------|
| Data too short for entry_count header | `FlushError::CorruptedData { message: "index block too short" }` |
| Truncated entry data | `FlushError::CorruptedData { message: "index block entry truncated" }` |
| Invalid CompositeKey (no separator) | Propagated from `CompositeKey::from_bytes()` |

### 7.7 Overlap Detection

`overlaps(start, end)` logic:
1. If index is empty → `false`
2. Let `(index_min, index_max)` = `key_range()`
3. If `index_max < start` → `false` (index entirely before query range)
4. If `index_min > end` → `false` (index entirely after query range)
5. Otherwise → `true`

Note: this is a conservative check using only index-level key range, not per-block granularity. It may produce false positives but never false negatives — suitable for compaction's overlap detection where false positives just mean an unnecessary compaction candidate.

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_index_block_tests.rs`

### Binary Search Tests
| Test | What It Validates |
|------|-------------------|
| `test_find_block_exact_match` | Key equals `first_key` of block 2 → returns block 2 |
| `test_find_block_between_keys` | Key falls between block 1 and block 2's first_keys → returns block 1 |
| `test_find_block_before_all` | Key before first block's first_key → returns `None` |
| `test_find_block_after_last` | Key after last block's first_key → returns last block |
| `test_find_block_single_entry` | Index with 1 entry: key at/after first_key → found; key before → `None` |

### Key Range Tests
| Test | What It Validates |
|------|-------------------|
| `test_key_range_multiple_blocks` | Returns (first block's first_key, last block's first_key) |
| `test_key_range_empty` | Empty index → `None` |
| `test_overlaps_disjoint_before` | Query range entirely before index range → `false` |
| `test_overlaps_disjoint_after` | Query range entirely after index range → `false` |
| `test_overlaps_contained` | Query range within index range → `true` |
| `test_overlaps_partial` | Partially overlapping range → `true` |

### Serialization Tests
| Test | What It Validates |
|------|-------------------|
| `test_serialize_deserialize_round_trip` | 10 entries: all fields preserved through serialize → deserialize |
| `test_serialize_empty_index` | Empty index serializes as `[count=0]` (4 bytes), deserializes to empty |
| `test_deserialize_rejects_truncated` | Truncated data → `CorruptedData` error |

### Ordering Tests
| Test | What It Validates |
|------|-------------------|
| `test_entries_in_key_order` | After building, `entries[i].first_key < entries[i+1].first_key` for all i |

---

## Done When

- [ ] Binary search finds correct block for keys at exact match, between blocks, and after last block
- [ ] `find_block` returns `None` for keys before all blocks
- [ ] `key_range()` returns correct min/max first_keys
- [ ] `overlaps()` correctly detects disjoint and overlapping ranges
- [ ] Serialize/deserialize round-trip preserves all entries and fields
- [ ] Empty index serializes and deserializes correctly
- [ ] `get(index)` provides sequential access for range iteration
- [ ] All tests pass
