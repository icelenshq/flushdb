# Task 4: Range Tombstone Index

**Crate:** `flushdb-engine`
**File:** `src/range_tombstone.rs`
**Depends on:** Nothing
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §6.5, Phase 3 §3

---

## Goal

Build a secondary index for range tombstones that enables efficient read-time filtering. During point lookups, this index determines whether a found entry is shadowed by a range tombstone with a higher sequence number. The `covers()` API must be reusable — Phase 5c will use the same interface for SSTable range tombstone checks.

---

## What to Build

### 4.1 RangeTombstone Struct

```
RangeTombstone {
    record_id: Bytes          // record this tombstone applies to
    start_key: Bytes          // inclusive lower bound of deleted range
    end_key: Bytes            // exclusive upper bound of deleted range
    sequence_number: u64      // tombstone's sequence number — only shadows entries with lower seq
}
```

**Derives:** `Debug`, `Clone`, `PartialEq`, `Eq`

**Design decisions:**
- `start_key` and `end_key` are item keys (not composite keys) — the range tombstone applies within a single record
- `start_key` is inclusive, `end_key` is exclusive — matches standard range semantics `[start, end)`
- Empty `start_key` means "from the beginning of the record"
- Empty `end_key` means "to the end of the record" (deletes everything from `start_key` onward)

### 4.2 RangeTombstoneIndex Struct

```
RangeTombstoneIndex {
    tombstones: Vec<RangeTombstone>    // sorted by (record_id, start_key) for binary search
}
```

**Design decisions:**
- **Sorted `Vec`** rather than a tree — inserts are append + re-sort (or insertion sort if maintaining order). For the expected number of range tombstones per memtable (typically 0-100), a sorted vec with binary search is faster than a tree due to cache locality.
- **Binary search by `(record_id, start_key)`** — for a point lookup against record R and item key K, binary search narrows to tombstones for record R, then linear scan checks if K falls within any tombstone's range.

### 4.3 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty index |
| `add` | `(&mut self, tombstone: RangeTombstone)` | Inserts tombstone, maintaining sorted order by `(record_id, start_key)`. Uses `Vec::binary_search_by` to find insertion point. |
| `covers` | `(&self, record_id: &[u8], item_key: &[u8], entry_sequence: u64) -> bool` | Returns `true` if any tombstone for this `record_id` has `start_key <= item_key < end_key` AND `tombstone.sequence_number > entry_sequence`. This is the read-time check. |
| `tombstones_for_record` | `(&self, record_id: &[u8]) -> &[RangeTombstone]` | Returns slice of all tombstones for a given record (contiguous in sorted vec). Returns empty slice if no tombstones for this record. |
| `len` | `(&self) -> usize` | Number of tombstones |
| `is_empty` | `(&self) -> bool` | True if no tombstones |
| `iter` | `(&self) -> impl Iterator<Item = &RangeTombstone>` | Iterates all tombstones in sorted order (used during flush to persist tombstones to SSTable) |

### 4.4 Covers Algorithm

```
covers(record_id, item_key, entry_sequence):
  // Binary search to find first tombstone for this record_id
  // Then linear scan through tombstones for this record
  for ts in tombstones_for_record(record_id):
    if ts.start_key <= item_key:
      // Check end_key: empty end_key means "unbounded"
      let in_range = ts.end_key.is_empty() || item_key < ts.end_key
      if in_range && ts.sequence_number > entry_sequence:
        return true
    // Since tombstones are sorted by start_key, if start_key > item_key,
    // no further tombstone can cover this key
    if ts.start_key > item_key:
      break
  return false
```

### 4.5 Comparison Function for Sorting

```
compare_tombstones(a, b) -> Ordering:
  match a.record_id.cmp(&b.record_id):
    Equal => a.start_key.cmp(&b.start_key)
    other => other
```

### 4.6 Future Phase Considerations

- **Merge-read path (Phase 5c):** The read path checks range tombstones across active memtable, frozen memtables, AND SSTables. The `covers()` method signature is designed to be reusable — consider extracting a `RangeTombstoneChecker` trait if SSTable range tombstones need a different backing store.
- **Flush pipeline (Phase 5b):** During flush, range tombstones are written to the SSTable's range tombstone block. The `iter()` method provides the ordered sequence needed for this.

---

## Tests

**File:** `crates/flushdb-engine/tests/range_tombstone_tests.rs`

### Basic Coverage Tests
| Test | What It Validates |
|------|-------------------|
| `test_covers_key_in_range` | Tombstone [a, d) with seq 10 covers key "b" with seq 5 |
| `test_does_not_cover_key_outside_range` | Tombstone [a, d) does NOT cover key "e" |
| `test_does_not_cover_key_at_end_exclusive` | Tombstone [a, d) does NOT cover key "d" (end is exclusive) |
| `test_covers_key_at_start_inclusive` | Tombstone [a, d) covers key "a" (start is inclusive) |
| `test_does_not_cover_higher_sequence` | Tombstone with seq 5 does NOT cover entry with seq 10 |
| `test_covers_lower_sequence` | Tombstone with seq 10 covers entry with seq 5 |
| `test_does_not_cover_equal_sequence` | Tombstone with seq 5 does NOT cover entry with seq 5 (strictly greater) |

### Unbounded Range Tests
| Test | What It Validates |
|------|-------------------|
| `test_empty_end_key_covers_all_after_start` | Tombstone with empty end_key covers all keys >= start_key |
| `test_empty_start_key_covers_from_beginning` | Tombstone with empty start_key covers all keys < end_key |
| `test_both_empty_covers_entire_record` | Tombstone with empty start and end covers all keys in record |

### Multi-Record Tests
| Test | What It Validates |
|------|-------------------|
| `test_tombstone_scoped_to_record` | Tombstone for "record-A" does NOT cover keys in "record-B" |
| `test_multiple_records_independent` | Separate tombstones for different records don't interfere |
| `test_tombstones_for_record_returns_correct_slice` | tombstones_for_record returns only tombstones for the given record |
| `test_tombstones_for_record_empty` | tombstones_for_record for absent record returns empty slice |

### Overlapping Tombstone Tests
| Test | What It Validates |
|------|-------------------|
| `test_overlapping_tombstones_any_covers` | Two overlapping tombstones — key covered if ANY tombstone covers it |
| `test_overlapping_different_sequences` | Two tombstones with different sequences — higher-seq one covers, lower doesn't |
| `test_adjacent_tombstones` | Tombstones [a, c) and [c, f) — key "c" is covered by second, not first |

### Ordering Tests
| Test | What It Validates |
|------|-------------------|
| `test_tombstones_maintained_sorted` | After multiple adds, tombstones are sorted by (record_id, start_key) |
| `test_iter_returns_sorted_order` | iter() yields tombstones in (record_id, start_key) order |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_empty_index_covers_nothing` | covers() on empty index returns false |
| `test_single_key_range` | Tombstone [x, y) where y is one byte after x — covers only "x" |
| `test_len_and_is_empty` | len() and is_empty() reflect actual state |

---

## Done When

- [ ] `add()` inserts tombstones in sorted order
- [ ] `covers()` correctly checks (record_id, item_key range, sequence number) with O(log n + k) performance
- [ ] Start is inclusive, end is exclusive
- [ ] Empty start/end keys work as unbounded ranges
- [ ] Tombstones for different records are independent
- [ ] Tombstone does NOT shadow entries with equal or higher sequence numbers
- [ ] `tombstones_for_record()` returns correct slice
- [ ] `iter()` yields all tombstones in sorted order (for flush)
- [ ] All tests pass
