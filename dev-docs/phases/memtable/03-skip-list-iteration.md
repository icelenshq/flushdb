# Task 3: Skip List Iteration & Range Scans

**Crate:** `flushdb-engine`
**File:** `src/skiplist.rs`
**Depends on:** Task 2 (Skip List Core)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §6.1, Phase 3 §1

---

## Goal

Add iteration and range scan capabilities to the skip list. The sorted iterator is the critical interface for the flush pipeline (Phase 5b) — it yields entries in `CompositeKey` order to build SSTables. Range scans and full-record scans are the read-path primitives used by the memtable.

---

## What to Build

### 3.1 SkipListIterator

A borrowing iterator that traverses the level-0 chain of the skip list:

```
SkipListIterator<'a> {
    skiplist: &'a SkipList
    current: Option<usize>      // index of current node (None = exhausted)
}
```

**Yields:** `&'a SkipNode` in `(CompositeKey ASC, sequence_number DESC)` order.

**Design decisions:**
- **Borrowing, not owning** — the iterator borrows the skip list. This is safe because the skip list is immutable during iteration (single-writer, and reads only happen from the owning core or on a frozen memtable).
- **Level-0 traversal** — the bottom level contains all nodes in sorted order. Higher levels are only for search acceleration.
- Implements `Iterator<Item = &'a SkipNode>`.

### 3.2 Methods on SkipList

| Method | Signature | Behavior |
|--------|-----------|----------|
| `iter` | `(&self) -> SkipListIterator<'_>` | Returns iterator starting at the first entry (after sentinel head) |
| `range` | `(&self, start: &CompositeKey, end: &CompositeKey) -> SkipListIterator<'_>` | Returns iterator starting at `start` (inclusive), stopping before `end` (exclusive). Uses skip list search to find start position in O(log n). |
| `range_from` | `(&self, start: &CompositeKey) -> SkipListIterator<'_>` | Returns iterator from `start` (inclusive) to end of skip list |
| `scan_record` | `(&self, record_id: &[u8]) -> SkipListIterator<'_>` | Returns iterator over all entries for a given `record_id`. Seeks to `min_key_for_record(record_id)`, stops when record_id changes. |

### 3.3 Seek Algorithm (for range and scan_record)

```
seek_to(key):
  current = head
  for level in (0..self.height).rev():
    while let Some(next_idx) = nodes[current].next[level]:
      if nodes[next_idx].key < *key:
        current = next_idx
      else:
        break

  // current is the last node < key at level 0
  // nodes[current].next[0] is >= key (or None)
  return nodes[current].next[0]
```

### 3.4 Iterator Stop Conditions

The iterator needs configurable stop conditions for different scan types:

```
SkipListIterator<'a> {
    skiplist: &'a SkipList
    current: Option<usize>
    stop: StopCondition<'a>
}

enum StopCondition<'a> {
    None,                           // iterate until exhausted (iter, range_from)
    BeforeKey(&'a CompositeKey),    // stop before reaching this key (range)
    RecordBoundary(&'a [u8]),       // stop when record_id changes (scan_record)
}
```

The `next()` method checks the stop condition before yielding each node:
- `None` — always yield
- `BeforeKey(end)` — yield if `node.key < end`
- `RecordBoundary(rid)` — yield if `node.key.record_id() == rid`

### 3.5 IntoIterator for Flush Pipeline

For the flush pipeline (Phase 5b), expose an owned iterator that consumes the skip list:

```
SkipListIntoIterator {
    skiplist: SkipList
    current: Option<usize>
}
```

**Yields:** `SkipNode` (owned). This is used when the frozen memtable is consumed by the flush pipeline — the skip list is drained entry by entry to build SSTable blocks.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `into_iter` | `(self) -> SkipListIntoIterator` | Consumes skip list, returns owned iterator over all entries |

---

## Tests

**File:** `crates/flushdb-engine/tests/skiplist_iteration_tests.rs`

### Full Iteration Tests
| Test | What It Validates |
|------|-------------------|
| `test_iter_empty_skiplist` | iter() on empty skip list yields nothing |
| `test_iter_single_entry` | iter() yields exactly one entry |
| `test_iter_all_entries_in_order` | Insert 100 entries, iter() yields them in (key ASC, seq DESC) order |
| `test_iter_matches_btreemap_oracle` | Insert 10K random entries, compare iter() output against sorted BTreeMap |

### Range Scan Tests
| Test | What It Validates |
|------|-------------------|
| `test_range_returns_subset` | range(start, end) returns only entries in [start, end) |
| `test_range_start_equals_end` | range(k, k) returns empty iterator |
| `test_range_start_not_present` | Start key doesn't exist — iterator begins at next key after start |
| `test_range_end_not_present` | End key doesn't exist — iterator stops at last key before end |
| `test_range_covers_all_entries` | range(min_key, max_key) equivalent to iter() |
| `test_range_within_single_record` | Range within one record returns correct items |
| `test_range_across_records` | Range spanning record boundary returns entries from both records |

### Range From Tests
| Test | What It Validates |
|------|-------------------|
| `test_range_from_beginning` | range_from(min_key) yields all entries |
| `test_range_from_middle` | range_from(mid_key) yields entries from mid to end |
| `test_range_from_past_end` | range_from(key_beyond_max) yields nothing |

### Record Scan Tests
| Test | What It Validates |
|------|-------------------|
| `test_scan_record_returns_all_items` | scan_record("r1") returns all entries for record "r1" |
| `test_scan_record_stops_at_boundary` | scan_record("r1") does NOT return entries for "r2" |
| `test_scan_record_nonexistent` | scan_record("missing") returns empty iterator |
| `test_scan_record_with_multiple_versions` | Same key in record with seq 1 and 5 — both appear (seq 5 first) |
| `test_scan_record_includes_tombstones` | DELETE and RANGE_DELETE entries for the record are included |

### Into Iterator Tests
| Test | What It Validates |
|------|-------------------|
| `test_into_iter_yields_all_entries` | Consuming iterator yields same entries as borrowing iterator |
| `test_into_iter_entries_are_owned` | Yielded SkipNodes are owned values, not references |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_iter_with_duplicate_keys` | Multiple entries with same CompositeKey, different sequences — all yielded in seq DESC order |
| `test_range_with_empty_item_keys` | Range scan correctly handles entries with empty item keys |

---

## Done When

- [ ] `iter()` yields all entries in `(CompositeKey ASC, sequence_number DESC)` order
- [ ] `range(start, end)` returns entries in `[start, end)` with O(log n) seek
- [ ] `range_from(start)` returns entries from `start` to end
- [ ] `scan_record(record_id)` returns all entries for a record, stops at boundary
- [ ] `into_iter()` consumes skip list and yields owned entries (for flush pipeline)
- [ ] Empty skip list iterators return nothing
- [ ] All tests pass
