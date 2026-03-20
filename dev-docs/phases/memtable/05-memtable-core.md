# Task 5: Memtable — Core Insert & Read

**Crate:** `flushdb-engine`
**File:** `src/memtable.rs`
**Depends on:** Task 2 (Skip List Core), Task 3 (Skip List Iteration), Task 4 (Range Tombstone Index)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §6, Phase 3 §1–§5

---

## Goal

Build the `Memtable` struct that composes the skip list and range tombstone index into a coherent in-memory sorted store. The memtable accepts `MemtableEntry` writes, assigns sequence numbers, serves point lookups and range scans with correct tombstone filtering, and tracks its memory footprint for freeze decisions.

---

## What to Build

### 5.1 MemtableConfig

```
MemtableConfig {
    size_threshold: usize       // default 64 MB (67_108_864) — freeze when total_allocated >= this
    max_frozen_count: usize     // default 3 — backpressure when this many frozen memtables exist
}
```

Derives: `Debug`, `Clone`

Provide `Default` impl with the above defaults.

### 5.2 Memtable Struct

```
Memtable {
    skiplist: SkipList
    range_tombstones: RangeTombstoneIndex
    next_sequence_number: u64           // monotonic counter, assigned per insert
    config: MemtableConfig
    created_at: Instant                 // for time-based freeze check
    frozen: bool                        // once true, no further inserts allowed
}
```

**Design decisions:**
- **`next_sequence_number` lives on the memtable** — each insert gets the next sequence number, then it increments. Initialized from the WAL's last sequence number during recovery (passed to constructor).
- **`frozen` flag** — when set, `insert()` returns an error. This is a safety check; the memtable manager (Task 7) is responsible for swapping before freezing.
- **`created_at`** — used by the memtable manager for time-based freeze triggers (5-minute threshold).
- **No `Arc`, no `Mutex`** — the memtable is single-owner per the shard-per-core model.

### 5.3 Construction

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: MemtableConfig, starting_sequence: u64) -> Self` | Creates empty memtable with given config and starting sequence number. Sets `created_at` to `Instant::now()`. |

### 5.4 Write Path

| Method | Signature | Behavior |
|--------|-----------|----------|
| `insert` | `(&mut self, mut entry: MemtableEntry) -> FlushResult<u64>` | Assigns the next sequence number to the entry, inserts into skip list, updates range tombstone index if entry is `RangeDelete`, increments sequence counter. Returns the assigned sequence number. Returns `FlushError::ResourceExhausted` if memtable is frozen. |

**Insert protocol:**
```
insert(entry):
  if self.frozen:
    return Err(FlushError::ResourceExhausted { resource: "memtable", message: "memtable is frozen" })

  seq = self.next_sequence_number
  self.next_sequence_number += 1
  entry.sequence_number = seq

  if entry.entry_type == RangeDelete:
    // Extract range tombstone from the entry
    // For RANGE_DELETE: composite_key has record_id, item_key has the start_key (after 0xFF prefix)
    // value contains the end_key
    range_tombstones.add(RangeTombstone {
      record_id: Bytes::copy_from_slice(entry.record_id()),
      start_key: extract_range_delete_start_key(entry),
      end_key: entry.value.clone(),
      sequence_number: seq,
    })

  skiplist.insert(entry)
  return Ok(seq)
```

### 5.5 Read Path — Point Lookup

| Method | Signature | Behavior |
|--------|-----------|----------|
| `get` | `(&self, key: &CompositeKey) -> Option<MemtableEntry>` | Looks up key in skip list. If found, checks range tombstone index — if the entry is covered by a tombstone with higher sequence number, returns `None`. If the entry itself is a Delete tombstone, returns the tombstone entry (caller decides how to handle). Returns cloned `MemtableEntry`. |

**Get protocol:**
```
get(key):
  node = skiplist.get(key)?

  // Check if covered by range tombstone
  if range_tombstones.covers(node.record_id, node.item_key, node.sequence_number):
    return None

  return Some(node_to_memtable_entry(node))
```

**Design decision:** `get` returns `Option<MemtableEntry>` rather than `Option<&SkipNode>` — the caller shouldn't depend on skip list internals. The conversion clones the Bytes fields (cheap — Bytes is reference-counted).

### 5.6 Read Path — Range Scan

| Method | Signature | Behavior |
|--------|-----------|----------|
| `scan` | `(&self, start: &CompositeKey, end: &CompositeKey) -> Vec<MemtableEntry>` | Range scan from `start` (inclusive) to `end` (exclusive). Filters out entries covered by range tombstones. Returns entries in `CompositeKey` order. For entries with the same CompositeKey, returns only the one with the highest sequence number. |
| `scan_record` | `(&self, record_id: &[u8]) -> Vec<MemtableEntry>` | Returns all live entries for a record. Filters range tombstones. Deduplicates by CompositeKey (highest seq wins). |

**Scan deduplication:** The skip list may contain multiple versions of the same key (different sequence numbers). The scan methods must deduplicate: for each unique CompositeKey, yield only the entry with the highest sequence number (which appears first due to DESC ordering). Skip entries that are point tombstones (EntryType::Delete) — they shadow the key.

**Scan protocol:**
```
scan(start, end):
  results = Vec::new()
  last_key = None
  for node in skiplist.range(start, end):
    // Skip duplicate keys (only take first = highest seq)
    if last_key == Some(&node.key):
      continue
    last_key = Some(&node.key)

    // Skip if covered by range tombstone
    if range_tombstones.covers(node.record_id, node.item_key, node.sequence_number):
      continue

    // Skip point tombstones (Delete entries)
    if node.entry_type == EntryType::Delete:
      continue

    results.push(node_to_memtable_entry(node))

  return results
```

### 5.7 Read Path — Raw Iteration (for Flush)

| Method | Signature | Behavior |
|--------|-----------|----------|
| `iter` | `(&self) -> impl Iterator<Item = &SkipNode>` | Raw iterator over all entries in sorted order — no tombstone filtering, no dedup. Used by the flush pipeline which needs ALL entries (including tombstones) to write to SSTable. |

### 5.8 Memtable State Queries

| Method | Signature | Behavior |
|--------|-----------|----------|
| `approximate_memory_usage` | `(&self) -> usize` | Returns skip list's approximate memory usage |
| `should_freeze_by_size` | `(&self) -> bool` | Returns `true` if `approximate_memory_usage() >= config.size_threshold` |
| `should_freeze_by_age` | `(&self, max_age: Duration) -> bool` | Returns `true` if `created_at.elapsed() >= max_age` |
| `entry_count` | `(&self) -> usize` | Number of entries in skip list |
| `next_sequence_number` | `(&self) -> u64` | Current sequence counter value |
| `is_frozen` | `(&self) -> bool` | Whether the memtable has been frozen |
| `freeze` | `(&mut self)` | Sets `frozen = true`. After this, `insert()` rejects writes. |
| `is_empty` | `(&self) -> bool` | True if skip list has no entries |
| `range_tombstone_count` | `(&self) -> usize` | Number of range tombstones in the index |

### 5.9 Helper: SkipNode to MemtableEntry Conversion

```
node_to_memtable_entry(node: &SkipNode) -> MemtableEntry:
  MemtableEntry::with_sequence(
    node.key.clone(),
    node.value.clone(),
    node.metadata.clone(),
    // IdempotencyToken needs to be stored in SkipNode or reconstructed
    node.idempotency_key.clone(),
    node.sequence_number,
    node.entry_type,
  )
```

This means `SkipNode` must also store the `IdempotencyToken` — update Task 2's SkipNode definition to include it.

---

## Tests

**File:** `crates/flushdb-engine/tests/memtable_tests.rs`

### Insert Tests
| Test | What It Validates |
|------|-------------------|
| `test_insert_assigns_sequence_number` | First insert gets starting_sequence, second gets starting_sequence + 1 |
| `test_insert_put_entry` | PUT entry is retrievable via get |
| `test_insert_delete_entry` | DELETE entry is stored (retrievable via raw iter) |
| `test_insert_range_delete_entry` | RANGE_DELETE entry adds to both skip list and range tombstone index |
| `test_insert_frozen_memtable_returns_error` | After freeze(), insert returns ResourceExhausted |

### Point Lookup Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_existing_key` | get returns the inserted entry |
| `test_get_nonexistent_key` | get returns None for missing key |
| `test_get_returns_latest_version` | Same key inserted twice — get returns the one with higher seq |
| `test_get_deleted_key_returns_tombstone` | After PUT then DELETE for same key — get returns the DELETE entry |
| `test_get_key_covered_by_range_tombstone` | Key within range tombstone with higher seq — get returns None |
| `test_get_key_not_covered_by_range_tombstone_lower_seq` | Range tombstone with lower seq than entry — get returns entry |

### Range Scan Tests
| Test | What It Validates |
|------|-------------------|
| `test_scan_returns_entries_in_range` | scan(start, end) returns correct subset |
| `test_scan_deduplicates_by_key` | Same key with seq 1 and 5 — scan returns only seq 5 |
| `test_scan_filters_point_tombstones` | DELETE entries are excluded from scan results |
| `test_scan_filters_range_tombstone_covered` | Entries covered by range tombstones excluded |
| `test_scan_empty_range` | scan where start >= end returns empty |

### Record Scan Tests
| Test | What It Validates |
|------|-------------------|
| `test_scan_record_returns_all_items` | All PUT entries for record are returned |
| `test_scan_record_excludes_other_records` | Entries for other records are NOT included |
| `test_scan_record_filters_tombstones` | DELETE and range-tombstoned entries excluded |
| `test_scan_record_nonexistent` | scan_record for missing record returns empty |

### Sequence Number Tests
| Test | What It Validates |
|------|-------------------|
| `test_sequence_monotonically_increasing` | Each insert gets seq = previous + 1 |
| `test_starting_sequence_from_constructor` | Memtable starts at the given starting_sequence |
| `test_next_sequence_number_query` | next_sequence_number() returns the next value to be assigned |

### Freeze & State Tests
| Test | What It Validates |
|------|-------------------|
| `test_freeze_prevents_inserts` | After freeze, insert returns error |
| `test_freeze_allows_reads` | After freeze, get/scan/scan_record still work |
| `test_should_freeze_by_size` | Returns true when memory usage >= threshold |
| `test_should_freeze_by_age` | Returns true when elapsed time >= max_age |
| `test_is_empty` | Empty memtable returns true, non-empty returns false |
| `test_entry_count` | entry_count matches number of inserts |

### Oracle Test
| Test | What It Validates |
|------|-------------------|
| `test_100k_entries_match_btreemap_oracle` | Insert 100K random entries into memtable and BTreeMap. scan_record for each record — results match oracle. Point lookups for random keys — results match. |

---

## Done When

- [ ] `insert()` assigns monotonic sequence numbers and handles all three entry types
- [ ] `get()` returns latest version, respects point and range tombstones
- [ ] `scan()` deduplicates, filters tombstones, returns correct range
- [ ] `scan_record()` returns all live entries for a record
- [ ] Frozen memtable rejects inserts but serves reads
- [ ] Range tombstone index is updated on RANGE_DELETE inserts
- [ ] `iter()` yields all raw entries for flush pipeline
- [ ] 100K entry oracle test passes
- [ ] All tests pass
