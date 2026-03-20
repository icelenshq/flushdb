# Task 7: Freeze, Swap & Frozen Memtable List

**Crate:** `flushdb-engine`
**Files:** `src/memtable.rs`, `src/memtable_list.rs`
**Depends on:** Task 5 (Memtable Core), Task 6 (Idempotency Dedup)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §6.3, Phase 3 §4, §6, §7

---

## Goal

Implement the memtable freeze/swap protocol and the `MemtableList` that manages the active memtable and a queue of frozen memtables. The list coordinates reads across all memtables (active first, then frozen newest-to-oldest), enforces backpressure when too many frozen memtables accumulate, provides cross-memtable dedup checking, and exposes frozen memtables for the flush pipeline to consume.

---

## What to Build

### 7.1 MemtableList Struct

```
MemtableList {
    active: Memtable                    // current writable memtable
    frozen: Vec<Memtable>               // frozen memtables, newest first (index 0 = most recent)
    config: MemtableConfig              // shared config for creating new memtables
}
```

**Design decisions:**
- **`Vec<Memtable>` for frozen list** — single-owner model means a simple Vec suffices. Newest-first ordering so reads iterate in recency order.
- **No `Arc`/`Mutex`** — the entire `MemtableList` is owned by a single core. The flush pipeline receives a frozen memtable by value (via `pop_oldest_frozen`), not by shared reference.
- **Active memtable is always present** — after freeze, a new active memtable is immediately created.

### 7.2 Construction

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: MemtableConfig, starting_sequence: u64) -> Self` | Creates list with a fresh active memtable and empty frozen list |

### 7.3 Write Path

| Method | Signature | Behavior |
|--------|-----------|----------|
| `insert` | `(&mut self, entry: MemtableEntry) -> FlushResult<u64>` | Checks cross-memtable dedup first (active + all frozen). If duplicate, returns `DuplicateToken`. Otherwise delegates to `active.insert(entry)`. |

**Insert with cross-memtable dedup:**
```
insert(entry):
  // Check dedup across all memtables (skip for all-zero tokens)
  if !entry.idempotency_key.is_none():
    if active.check_dedup(&entry.idempotency_key):
      return Err(DuplicateToken)
    for frozen_mt in &frozen:
      if frozen_mt.check_dedup(&entry.idempotency_key):
        return Err(DuplicateToken)

  // Delegate to active memtable
  active.insert(entry)
```

### 7.4 Read Path

| Method | Signature | Behavior |
|--------|-----------|----------|
| `get` | `(&self, key: &CompositeKey) -> Option<MemtableEntry>` | Checks active memtable first. If not found, checks frozen memtables newest-to-oldest. Returns the first match (highest sequence number across all memtables). |
| `scan` | `(&self, start: &CompositeKey, end: &CompositeKey) -> Vec<MemtableEntry>` | Scans active + all frozen memtables, merges results by CompositeKey order, deduplicates (highest seq wins), filters tombstones. |
| `scan_record` | `(&self, record_id: &[u8]) -> Vec<MemtableEntry>` | Like `scan` but scoped to a single record. |

**Get protocol (waterfall):**
```
get(key):
  // Check active first
  if let Some(entry) = active.get(key):
    return Some(entry)

  // Check frozen memtables, newest first
  for frozen_mt in &frozen:
    if let Some(entry) = frozen_mt.get(key):
      return Some(entry)

  None
```

**Scan merge protocol:**
The scan across multiple memtables must merge results correctly. Since each memtable's scan already deduplicates internally, the cross-memtable merge needs to:
1. Collect results from each memtable
2. For entries with the same CompositeKey, keep only the one with the highest sequence number
3. Sort by CompositeKey

### 7.5 Freeze and Swap

| Method | Signature | Behavior |
|--------|-----------|----------|
| `freeze_active` | `(&mut self) -> FlushResult<()>` | Freezes current active memtable, pushes it to front of frozen list, creates new active memtable with sequence number continuing from where the old one left off. Returns `ResourceExhausted` if frozen count >= `max_frozen_count` (backpressure). |
| `should_freeze` | `(&self, max_age: Duration) -> bool` | Returns true if active memtable exceeds size threshold OR age threshold. |

**Freeze protocol:**
```
freeze_active():
  if frozen.len() >= config.max_frozen_count:
    return Err(FlushError::ResourceExhausted {
      resource: "memtable",
      message: format!("too many frozen memtables: {}/{}", frozen.len(), config.max_frozen_count)
    })

  next_seq = active.next_sequence_number()
  active.freeze()

  // Swap: move old active to frozen, create new active
  let old_active = std::mem::replace(&mut active, Memtable::new(config, next_seq))
  frozen.insert(0, old_active)    // newest first

  Ok(())
```

### 7.6 Flush Pipeline Interface

| Method | Signature | Behavior |
|--------|-----------|----------|
| `pop_oldest_frozen` | `(&mut self) -> Option<Memtable>` | Removes and returns the oldest frozen memtable (last in the Vec). The flush pipeline calls this to get a memtable to flush to SSTable. Returns `None` if no frozen memtables. |
| `frozen_count` | `(&self) -> usize` | Number of frozen memtables waiting for flush |
| `has_frozen` | `(&self) -> bool` | True if any frozen memtables exist |

**Design decision:** `pop_oldest_frozen` transfers ownership — the flush pipeline gets the full memtable to iterate and write to SSTable. After the SSTable is confirmed on S3 and the manifest is updated, the memtable is dropped, releasing its arena memory.

### 7.7 Backpressure

When `frozen.len() >= config.max_frozen_count` (default 3, meaning ~192 MB pinned):
- `freeze_active()` returns `ResourceExhausted`
- The caller (engine or server layer) must reject new writes until a flush completes and a frozen memtable is removed via `pop_oldest_frozen()`

| Method | Signature | Behavior |
|--------|-----------|----------|
| `is_backpressured` | `(&self) -> bool` | Returns `true` if `frozen.len() >= config.max_frozen_count` |

**Backpressure error context:** The error includes the current frozen count and threshold, so the server (Phase 7) can compute a meaningful `retry-after` hint.

### 7.8 State Queries

| Method | Signature | Behavior |
|--------|-----------|----------|
| `active_memory_usage` | `(&self) -> usize` | Active memtable's approximate memory usage |
| `total_memory_usage` | `(&self) -> usize` | Sum of active + all frozen memtables' memory usage |
| `active_entry_count` | `(&self) -> usize` | Number of entries in active memtable |
| `total_entry_count` | `(&self) -> usize` | Sum of entries across all memtables |
| `next_sequence_number` | `(&self) -> u64` | Active memtable's next sequence number |

### 7.9 Send Bound Verification

The `Memtable` must be `Send` so frozen memtables can be transferred to flush tasks. Verify with a compile-time assertion:

```rust
fn _assert_memtable_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Memtable>();
}
```

---

## Tests

**File:** `crates/flushdb-engine/tests/memtable_list_tests.rs`

### Freeze & Swap Tests
| Test | What It Validates |
|------|-------------------|
| `test_freeze_creates_new_active` | After freeze, active memtable is empty with continued sequence numbers |
| `test_freeze_preserves_frozen_data` | After freeze, data from old active is readable via get |
| `test_freeze_sequence_continuity` | New active starts at the sequence number where old active left off |
| `test_freeze_backpressure` | After max_frozen_count freezes without pops, next freeze returns ResourceExhausted |
| `test_freeze_empty_memtable` | Freezing an empty memtable works (creates empty frozen entry) |

### Cross-Memtable Read Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_checks_active_first` | Key in both active and frozen — returns active's version (higher seq) |
| `test_get_falls_through_to_frozen` | Key only in frozen — get returns it |
| `test_get_checks_frozen_newest_first` | Key in two frozen memtables — returns the one from the more recently frozen |
| `test_get_not_found_anywhere` | Key in neither active nor frozen — returns None |
| `test_scan_merges_active_and_frozen` | Entries split across active and frozen — scan returns merged sorted result |
| `test_scan_deduplicates_across_memtables` | Same key in active (seq 10) and frozen (seq 5) — scan returns only seq 10 |
| `test_scan_record_across_memtables` | Record entries split across active and frozen — scan_record returns all |

### Cross-Memtable Dedup Tests
| Test | What It Validates |
|------|-------------------|
| `test_dedup_rejects_token_in_active` | Token in active memtable — insert with same token returns DuplicateToken |
| `test_dedup_rejects_token_in_frozen` | Token in frozen memtable — insert into active with same token returns DuplicateToken |
| `test_dedup_all_zero_bypasses_across_memtables` | All-zero tokens always accepted, even if same all-zero was used in frozen |
| `test_dedup_after_freeze` | Token inserted, freeze, same token rejected in new active |

### Pop Frozen Tests
| Test | What It Validates |
|------|-------------------|
| `test_pop_oldest_frozen` | Freeze twice (A, B). pop_oldest_frozen returns A (oldest), then B |
| `test_pop_oldest_frozen_empty` | No frozen memtables — returns None |
| `test_pop_reduces_frozen_count` | After pop, frozen_count decreases by 1 |
| `test_pop_relieves_backpressure` | At max frozen count (backpressured), pop one, then freeze succeeds |

### Backpressure Tests
| Test | What It Validates |
|------|-------------------|
| `test_is_backpressured` | Returns true when frozen count >= max_frozen_count |
| `test_not_backpressured_initially` | Fresh list is not backpressured |
| `test_backpressure_error_has_context` | ResourceExhausted error includes frozen count and threshold |

### State Query Tests
| Test | What It Validates |
|------|-------------------|
| `test_active_memory_usage` | Reflects active memtable's usage |
| `test_total_memory_usage` | Sums active + all frozen |
| `test_total_entry_count` | Sums entries across all memtables |

### Send Bound Tests
| Test | What It Validates |
|------|-------------------|
| `test_memtable_is_send` | Memtable satisfies Send bound (compile-time) |
| `test_memtable_list_is_send` | MemtableList satisfies Send bound (compile-time) |

---

## Done When

- [ ] `freeze_active()` freezes current memtable, creates new active with continued sequence numbers
- [ ] Frozen memtables are readable via `get`, `scan`, `scan_record`
- [ ] Reads check active → frozen (newest first) with correct merge semantics
- [ ] Cross-memtable dedup rejects tokens found in any active or frozen memtable
- [ ] Backpressure returns `ResourceExhausted` when frozen count >= max_frozen_count
- [ ] `pop_oldest_frozen()` transfers ownership of oldest frozen memtable to caller
- [ ] Pop relieves backpressure (allows new freezes)
- [ ] `Memtable` and `MemtableList` are `Send`
- [ ] All tests pass
