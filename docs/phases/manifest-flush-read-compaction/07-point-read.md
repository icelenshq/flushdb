# Task 7: Point Read Path

**Crate:** `flushdb-engine`
**File:** `src/read_path.rs`
**Depends on:** Task 4 (BlockFetcher, SSTableHandle, LevelState), Task 5 (MergeEntry)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §11.2 (Point Read), §11.6 (Parallel S3 GETs for L0)

---

## Goal

Implement point reads that merge results across all layers — active memtable, frozen memtable(s), and SSTables at L0-L3. The read path checks bloom filters to eliminate non-matching SSTables, fetches data blocks through BlockFetcher, and resolves conflicts by sequence number (highest wins). Tombstones cause not-found results.

---

## What to Build

### 7.1 ReadPath

```
ReadPath<B: StorageBackend>
```

This struct is NOT stored long-term — it's created per-read with references to the current state.

**Construction:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(fetcher: &dyn BlockFetcher) -> Self` | Creates a read path with the block fetcher |

### 7.2 Point Read Method

| Method | Signature | Behavior |
|--------|-----------|----------|
| `point_read` | `async (&self, key: &CompositeKey, memtable_list: &MemtableList, levels: &[LevelState]) -> FlushResult<Option<MergeEntry>>` | Full point read across all layers. Returns the latest (highest sequence) entry for the key, or None if not found or tombstoned. |

**Point read algorithm:**

**Step 1: Check memtables (newest first)**
- Search active memtable via `memtable_list.get(key)`
- If found: convert to `MergeEntry`, check range tombstones, return if Put (or return None if tombstone)
- Search each frozen memtable (newest first) — same logic

**Step 2: Check SSTables (L0 first, then L1-L3)**

For each level, starting at L0:

**L0 (overlapping):**
- Call `level_state.find_candidates_for_key(key)` — returns all handles passing bloom filter
- For each candidate, call `handle.get(key, fetcher)` — these can be done concurrently
- Collect all found entries

**L1-L3 (non-overlapping):**
- Call `level_state.find_candidates_for_key(key)` — returns at most one handle
- If found, call `handle.get(key, fetcher)`

**Step 3: Merge results**
- From all found entries (memtable + SSTable), pick the one with the highest `sequence_number`
- If the winner is a tombstone (Delete or RangeDelete): return `None`
- If the winner is a Put: return `Some(entry)`
- If no entries found at all: return `None`

**Step 4: Range tombstone check**
- Before returning a Put result, check if any range tombstone (from memtable or SSTable) with a higher sequence number covers this key
- If covered: return `None`

### 7.3 Early Termination

Point reads can short-circuit:
- If memtable returns a tombstone with sequence S, no need to check SSTables with max_sequence < S
- If memtable returns a Put, still need to check range tombstones at higher layers, but can skip SSTables entirely (memtable is always newest)

The algorithm should take advantage of this: if a definitive result is found in the active memtable, return immediately without checking frozen memtables or SSTables.

### 7.4 Range Tombstone Checking

For point reads, range tombstone checking uses the existing `RangeTombstoneIndex::covers` method from Phase 3:
- Check active memtable's range tombstone index
- Check each frozen memtable's range tombstone index
- For SSTables: range tombstones within SSTable blocks are discovered during the block scan — entries with `EntryType::RangeDelete` and `RANGE_TOMBSTONE_PREFIX` in the key

### 7.5 GetResult

The return type for point reads exposed to the Engine:

| Field | Type | Description |
|-------|------|-------------|
| `key` | `CompositeKey` | The looked-up key |
| `value` | `Bytes` | Entry value |
| `metadata` | `Bytes` | Entry metadata |
| `sequence_number` | `u64` | Sequence number of the winning entry |

Constructed from `MergeEntry` when the result is a Put.

---

## Tests

**File:** `crates/flushdb-engine/tests/point_read_tests.rs`

### Memtable-Only Tests
| Test | What It Validates |
|------|-------------------|
| `test_point_read_active_memtable_hit` | Key in active memtable → found |
| `test_point_read_active_memtable_miss` | Key not in any layer → None |
| `test_point_read_frozen_memtable_hit` | Key in frozen memtable → found |
| `test_point_read_active_overrides_frozen` | Same key in active (seq 10) and frozen (seq 5) → active wins |
| `test_point_read_tombstone_returns_none` | Key deleted in active memtable → None |
| `test_point_read_range_tombstone_covers_memtable_put` | Range tombstone in active covers a put in frozen → None |

### SSTable Tests
| Test | What It Validates |
|------|-------------------|
| `test_point_read_l0_hit` | Key found in L0 SSTable |
| `test_point_read_l1_hit` | Key found in L1 SSTable via binary search |
| `test_point_read_bloom_filter_eliminates` | Key not in bloom filter → no data block fetch (verify via fetch count) |
| `test_point_read_l0_parallel_candidates` | Multiple L0 SSTables match bloom → all checked, highest seq wins |

### Cross-Layer Tests
| Test | What It Validates |
|------|-------------------|
| `test_point_read_memtable_overrides_sstable` | Same key in memtable (seq 20) and L0 (seq 10) → memtable wins |
| `test_point_read_l0_overrides_l1` | Same key in L0 (seq 15) and L1 (seq 5) → L0 wins |
| `test_point_read_tombstone_in_memtable_shadows_sstable` | Delete in memtable, Put in L0 → None |
| `test_point_read_tombstone_in_l0_shadows_l1` | Delete in L0, Put in L1 → None |
| `test_point_read_put_after_delete` | Put (seq 20) after Delete (seq 10) → Put wins |

### Early Termination Tests
| Test | What It Validates |
|------|-------------------|
| `test_point_read_active_memtable_hit_skips_sstables` | Found in active memtable → SSTables not accessed |
| `test_point_read_frozen_hit_skips_sstables` | Found in frozen → SSTables not accessed |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_point_read_empty_all_layers` | No data anywhere → None |
| `test_point_read_key_at_sstable_boundary` | Key equals min_key or max_key of SSTable → found |
| `test_point_read_multiple_l0_sstables` | 4 L0 SSTables, key in oldest one → still found |

---

## Done When

- [ ] Point read searches active → frozen → L0 → L1 → L2 → L3 in order
- [ ] Bloom filter checks eliminate non-matching SSTables
- [ ] L0 candidates checked (conceptually) in parallel, highest sequence wins
- [ ] L1-L3 use binary search to find at most one candidate
- [ ] Tombstones (point and range) cause not-found results
- [ ] Higher sequence numbers override lower ones across all layers
- [ ] Early termination when memtable has definitive answer
- [ ] All tests pass
