# Task 5: ContinuityTracker — Negative Lookup Cache

**Crate:** `flushdb-engine`
**File:** `src/cache/continuity.rs`
**Depends on:** Task 1 (CacheConfig), Task 4 (Compaction-Aware Cache Eviction — for invalidation integration)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §12 (Continuity tracking), Phase 6 §5 (Continuity Tracking)

---

## Goal

Track which key ranges within a record are fully cached, enabling "negative lookups" — if a read requests a key that falls within a fully-cached range and that key wasn't found during the original scan, the key definitively doesn't exist. This skips a StorageBackend round-trip for keys known to be absent. Intervals are tagged with the manifest version at the time of caching and automatically invalidated when compaction produces a new manifest that could affect the cached range.

---

## What to Build

### 5.1 ContinuityInterval

Represents a fully-cached key range within a single record:

```
ContinuityInterval {
    start_key: Bytes,        // Inclusive start of the cached range (item_key portion)
    end_key: Bytes,          // Exclusive end of the cached range (item_key portion)
    manifest_id: ManifestId, // Manifest version when this range was cached
}
```

- Empty `start_key` means "from the beginning of the record"
- Empty `end_key` means "to the end of the record"
- Implements `Clone`, `Debug`

### 5.2 ContinuityTracker

Tracks continuity intervals grouped by record_id:

```
ContinuityTracker {
    intervals: HashMap<Bytes, Vec<ContinuityInterval>>,  // record_id → sorted intervals
    enabled: bool,
    max_records: usize,   // Capacity bound on tracked records
    max_intervals_per_record: usize,  // Capacity bound per record
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: &CacheConfig) -> Self` | Creates tracker, `enabled` from `config.enable_continuity_tracking`. Default `max_records`: 10_000. Default `max_intervals_per_record`: 100. |
| `mark_range_complete` | `(&mut self, record_id: &[u8], start_key: &[u8], end_key: &[u8], manifest_id: ManifestId)` | Records that the range `[start_key, end_key)` for this record is fully cached as of the given manifest version. Merges with adjacent/overlapping intervals for the same record. If disabled, no-op. |
| `is_known_absent` | `(&self, record_id: &[u8], item_key: &[u8], current_manifest_id: ManifestId) -> bool` | Returns true if `item_key` falls within a continuity interval for this record AND the interval's `manifest_id` matches `current_manifest_id`. If the manifest has advanced (compaction occurred), returns false — the caller must re-check StorageBackend. If disabled, always returns false. |
| `invalidate_for_record` | `(&mut self, record_id: &[u8])` | Removes all continuity intervals for the given record. Called when a write to this record invalidates cached state. |
| `invalidate_before_manifest` | `(&mut self, manifest_id: ManifestId)` | Removes all intervals tagged with a manifest_id older than the given version. Called after compaction produces a new manifest. |
| `invalidate_all` | `(&mut self)` | Clears all tracked intervals. |
| `tracked_record_count` | `(&self) -> usize` | Number of records with at least one continuity interval |
| `total_interval_count` | `(&self) -> usize` | Total number of intervals across all records |

### 5.3 Interval Merging

When `mark_range_complete` is called and overlapping/adjacent intervals exist for the same record with the same manifest_id:

1. Find all existing intervals that overlap or are adjacent to `[start_key, end_key)`
2. Merge them into a single interval: `[min(starts), max(ends))`
3. If intervals have different manifest_ids, keep only the one with the higher manifest_id (the more recent data)

Adjacency: intervals `[a, b)` and `[b, c)` are adjacent and merge to `[a, c)`.

### 5.4 Integration Points

**Read path (point_read):** Before checking SSTables, the read path can ask:
```
if continuity_tracker.is_known_absent(record_id, item_key, current_manifest_id) {
    return Ok(None);  // Key definitively doesn't exist
}
```

**Read path (range_read):** After completing a range scan that returned all entries in `[start, end)`, mark the range as complete:
```
continuity_tracker.mark_range_complete(record_id, start_key, end_key, manifest_id);
```

**Write path:** After a write to record_id, invalidate that record's continuity:
```
continuity_tracker.invalidate_for_record(record_id);
```

**Compaction:** After a manifest update:
```
continuity_tracker.invalidate_before_manifest(new_manifest_id);
```

These integrations are wired in Task 9 (Engine Integration).

### 5.5 Capacity Management

- `max_records` bounds the number of tracked records. When exceeded, the oldest record (by earliest interval manifest_id) is evicted to make room.
- `max_intervals_per_record` bounds intervals per record. When exceeded, the oldest interval is evicted.
- This prevents unbounded memory growth from tracking every record ever read.

### 5.6 Design Decisions

- **Manifest version tagging:** Intervals become invalid after compaction because compaction can introduce new data or remove tombstones in the tracked range. Checking `manifest_id == current_manifest_id` is a conservative but correct invalidation strategy.
- **Per-record grouping:** Intervals are grouped by record_id because queries always target a specific record. This makes lookups O(intervals_per_record), not O(total_intervals).
- **Write-side invalidation:** A write to a record makes all continuity intervals for that record stale. Point invalidation by record_id is O(1) in the HashMap.
- **No cross-SSTable tracking:** Continuity is tracked at the logical level (record_id + key range), not at the SSTable level. This means a continuity interval covers the merged view across all SSTables for that range.

---

## Tests

**File:** `crates/flushdb-engine/tests/continuity_tracker_tests.rs`

### Basic Operations
| Test | What It Validates |
|------|-------------------|
| `test_mark_and_check_present` | Mark [a, z) for record, is_known_absent for "m" returns true (key absent in fully-cached range) |
| `test_check_outside_range` | Mark [a, m), is_known_absent for "z" returns false (outside cached range) |
| `test_check_at_boundaries` | Mark [a, m), "a" returns true (inclusive start), "m" returns false (exclusive end) |
| `test_empty_tracker` | is_known_absent on empty tracker returns false |
| `test_disabled_tracker` | Disabled tracker always returns false for is_known_absent, mark_range_complete is no-op |

### Manifest Version Tracking
| Test | What It Validates |
|------|-------------------|
| `test_same_manifest_version_hit` | Mark with manifest_id 5, query with manifest_id 5 → returns true |
| `test_different_manifest_version_miss` | Mark with manifest_id 5, query with manifest_id 6 → returns false (stale) |
| `test_invalidate_before_manifest` | Mark intervals with manifest_ids 3,5,7. Invalidate before 6. Only interval with id 7 remains. |

### Interval Merging
| Test | What It Validates |
|------|-------------------|
| `test_adjacent_intervals_merge` | Mark [a, m) then [m, z) for same record/manifest → merged to [a, z) |
| `test_overlapping_intervals_merge` | Mark [a, n) then [f, z) → merged to [a, z) |
| `test_non_overlapping_intervals_separate` | Mark [a, d) and [x, z) → two separate intervals |
| `test_merge_different_manifest_keeps_newer` | Mark [a, m) with manifest 5, [f, z) with manifest 7 → merged interval uses manifest 7 |

### Write Invalidation
| Test | What It Validates |
|------|-------------------|
| `test_invalidate_for_record` | Mark range for record "A" and "B", invalidate_for_record("A"), "A" intervals gone, "B" intact |
| `test_invalidate_all` | Mark ranges for 3 records, invalidate_all, all gone |

### Capacity Bounds
| Test | What It Validates |
|------|-------------------|
| `test_max_records_eviction` | Set max_records to 3, mark ranges for 4 records → oldest record evicted |
| `test_max_intervals_per_record` | Set max_intervals_per_record to 2, mark 3 non-overlapping ranges for same record → oldest evicted |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_empty_start_key` | Mark range from "" (record start) to "m" — is_known_absent works correctly |
| `test_empty_end_key` | Mark range from "m" to "" (record end) — is_known_absent works correctly |
| `test_full_record_range` | Mark "" to "" (full record) — all keys within record are known-absent |
| `test_single_byte_range` | Mark [a, b) — only "a" is covered |

---

## Done When

- [ ] ContinuityTracker records fully-cached key ranges per record
- [ ] `is_known_absent` returns true only when key falls within a cached range AND manifest version matches
- [ ] Interval merging correctly handles adjacent and overlapping ranges
- [ ] `invalidate_before_manifest` removes all stale intervals
- [ ] `invalidate_for_record` correctly invalidates on writes
- [ ] Capacity bounds prevent unbounded memory growth
- [ ] Disabled tracker is a pure no-op
- [ ] All tests pass
