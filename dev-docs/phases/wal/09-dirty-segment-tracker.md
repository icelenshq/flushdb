# Task 9: Dirty Segment Tracker

**Crate:** `flushdb-wal`
**File:** `src/dirty_tracker.rs`
**Depends on:** Task 1 (WalConfig for thresholds)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §5.5; phase-2-wal.md §7, Future Work Considerations

---

## Goal

Track which WAL segments are still "dirty" (contain entries for unflushed memtable generations) and which are safe to delete. This prevents premature segment deletion — a segment can only be removed after ALL memtable generations that wrote to it have been flushed to S3.

---

## What to Build

### 9.1 DirtySegmentTracker Struct

| Field | Type | Description |
|-------|------|-------------|
| `dirty_maps` | `HashMap<u64, HashMap<u64, u64>>` | Outer key: segment_number. Inner key: generation_id. Inner value: highest sequence number in that segment for that generation. |
| `segment_created_at` | `HashMap<u64, Instant>` | When each segment was first registered (for age-based flush triggers). |

### 9.2 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty tracker. |
| `record_write` | `(&mut self, segment_number: u64, generation_id: u64, sequence_number: u64)` | Records that `generation_id` wrote entry with `sequence_number` to `segment_number`. Updates highest sequence if this is higher. Creates segment entry and timestamps it on first write. |
| `mark_generation_flushed` | `(&mut self, generation_id: u64) -> Vec<u64>` | Removes `generation_id` from ALL segments' dirty maps. Returns list of segment numbers that became fully clean (eligible for deletion) as a result. |
| `is_segment_clean` | `(&self, segment_number: u64) -> bool` | True if the segment has no dirty generations (or is unknown to the tracker). |
| `deletable_segments` | `(&self) -> Vec<u64>` | Returns all segment numbers with empty dirty maps, sorted ascending. |
| `dirty_generation_ids` | `(&self, segment_number: u64) -> Vec<u64>` | Returns the generation IDs that are keeping a specific segment dirty. Empty vec if segment is clean or unknown. |
| `all_dirty_segments` | `(&self) -> Vec<u64>` | Returns all tracked segment numbers that have non-empty dirty maps, sorted ascending. |
| `segments_older_than` | `(&self, max_age: Duration) -> Vec<u64>` | Returns dirty segment numbers whose `segment_created_at` is older than `max_age`. For age-based flush triggers. |
| `oldest_pinned_segment` | `(&self) -> Option<u64>` | Returns the dirty segment number with the oldest `segment_created_at`. For WAL size pressure — flush the memtable referencing this segment first. |
| `generations_for_segment` | `(&self, segment_number: u64) -> Vec<u64>` | Returns generation IDs associated with a dirty segment. Used to determine which memtables to force-flush. |
| `remove_segment` | `(&mut self, segment_number: u64)` | Removes all tracking state for a segment (call after the segment file has been deleted). |

### 9.3 External Caller API

The Future Work section emphasizes: "The `dirty_map` API must support external callers (the flush pipeline) marking generations as flushed — don't make this internal-only."

This is satisfied by `mark_generation_flushed` being a public method. The flush pipeline (Phase 5) will call:

1. `tracker.mark_generation_flushed(generation_id)` → after S3 SSTable confirmation
2. Check returned clean segment numbers
3. Delete clean segments via `WalWriter::remove_segment`

### 9.4 Flush Trigger Helpers

Methods that help the WAL Manager decide when to force-flush:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `needs_age_flush` | `(&self, max_age: Duration) -> bool` | True if any dirty segment is older than `max_age`. |
| `needs_size_flush` | `(&self, total_wal_size: u64, threshold: u64) -> bool` | True if `total_wal_size` exceeds `threshold`. |

These don't trigger flushes — they inform the WAL Manager, which coordinates with the engine.

---

## Tests

**File:** `crates/flushdb-wal/tests/dirty_tracker_tests.rs`

### Basic Tracking Tests
| Test | What It Validates |
|------|-------------------|
| `test_record_write_creates_segment_entry` | First write to a segment creates a dirty map entry |
| `test_record_write_tracks_highest_sequence` | Multiple writes to same segment+generation → highest seq tracked |
| `test_record_write_multiple_generations` | Two generations writing to same segment → both tracked |
| `test_record_write_multiple_segments` | One generation writing to two segments → both segments tracked |

### Flush and Cleanup Tests
| Test | What It Validates |
|------|-------------------|
| `test_mark_flushed_removes_from_all_segments` | Generation writes to 3 segments, mark flushed → removed from all |
| `test_mark_flushed_returns_clean_segments` | Segment with single generation → becomes clean on flush, returned in result |
| `test_mark_flushed_partial_clean` | Segment with 2 generations → stays dirty after flushing only 1 |
| `test_mark_flushed_unknown_generation` | Flushing unknown generation → returns empty vec (no-op) |
| `test_deletable_segments_empty_initially` | No writes recorded → no deletable segments |
| `test_deletable_segments_after_full_flush` | All generations flushed → segment listed as deletable |

### Query Tests
| Test | What It Validates |
|------|-------------------|
| `test_is_segment_clean_unknown_segment` | Unknown segment → true (conservative default) |
| `test_is_segment_clean_dirty_segment` | Segment with active generation → false |
| `test_dirty_generation_ids_returns_correct_ids` | Returns correct generation IDs for a dirty segment |
| `test_all_dirty_segments_lists_all` | Lists all segments with active generations, sorted |

### Flush Trigger Tests
| Test | What It Validates |
|------|-------------------|
| `test_segments_older_than_threshold` | Segments created > threshold ago are returned |
| `test_oldest_pinned_segment` | Returns segment with earliest `created_at` that is still dirty |
| `test_needs_age_flush_true` | Returns true when any segment exceeds max age |
| `test_needs_age_flush_false` | Returns false when all segments are young |
| `test_needs_size_flush` | Returns true when total WAL size exceeds threshold |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_remove_segment_cleans_all_tracking` | `remove_segment` removes dirty map and created_at |
| `test_multiple_flush_cycles` | Record → flush → record → flush cycle works correctly |
| `test_concurrent_generations_complex` | 5 generations across 3 segments with interleaved flushes → correct cleanup |

---

## Done When

- [ ] Writes are tracked per segment per generation with highest sequence number
- [ ] `mark_generation_flushed` removes generation from all segments and returns newly clean segments
- [ ] Clean segments (empty dirty map) are correctly identified
- [ ] Segment age tracking supports flush trigger queries
- [ ] API is public for external callers (flush pipeline in Phase 5)
- [ ] All tests pass
