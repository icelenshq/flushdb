# Task 7: WAL Reader & Recovery

**Crate:** `flushdb-wal`
**File:** `src/wal_reader.rs`
**Depends on:** Task 5 (SegmentReader)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §5.2; phase-2-wal.md §4, Future Work Considerations

---

## Goal

Build the multi-segment WAL reader that replays entries across all segments in sequence order with support for filtering by sequence number range. This is how crash recovery works — the engine reads `last_flushed_sequence` from the manifest and replays all WAL entries after that point.

---

## What to Build

### 7.1 WalReader Struct

| Field | Type | Description |
|-------|------|-------------|
| `partition_dir` | `PathBuf` | WAL directory to read from |
| `segment_numbers` | `Vec<u64>` | Sorted segment numbers discovered on disk |

### 7.2 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `(partition_dir: &Path) -> FlushResult<Self>` | Discovers and sorts all `.wal` files in the directory. Does NOT read segment contents yet (lazy). Returns error if directory doesn't exist. |
| `replay_all` | `(&self) -> FlushResult<Vec<WalEntry>>` | Reads all entries from all segments in sequence order. Collects into a Vec. |
| `replay_from` | `(&self, min_sequence: u64) -> FlushResult<Vec<WalEntry>>` | Reads entries with `sequence_number >= min_sequence`. Uses segment headers to skip segments whose entries are all below min_sequence. |
| `iter` | `(&self) -> WalEntryIterator` | Returns a lazy iterator over all entries across all segments. |
| `iter_from` | `(&self, min_sequence: u64) -> WalEntryIterator` | Returns a lazy iterator that only yields entries with `sequence_number >= min_sequence`. |
| `segment_count` | `(&self) -> usize` | Number of segments discovered. |
| `is_empty` | `(&self) -> bool` | True if no segments exist. |

### 7.3 WalEntryIterator

An iterator over entries across multiple segments:

- Implements `Iterator<Item = FlushResult<WalEntry>>`
- Opens segments lazily as iteration progresses (reads segment file only when the previous segment is exhausted)
- Reads segments in ascending segment number order
- Within each segment, yields entries in file order (which is sequence order)
- Supports optional `min_sequence` filter: skips entries with `sequence_number < min_sequence`

**Segment skipping optimization:** Before opening a segment, check the *next* segment's header `starting_sequence_number`. If the next segment's starting sequence is ≤ `min_sequence`, the current segment can be skipped entirely (all its entries are below the filter threshold). If there is no next segment, the current segment must be scanned entry-by-entry.

### 7.4 Recovery Semantics

- Entries are yielded in sequence number order (guaranteed by: segments read in number order + entries appended in sequence order within each segment)
- If a segment has tail corruption (partial write at end): the corrupted tail entries are silently discarded, iteration continues to the next segment
- If a segment has mid-segment corruption: the reader returns the `CrcMismatch` error — recovery halts
- An empty WAL (no segment files) returns an empty iterator / empty Vec

---

## Tests

**File:** `crates/flushdb-wal/tests/wal_reader_tests.rs`

All tests use `tempfile::tempdir()` and write entries using WalWriter (Task 6).

### Multi-Segment Replay Tests
| Test | What It Validates |
|------|-------------------|
| `test_replay_single_segment` | Write 100 entries in one segment → `replay_all` returns 100 entries in order |
| `test_replay_across_segments` | Write entries across 3 segments (via rotation) → `replay_all` returns all entries in sequence order |
| `test_replay_empty_wal` | No segments → `replay_all` returns empty vec |
| `test_replay_preserves_entry_data` | All entry fields (namespace, record_id, value, metadata, etc.) match originals |

### Sequence Filtering Tests
| Test | What It Validates |
|------|-------------------|
| `test_replay_from_filters_by_sequence` | Write 100 entries, `replay_from(50)` → returns entries with seq 50–100 |
| `test_replay_from_skips_entire_segments` | 3 segments with seqs 1–30, 31–60, 61–90 → `replay_from(61)` skips segments 1 and 2 |
| `test_replay_from_with_min_zero` | `replay_from(0)` returns all entries |
| `test_replay_from_with_min_beyond_max` | `replay_from(999)` when max seq is 100 → empty result |
| `test_replay_from_at_segment_boundary` | `replay_from(31)` when segment 2 starts at seq 31 → returns segments 2 and 3 |

### Iterator Tests
| Test | What It Validates |
|------|-------------------|
| `test_iter_yields_all_entries` | `iter()` yields same entries as `replay_all()` |
| `test_iter_from_skips_early_entries` | `iter_from(50)` only yields entries with seq >= 50 |

### Corruption Handling Tests
| Test | What It Validates |
|------|-------------------|
| `test_replay_with_tail_corruption` | Last segment has partial write → all complete entries recovered, partial discarded |
| `test_replay_stops_on_mid_segment_corruption` | Corruption in middle of segment → error returned |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_replay_with_gaps_in_segment_numbers` | Segments 1, 3, 5 (gaps from deletion) → replay works correctly |
| `test_replay_single_entry` | WAL with exactly 1 entry replays correctly |
| `test_open_nonexistent_directory` | Returns error |

---

## Done When

- [ ] Multi-segment replay returns entries in sequence order
- [ ] Sequence number filtering correctly skips segments and entries
- [ ] Tail corruption in last segment doesn't prevent recovery of valid entries
- [ ] Segment skipping optimization uses header `starting_sequence_number`
- [ ] Iterator provides lazy evaluation across segments
- [ ] All tests pass
