# Task 6: WAL Writer

**Crate:** `flushdb-wal`
**File:** `src/wal_writer.rs`
**Depends on:** Task 4 (SegmentWriter), Task 5 (SegmentReader — for sequence recovery on startup)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §5.1, §5.3; phase-2-wal.md §1, §3, Future Work Considerations

---

## Goal

Build the multi-segment WAL writer that manages segment rotation, monotonic sequence number assignment, and startup recovery of the sequence counter. This is the core write-path component that group commit (Task 8) builds on top of.

---

## What to Build

### 6.1 WalWriter Struct

| Field | Type | Description |
|-------|------|-------------|
| `partition_dir` | `PathBuf` | WAL directory for this partition (e.g., `wal/partition-0/`) |
| `config` | `WalConfig` | Configuration (segment size target, etc.) |
| `current_writer` | `SegmentWriter` | Active segment being appended to |
| `next_sequence_number` | `u64` | Next sequence number to assign (monotonically increasing) |
| `segment_numbers` | `Vec<u64>` | Sorted list of all existing segment numbers (active + old) |

### 6.2 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `(partition_dir: &Path, config: &WalConfig) -> FlushResult<Self>` | Opens or creates the WAL directory. Discovers existing segments, recovers last sequence number, opens or creates current segment. See startup protocol below. |
| `append` | `(&mut self, entry: &mut WalEntry) -> FlushResult<()>` | Assigns `self.next_sequence_number` to `entry.sequence_number`, increments counter, encodes and appends to current segment. Triggers rotation if size exceeds target. |
| `append_batch` | `(&mut self, entries: &mut [WalEntry]) -> FlushResult<()>` | Assigns consecutive sequence numbers to all entries, writes batch to current segment via `SegmentWriter::append_batch`. May rotate mid-batch if size exceeded. |
| `sync` | `(&mut self) -> FlushResult<()>` | Fsyncs the current segment. |
| `rotate` | `(&mut self) -> FlushResult<()>` | Syncs current segment, creates next segment with `number = current + 1`, switches `current_writer` to the new segment. |
| `current_segment_number` | `(&self) -> u64` | Returns the active segment's number. |
| `next_sequence_number` | `(&self) -> u64` | Returns the next sequence number that will be assigned. |
| `active_segment_numbers` | `(&self) -> &[u64]` | Returns sorted slice of all existing segment numbers. |
| `total_size` | `(&self) -> FlushResult<u64>` | Calculates total WAL size across all segment files on disk. |
| `remove_segment` | `(&mut self, segment_number: u64) -> FlushResult<()>` | Deletes a segment file and removes it from `segment_numbers`. Returns error if attempting to remove the current (active) segment. |

### 6.3 Startup Protocol

When `open()` is called:

1. Create `partition_dir` if it doesn't exist (`std::fs::create_dir_all`)
2. List all `.wal` files in the directory, parse segment numbers via `parse_segment_number`, sort ascending
3. **If no segments exist:** create segment 1 with `starting_sequence = 1`, set `next_sequence_number = 1`
4. **If segments exist:**
   a. Open the **last** segment (highest number) with SegmentReader
   b. Iterate all entries to find the highest sequence number
   c. If entries found: `next_sequence_number = highest_seq + 1`
   d. If last segment is empty (header only): scan the second-to-last segment, and so on
   e. If all segments are empty: `next_sequence_number = 1`
   f. Create a **new** segment with `number = last + 1` for fresh appends (don't reopen the last segment — simpler than tracking file position in an existing file)

**Design note from Future Work:** "Ensure the counter survives segment rotation and the last-assigned number is recoverable from the WAL on startup." This startup protocol satisfies that — the sequence is recovered by reading entries, not stored in a separate file.

### 6.4 Segment Rotation Logic

After each `append` or `append_batch`:

1. Check if `current_writer.current_size() >= config.segment_size_target`
2. If so, call `rotate()`:
   a. Sync current segment (`current_writer.sync()`)
   b. New segment number = `current_writer.segment_number() + 1`
   c. Create new segment with `starting_sequence_number = self.next_sequence_number`
   d. Switch `current_writer` to the new SegmentWriter
   e. Add new segment number to `segment_numbers`

### 6.5 Sequence Number Assignment

- `append()` assigns `self.next_sequence_number` to the entry and increments the counter
- `append_batch()` assigns consecutive sequence numbers: `N, N+1, N+2, ...` for a batch of entries
- Sequence numbers are globally monotonic across segments — they never reset on rotation
- The counter is a simple `u64` field — no persistence needed beyond what the entries themselves carry

---

## Tests

**File:** `crates/flushdb-wal/tests/wal_writer_tests.rs`

All tests use `tempfile::tempdir()`.

### Startup Tests
| Test | What It Validates |
|------|-------------------|
| `test_open_creates_first_segment` | Opening empty directory creates `segment-000000000001.wal` |
| `test_open_recovers_sequence_from_existing` | Write entries, drop writer, reopen → `next_sequence_number()` continues correctly |
| `test_open_recovers_from_empty_last_segment` | Write entries, rotate (creating empty new segment), reopen → sequence recovered from previous segment |
| `test_open_handles_gaps_in_segment_numbers` | Segments 1, 3, 5 (gaps from deletion) → opens correctly, creates segment 6 |

### Append Tests
| Test | What It Validates |
|------|-------------------|
| `test_append_assigns_monotonic_sequences` | 100 appends → sequence numbers are 1, 2, 3, ..., 100 |
| `test_append_batch_assigns_consecutive_sequences` | Batch of 10 entries → sequences are N, N+1, ..., N+9 |
| `test_appended_entries_readable` | Write 10 entries, read back with SegmentReader → all match |

### Rotation Tests
| Test | What It Validates |
|------|-------------------|
| `test_rotation_at_size_threshold` | Set `segment_size_target = 1024`, write entries until rotation → new segment created |
| `test_rotation_preserves_sequence_continuity` | Sequence numbers don't reset or skip after rotation |
| `test_rotation_syncs_old_segment` | After rotation, old segment's entries are readable (was synced) |
| `test_multiple_rotations` | Write enough entries for 3+ rotations → all segments valid and readable |
| `test_rotation_creates_consecutive_segment_numbers` | Segment numbers are 1, 2, 3, ... (no gaps during normal operation) |

### Size Tracking Tests
| Test | What It Validates |
|------|-------------------|
| `test_total_size_across_segments` | `total_size()` sums all segment file sizes |
| `test_total_size_after_segment_removal` | Remove a segment → `total_size()` decreases |

### Segment Removal Tests
| Test | What It Validates |
|------|-------------------|
| `test_remove_segment_deletes_file` | `remove_segment()` deletes the file from disk |
| `test_remove_segment_updates_list` | Segment number removed from `active_segment_numbers()` |
| `test_remove_current_segment_returns_error` | Cannot remove the segment currently being written to |

---

## Done When

- [ ] WAL opens correctly on empty directory (creates first segment)
- [ ] Sequence number recovery works after restart
- [ ] Monotonic sequence assignment across appends and batches
- [ ] Segment rotation triggers at size threshold
- [ ] Sequence numbers are continuous across segment boundaries
- [ ] Segment removal deletes files and updates tracking
- [ ] All tests pass
