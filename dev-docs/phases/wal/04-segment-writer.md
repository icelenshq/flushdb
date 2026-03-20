# Task 4: Segment Writer

**Crate:** `flushdb-wal`
**File:** `src/segment_writer.rs`
**Depends on:** Task 1 (config, segment naming), Task 2 (WalEntry encoding), Task 3 (SegmentHeader)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §5.1, §5.4; phase-2-wal.md §3

---

## Goal

Implement a single-segment writer that creates segment files, writes the header, appends encoded WAL entries, and handles fsync. This is the lowest-level I/O component — one SegmentWriter manages exactly one segment file.

---

## What to Build

### 4.1 SegmentWriter Struct

| Field | Type | Description |
|-------|------|-------------|
| `file` | `std::fs::File` | Open file handle for the segment |
| `segment_number` | `u64` | This segment's number |
| `current_size` | `u64` | Current byte offset (starts at SEGMENT_HEADER_SIZE after header write) |
| `entry_count` | `u64` | Number of entries written to this segment |
| `path` | `PathBuf` | Absolute path to the segment file |

**Design decisions:**
- Uses `std::fs::File` (synchronous) because WAL writes are sequential and `fsync()` is inherently blocking. The async boundary lives at the group commit layer (Task 8), which calls sync I/O via `spawn_blocking`.
- Single-owner — no concurrent writes to the same segment.

### 4.2 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `create` | `(dir: &Path, segment_number: u64, starting_sequence: u64) -> FlushResult<Self>` | Creates new segment file, writes 32-byte header, returns writer positioned after header. Creates parent directories if they don't exist. Fails if file already exists. |
| `append` | `(&mut self, entry: &WalEntry) -> FlushResult<u64>` | Encodes entry via `entry.encode()`, writes bytes to file, updates current_size and entry_count. Returns the byte offset where this entry starts. Does NOT fsync. |
| `append_batch` | `(&mut self, entries: &[WalEntry]) -> FlushResult<()>` | Encodes all entries, concatenates into a single buffer, writes in one I/O call. More efficient for group commit batches. Updates current_size and entry_count. |
| `sync` | `(&mut self) -> FlushResult<()>` | Calls `file.sync_data()` (fdatasync equivalent). Ensures all appended data is durable on disk. |
| `current_size` | `(&self) -> u64` | Returns current segment file size. |
| `segment_number` | `(&self) -> u64` | Returns this segment's number. |
| `entry_count` | `(&self) -> u64` | Returns number of entries written. |
| `path` | `(&self) -> &Path` | Returns the segment file path. |

### 4.3 Error Handling

| Scenario | Behavior |
|----------|----------|
| File already exists on `create` | Return `FlushError::Io` wrapping the OS error |
| Write failure | Return `FlushError::Io` — caller decides whether to retry or abort |
| Fsync failure | Return `FlushError::Io` — critical error, caller must handle |
| Parent directory doesn't exist | `create` makes parent dirs via `std::fs::create_dir_all` |

### 4.4 Trait Does NOT Include

- **No segment rotation logic** — SegmentWriter writes to one file. Rotation is WalWriter's job (Task 6).
- **No group commit batching** — SegmentWriter writes what it's given. Batching is the Group Commit layer's job (Task 8).
- **No async methods** — sync I/O only. Async wrapping happens at the group commit level.

---

## Tests

**File:** `crates/flushdb-wal/tests/segment_writer_tests.rs`

All tests use `tempfile::tempdir()` for isolated test directories.

### Basic Write Tests
| Test | What It Validates |
|------|-------------------|
| `test_create_segment_writes_header` | Created segment file starts with valid 32-byte header |
| `test_create_segment_size_is_header_size` | Newly created segment has `current_size() == 32` |
| `test_append_single_entry` | Append one entry, `current_size()` increases by `entry.total_size()` |
| `test_append_multiple_entries` | Append 100 entries, `current_size()` is header + sum of all `total_size()` |
| `test_append_batch_equivalent_to_individual` | `append_batch` produces identical file bytes as individual appends |
| `test_entry_count_tracks_appends` | `entry_count()` increments correctly after each append |

### File Content Tests
| Test | What It Validates |
|------|-------------------|
| `test_written_bytes_are_decodable` | Read raw file bytes, parse header + entries, all match originals |
| `test_entries_written_contiguously` | No gaps between entries in the file |
| `test_large_entry_writes_correctly` | 1MB value entry writes completely and is readable |

### Fsync Tests
| Test | What It Validates |
|------|-------------------|
| `test_sync_completes_without_error` | `sync()` succeeds on a valid file |
| `test_sync_after_no_writes` | `sync()` on freshly created segment succeeds |

### Error Tests
| Test | What It Validates |
|------|-------------------|
| `test_create_fails_if_file_exists` | Creating segment with existing filename returns Io error |
| `test_create_makes_parent_directories` | Creating segment in non-existent subdirectory succeeds |

---

## Done When

- [ ] Segments are created with valid 32-byte header
- [ ] Entries append contiguously after the header
- [ ] `current_size()` accurately tracks file position
- [ ] `sync()` calls fdatasync on the underlying file
- [ ] `append_batch` writes all entries in a single I/O operation
- [ ] File creation handles missing parent directories
- [ ] All tests pass
