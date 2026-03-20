# Task 5: Segment Reader

**Crate:** `flushdb-wal`
**File:** `src/segment_reader.rs`
**Depends on:** Task 2 (WalEntry decoding), Task 3 (SegmentHeader decoding)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §5.2; phase-2-wal.md §4

---

## Goal

Implement a single-segment reader that parses WAL entries from a segment file with CRC validation and corruption detection. This is the foundation of crash recovery — it must correctly handle partial writes at the segment tail (normal crash artifact) and detect true corruption mid-segment.

---

## What to Build

### 5.1 SegmentReader Struct

| Field | Type | Description |
|-------|------|-------------|
| `data` | `Vec<u8>` | Full segment file contents loaded into memory |
| `header` | `SegmentHeader` | Parsed segment header |
| `position` | `usize` | Current read position (starts at SEGMENT_HEADER_SIZE after header) |
| `segment_path` | `PathBuf` | Path for error reporting |

**Design decisions:**
- Loads entire segment into memory. Segments are ≤ 32MB (the rotation target), so this is acceptable. Memory-mapping is a potential future optimization but adds complexity without meaningful benefit for recovery (which is infrequent).
- Read-only — the reader never modifies the segment file.

### 5.2 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `(path: &Path) -> FlushResult<Self>` | Reads entire file into memory, parses and validates header, positions cursor at `SEGMENT_HEADER_SIZE`. Returns error if file is too short for header or header is invalid. |
| `header` | `(&self) -> &SegmentHeader` | Returns the parsed segment header. |
| `segment_number` | `(&self) -> u64` | Returns segment number from header. |
| `next_entry` | `(&mut self) -> FlushResult<Option<WalEntry>>` | Reads next entry. Returns `Ok(None)` at end of segment or on tail truncation. Returns `Err` on mid-segment corruption. See parsing algorithm below. |
| `entries` | `(self) -> SegmentEntryIterator` | Converts into an iterator yielding `FlushResult<WalEntry>`. |

### 5.3 Entry Parsing Algorithm

The parsing algorithm from phase-2-wal.md §4:

1. If fewer than 4 bytes remain from current position: return `Ok(None)` (end of segment)
2. Read `entry_length` (4 bytes, LE u32)
3. If `entry_length == 0`: return `Ok(None)` (zero padding / unused space)
4. Check if `entry_length + 4` bytes (body + CRC) remain after the length prefix
5. If not enough bytes remain: **tail truncation** — partial write from crash. Log `tracing::warn!` and return `Ok(None)`.
6. Read `entry_length` bytes as body, read next 4 bytes as expected CRC (LE u32)
7. Compute CRC32 over body bytes, compare to expected CRC
8. If CRC passes: decode body into `WalEntry`, advance position past entry_length + body + CRC, return `Ok(Some(entry))`
9. If CRC fails:
   - Check if all remaining bytes after this position are zeros, or if fewer bytes remain than a minimum valid entry. If so: **tail corruption** from crash — log warning, return `Ok(None)`.
   - Otherwise: **mid-segment corruption** — return `Err(FlushError::CrcMismatch { expected, actual })`. Recovery halts.

### 5.4 SegmentEntryIterator

An iterator adapter over SegmentReader:

- Implements `Iterator<Item = FlushResult<WalEntry>>`
- On `Ok(None)` from `next_entry()`: iteration stops (`next()` returns `None`)
- On `Err(...)`: yields the error as the last item, then stops
- On `Ok(Some(entry))`: yields `Ok(entry)` and continues

---

## Tests

**File:** `crates/flushdb-wal/tests/segment_reader_tests.rs`

All tests use `tempfile::tempdir()` and write segment files using SegmentWriter (Task 4).

### Happy Path Tests
| Test | What It Validates |
|------|-------------------|
| `test_read_single_entry` | Write 1 entry, read 1 entry — all fields match |
| `test_read_multiple_entries` | Write 100 entries, read 100 entries — all match in order |
| `test_read_empty_segment` | Segment with header only → `next_entry()` returns `None` |
| `test_header_parsed_correctly` | Reader's header matches the header that was written |
| `test_entries_iterator` | `entries()` yields all entries then stops |

### Corruption Detection Tests
| Test | What It Validates |
|------|-------------------|
| `test_tail_truncation_extra_bytes` | Write 3 entries, append 10 random bytes → reads 3 entries, returns None (no error) |
| `test_tail_truncation_partial_length_prefix` | Write entries, append 2 bytes → reads all complete entries, ignores partial |
| `test_tail_truncation_partial_body` | Write entries, then write entry_length prefix but incomplete body → reads complete entries, ignores partial |
| `test_mid_segment_corruption_returns_error` | Write 5 entries, corrupt a byte in entry 3's body → reads 2 entries, returns CrcMismatch on entry 3 |
| `test_zero_entry_length_stops_iteration` | Insert 4 zero bytes between valid entries → reads entries before, then returns None |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_segment_with_only_header` | 32-byte file → valid header, no entries |
| `test_entry_with_empty_variable_fields` | Entry with zero-length namespace, key, value, metadata reads correctly |
| `test_entry_with_large_value` | 1MB value entry reads correctly |
| `test_binary_data_in_all_fields` | Non-UTF8 bytes in namespace, record_id, item_key round-trip |

### Header Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_open_rejects_empty_file` | 0-byte file → CorruptedData |
| `test_open_rejects_short_file` | 16-byte file → CorruptedData |
| `test_open_rejects_bad_magic` | File with wrong magic bytes → CorruptedData |

---

## Done When

- [ ] Reader correctly parses all entries written by SegmentWriter
- [ ] Tail corruption (partial write at end) is silently discarded with a warning log
- [ ] Mid-segment corruption returns CrcMismatch error
- [ ] Header validation rejects invalid segment files
- [ ] Empty segments (header only) produce zero entries
- [ ] Iterator adapter works correctly
- [ ] All tests pass
