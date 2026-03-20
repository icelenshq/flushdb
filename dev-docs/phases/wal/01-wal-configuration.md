# Task 1: WAL Configuration & Constants

**Crate:** `flushdb-wal`
**File:** `src/config.rs`
**Depends on:** Nothing
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §5.1, §5.3, §5.4, §5.5; phase-2-wal.md §1, §5, §6, §7

---

## Goal

Define all WAL configuration, constants, and utility types. This is the foundation that every other WAL component references — segment sizing, fsync modes, commit intervals, backpressure thresholds, and segment file naming conventions.

---

## What to Build

### 1.1 Constants

```
WAL_MAGIC: [u8; 4] = *b"FWAL"
WAL_VERSION: u8 = 1
SEGMENT_HEADER_SIZE: usize = 32
SEGMENT_FILE_EXTENSION: &str = ".wal"
SEGMENT_FILE_PREFIX: &str = "segment-"
SEGMENT_NUMBER_WIDTH: usize = 12
```

### 1.2 FsyncMode Enum

Two sync modes from STORAGE_DESIGN.md §5.4:

| Variant | Behavior |
|---------|----------|
| `Sync` | `fsync()` after every write batch — survives power loss |
| `BatchSync` | `fsync()` on timer (configurable, default 10ms) — may lose last interval on power loss |

Derives: `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`

Default: `Sync`

### 1.3 WalConfig Struct

All tunables for the WAL subsystem:

| Field | Type | Default | Source |
|-------|------|---------|--------|
| `segment_size_target` | `u64` | `33_554_432` (32 MB) | §5.1 — rotation threshold |
| `max_wal_size` | `u64` | `268_435_456` (256 MB) | §5.5 — backpressure, stall writes with `ResourceExhausted` |
| `max_total_wal_bytes` | `u64` | `536_870_912` (512 MB) | §5.5 — force-flush trigger |
| `segment_max_age` | `Duration` | `Duration::from_secs(300)` (5 min) | §5.5 — segment age flush trigger |
| `fsync_mode` | `FsyncMode` | `FsyncMode::Sync` | §5.4 |
| `group_commit_interval` | `Duration` | `Duration::from_micros(200)` | §5.3 — timer trigger |
| `group_commit_max_bytes` | `usize` | `262_144` (256 KB) | §5.3 — size trigger |
| `batch_sync_interval` | `Duration` | `Duration::from_millis(10)` | §5.4 — BatchSync fsync timer |

Derives: `Debug`, `Clone`

Provide `Default` impl with all defaults above.

### 1.4 Segment Naming Utilities

Functions for segment file naming and discovery:

| Function | Signature | Behavior |
|----------|-----------|----------|
| `segment_filename` | `(segment_number: u64) -> String` | Returns `segment-{number:012}.wal` (12-digit zero-padded) |
| `parse_segment_number` | `(filename: &str) -> Option<u64>` | Extracts segment number from filename. Returns `None` if format doesn't match (wrong prefix, extension, or non-numeric middle). |
| `segment_path` | `(dir: &Path, segment_number: u64) -> PathBuf` | Joins directory with segment filename. |

---

## Tests

**File:** `crates/flushdb-wal/tests/config_tests.rs`

### Default Value Tests
| Test | What It Validates |
|------|-------------------|
| `test_default_config_values` | `WalConfig::default()` returns expected defaults for all fields |
| `test_fsync_mode_default_is_sync` | `FsyncMode::default()` == `FsyncMode::Sync` |

### Segment Naming Tests
| Test | What It Validates |
|------|-------------------|
| `test_segment_filename_zero_padded` | `segment_filename(1)` → `"segment-000000000001.wal"` |
| `test_segment_filename_large_number` | `segment_filename(999_999_999_999)` → `"segment-999999999999.wal"` |
| `test_parse_segment_number_valid` | `parse_segment_number("segment-000000000042.wal")` → `Some(42)` |
| `test_parse_segment_number_invalid_prefix` | `parse_segment_number("log-000000000001.wal")` → `None` |
| `test_parse_segment_number_invalid_extension` | `parse_segment_number("segment-000000000001.log")` → `None` |
| `test_parse_segment_number_non_numeric` | `parse_segment_number("segment-abcdefghijkl.wal")` → `None` |
| `test_segment_filename_roundtrip` | `parse_segment_number(&segment_filename(N))` → `Some(N)` for various N |
| `test_segment_path_joins_correctly` | `segment_path(dir, 5)` produces `{dir}/segment-000000000005.wal` |

---

## Done When

- [ ] WalConfig has all fields with correct defaults
- [ ] FsyncMode has Sync and BatchSync variants
- [ ] Segment naming functions produce 12-digit zero-padded filenames
- [ ] Segment filename round-trip (format → parse) is lossless
- [ ] All tests pass
