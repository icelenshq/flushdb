# Task 2: FlushError — Unified Error Types

**Crate:** `flushdb-types`
**File:** `src/error.rs`
**Depends on:** Task 1 (workspace scaffolding)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md (error handling throughout), Phase 1 §2

---

## Goal

Define the unified error enum that every crate in the workspace will depend on. This must be comprehensive from day one — adding error variants later forces cascading changes across all `match` arms in the codebase.

---

## What to Build

### 2.1 FlushError Enum

Use `thiserror` for derivation. Every variant must carry enough context for useful diagnostics.

**Variants:**

| Variant | Context Fields | When Used | Phase First Used |
|---------|---------------|-----------|-----------------|
| `Io` | wraps `std::io::Error` | Filesystem or network I/O failure | P1 (LocalFsBackend) |
| `KeyTooLong` | `field: &'static str`, `actual: usize`, `max: usize` | record_id > 256B or item_key > 4096B | P1 (CompositeKey) |
| `InvalidKey` | `reason: String` | record_id contains null byte, is empty, or other format violations | P1 (CompositeKey) |
| `NotFound` | `key: String` | Requested record, item, or storage object does not exist | P1 (StorageBackend) |
| `PreconditionFailed` | `message: String` | CAS conflict (manifest update, lease acquisition) | P1 (conditional_put) |
| `CrcMismatch` | `expected: u32`, `actual: u32` | Data integrity failure in WAL entry or SSTable block | P2 (WAL) |
| `CorruptedData` | `message: String` | Structural corruption (bad magic number, invalid format, truncated data) | P2 (WAL) |
| `DuplicateToken` | `token: String` | Idempotency token already applied within retention window | P2 (WAL dedup) |
| `ResourceExhausted` | `resource: String`, `message: String` | Backpressure — WAL size limit, L0 stall, memory pressure | P5 |
| `EpochFenced` | `expected: u64`, `actual: u64` | Zombie writer/compactor detected — stale epoch in manifest CAS | P5/P7 |
| `InvalidArgument` | `message: String` | Malformed request parameters (bad protobuf, invalid predicate, etc.) | P7 (gRPC) |

### 2.2 Design Decisions

- **`#[derive(Debug)]` is mandatory** — errors must be printable for logging
- **Display messages via `thiserror`** — use `#[error("...")]` attributes, not manual `Display` impl
- **`Io` wraps `std::io::Error` via `#[from]`** — enables `?` propagation from all I/O operations
- **No generic `Other(String)` variant** — every failure mode must be explicitly categorized
- **`Send + Sync` bounds** — the error must be sendable across threads (async runtime requirement). `std::io::Error` is already `Send + Sync`, so this should work naturally.

### 2.3 Type Alias

Define a convenience type alias:

```
pub type FlushResult<T> = Result<T, FlushError>;
```

This is used throughout the codebase to avoid repeating `Result<T, FlushError>`.

### 2.4 Future-Proofing

The following variants won't be used until later phases, but defining them now avoids error type refactoring:
- `CrcMismatch` — used starting Phase 2 (WAL)
- `CorruptedData` — used starting Phase 2 (WAL)
- `DuplicateToken` — used starting Phase 2 (WAL dedup)
- `ResourceExhausted` — used starting Phase 5 (backpressure)
- `EpochFenced` — used starting Phase 5 (manifest) / Phase 7 (cluster)

Even though these aren't exercised in Phase 1, they must be present so that downstream crates can pattern-match against them without modifying the error type.

---

## Tests

**File:** `crates/flushdb-types/tests/error_tests.rs`

| Test | What It Validates |
|------|-------------------|
| `test_io_error_from_std` | `std::io::Error` converts to `FlushError::Io` via `?` operator |
| `test_key_too_long_display` | Display message includes field name, actual size, and max size |
| `test_invalid_key_display` | Display message includes the reason string |
| `test_not_found_display` | Display message includes the key |
| `test_precondition_failed_display` | Display message includes the message |
| `test_crc_mismatch_display` | Display shows expected vs actual CRC values |
| `test_corrupted_data_display` | Display includes the corruption description |
| `test_epoch_fenced_display` | Display shows expected vs actual epoch |
| `test_resource_exhausted_display` | Display shows resource name and message |
| `test_error_is_send_sync` | Compile-time assertion that `FlushError: Send + Sync` |
| `test_flush_result_alias` | `FlushResult<u64>` works as expected for Ok and Err |

---

## Done When

- [ ] `FlushError` compiles with all 11 variants
- [ ] `FlushResult<T>` type alias exists
- [ ] `std::io::Error` converts via `From` / `?`
- [ ] All display messages are informative (not just variant names)
- [ ] Error type is `Send + Sync`
- [ ] All tests pass
