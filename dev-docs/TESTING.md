# flushdb — Testing Standards

## Core Rules

1. **Every test must run against real infrastructure.** No mocks, no stubs, no fakes for storage backends. Tests hit actual S3 (MinIO) via testcontainers. If a test can't run, it must fail loudly — never skip silently.
2. **Never skip tests due to service unavailability.** If Docker or MinIO is not running, the test suite must panic with a clear error, not pass with skipped tests. A green test run means every test executed.
3. **No silent error swallowing in test assertions.** Every `match` arm in a test must assert something concrete. Never write `Err(_) => {}` or `Err(_) => return`. If an error is expected, assert the specific variant (`FlushError::NotFound`, `FlushError::PreconditionFailed`, etc.).
4. **Tests must be production-grade, not filler.** Every test must simulate a real-world usage pattern or catch a real edge case. Don't write tests that just exercise the happy path and call it done.

---

## Testcontainers — Mandatory Patterns

All integration tests that need external services (S3/MinIO, etc.) use the `testcontainers` crate. No Docker Compose files, no manual Docker setup.

### Container Lifecycle

- **One shared container per test run.** MinIO starts once via `OnceLock` and is reused across all tests. Each test gets isolation via a unique UUID-based key prefix.
- **Startup cleanup.** Every test run begins by killing stale testcontainers-managed containers from previous runs (`docker rm -f` filtered by `org.testcontainers.managed-by` label). This is the safety net for crashed runs.
- **Post-exit cleanup.** A detached shell process monitors the test process PID and runs `docker rm -f` when it exits. No orphaned containers.
- **Never use `#[ignore]` or conditional skips** to work around missing infrastructure. The test must run or the suite must fail.

### Resource Limits

- **Batch concurrent S3 operations.** Never spawn 1000+ concurrent `tokio::spawn` tasks that each open a TCP connection. Batch in groups of 50 to stay within OS file descriptor limits.
- **Clean up test data.** Every test calls `cleanup_test_prefix()` at the end to remove its objects from MinIO.

### Test Isolation

- Each test gets a unique key prefix: `test-{name}-{uuid}/`
- Tests run in parallel without interference
- `cleanup_test_prefix()` removes all objects under the prefix after each test

---

## S3 StorageBackend Test Categories

All categories below are mandatory for any StorageBackend implementation. See `crates/flushdb-test/tests/s3_backend_tests.rs` for the reference implementation.

### 1. Basic CRUD Operations
- `put` → `get` round-trip returns exact bytes
- `put` with empty bytes (zero-length object)
- `put` with large payload (10 MB — typical SSTable size)
- `get` on non-existent key returns `FlushError::NotFound`
- `delete` on existing key, then `get` returns `NotFound`
- `delete` on non-existent key succeeds silently (idempotent)
- Overwrite: `put` same key twice, `get` returns latest value

### 2. Byte-Range Reads (`get_range`)
- Read first N bytes
- Read last N bytes (footer simulation: `offset = file_len - 80, length = 80`)
- Read middle slice
- Read entire object via range (`offset=0, length=full_size`)
- Range extending beyond object bounds — S3 returns partial content (HTTP 206)
- Range on non-existent key returns `NotFound`
- Multiple sequential `get_range` calls (SSTable open simulation: footer → bloom → index → data block)
- Zero-length range returns empty `Bytes` (NOT an error)
- Zero-length range on non-existent key returns `NotFound`
- Offset at exact file end with `length > 0` returns an error (not a panic)

### 3. Conditional Put (CAS) — Critical Path
- `conditional_put` on new key succeeds
- `conditional_put` on existing key returns `FlushError::PreconditionFailed`
- Two concurrent `conditional_put` calls — exactly one succeeds, the other gets `PreconditionFailed`
- `conditional_put` followed by `get` returns the written data
- `conditional_put`, then `delete`, then `conditional_put` succeeds again
- `put` (unconditional) then `conditional_put` on same key fails
- Stress test: 10 concurrent writers — exactly 1 wins, 9 get `PreconditionFailed`

### 4. List Prefix
- Empty prefix returns all keys
- Specific prefix filters correctly
- Results are sorted lexicographically (critical for manifest discovery)
- Pagination: >1000 objects (S3 returns max 1000 per page) — must batch writes to avoid fd exhaustion
- Non-existent prefix returns empty vec
- Prefix boundary: `list_prefix("a")` matches `"ab"`, `"a/"`, `"abc"` — verify exact string-prefix semantics
- Concurrent list during writes — results always sorted

### 5. S3 Path Convention Tests
Use the actual flushdb path patterns from `STORAGE_DESIGN.md`:
- `flushdb/{namespace}/manifests/00000000000000000001`
- `flushdb/{namespace}/sstables/L0/{ulid}.sst`
- `flushdb/{namespace}/sstables/L1/run-{ulid}/frag-0000.sst`
- `/` in keys works correctly (S3 treats `/` as a regular character)
- `list_prefix("flushdb/ns/manifests/")` returns manifest files in sorted order
- Out-of-order insertion still produces lexicographic sort on list
- Full manifest discovery protocol: write → list → find highest → conditional_put next

### 6. Parity Tests: S3 vs LocalFs
Run the **exact same** test functions against both `S3StorageBackend` (MinIO) and `LocalFsBackend` (tempdir). This catches semantic divergence between implementations.

Required parity functions:
- `put`/`get` round-trip
- `get` on non-existent key → `NotFound`
- `put` overwrite
- `get_range` (partial, first byte, full object)
- `get_range` on non-existent key → `NotFound`
- `get_range` with zero length → empty `Bytes`
- `get_range` zero length on missing key → `NotFound`
- `conditional_put` (new key succeeds, existing fails)
- `conditional_put` after delete succeeds again
- `delete` idempotency (double delete succeeds)
- `delete` then `get` → `NotFound`
- `list_prefix` sorting
- `list_prefix` on non-existent prefix → empty
- Binary data (all 256 byte values) round-trip
- Deeply nested path keys

### 7. Error Handling and Edge Cases
- Very long key names (near S3's 1024-byte limit, use path segments for MinIO)
- Binary data in values (not just UTF-8)
- Empty prefix listing on empty backend
- Concurrent reads and writes to the same key
- Zero-byte value (`Bytes::new()`)
- Special URL characters in keys: `+`, `%`, `@`, `=`, spaces, `#`, `?`
- Overwrite then range read sees new data (not stale)
- Range read at exact end boundary

---

## Test Commands

```bash
# Run S3 integration tests (requires Docker)
cargo test -p flushdb-test --features s3 s3

# Run all tests including S3
cargo test --workspace --features s3

# Run parity tests only
cargo test -p flushdb-test --features s3 parity

# Run non-S3 tests (no Docker required)
cargo test --workspace
```

---

## Adding New StorageBackend Tests

When adding a new `StorageBackend` implementation:

1. Add all 7 test categories above against the new backend
2. Add the new backend to every parity function call in `test_parity_*`
3. Ensure the new backend's container cleanup follows the testcontainers patterns
4. Run `cargo test --workspace --features <feature>` — zero failures, zero ignored, zero skipped
