# Task 2: S3 Integration Tests (Testcontainers/MinIO)

**Crate:** `flushdb-test`
**File:** `tests/s3_backend_tests.rs`, `src/s3_test_utils.rs`
**Depends on:** Task 1 (S3StorageBackend)
**Estimated complexity:** L
**Design reference:** docs/TESTING.md (all 7 categories), STORAGE_DESIGN.md §23

---

## Goal

Build comprehensive S3 integration tests against MinIO using testcontainers. This validates that the S3StorageBackend behaves identically to LocalFsBackend and satisfies all 7 mandatory test categories from TESTING.md. These tests run with real S3-compatible infrastructure — no mocks, no fakes.

---

## What to Build

### 2.1 Test Infrastructure — MinIO Container

Shared MinIO container managed via `testcontainers` crate with `OnceLock`:

```
static MINIO: OnceLock<MinioContainer> = OnceLock::new();
```

**Container setup:**
- Image: `minio/minio:latest` (or pinned version)
- Startup: `minio server /data`
- Port: expose 9000
- Credentials: `minioadmin` / `minioadmin` (default MinIO root credentials)
- Create a test bucket on first use

**Lifecycle rules (from TESTING.md):**
- One shared container per test run via `OnceLock`
- Startup cleanup: kill stale testcontainers-managed containers from previous runs
- Post-exit cleanup: detached shell process monitors test PID and runs `docker rm -f` on exit
- Never use `#[ignore]` or conditional skips

### 2.2 Test Isolation

Each test gets a unique key prefix for isolation:

```
fn test_prefix(test_name: &str) -> String {
    format!("test-{}-{}/", test_name, Uuid::new_v4())
}
```

**Cleanup:** Every test calls `cleanup_test_prefix()` at the end to remove all objects under its prefix.

**Concurrency:** Batch S3 operations in groups of 50 to stay within OS file descriptor limits.

### 2.3 S3StorageBackend Factory

Helper to create an `S3StorageBackend` pointed at the MinIO container:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `create_test_s3_backend` | `() -> S3StorageBackend` | Returns backend configured for MinIO (endpoint override, path-style access, test bucket) |
| `cleanup_test_prefix` | `(backend: &S3StorageBackend, prefix: &str) -> ()` | Lists and deletes all objects under prefix |

### 2.4 Test Categories (All 7 from TESTING.md)

All categories below are mandatory. Each test uses the test prefix for isolation.

---

## Tests

**File:** `crates/flushdb-test/tests/s3_backend_tests.rs`

### Category 1: Basic CRUD Operations
| Test | What It Validates |
|------|-------------------|
| `test_s3_put_get_round_trip` | `put` → `get` returns exact bytes |
| `test_s3_put_empty_bytes` | `put` with zero-length payload, `get` returns empty `Bytes` |
| `test_s3_put_large_payload` | `put` with 10 MB payload (typical SSTable size), `get` returns exact bytes |
| `test_s3_get_nonexistent_key` | `get` on non-existent key returns `FlushError::NotFound` |
| `test_s3_delete_existing_key` | `delete` existing key, then `get` returns `NotFound` |
| `test_s3_delete_nonexistent_idempotent` | `delete` on non-existent key succeeds silently |
| `test_s3_put_overwrite` | `put` same key twice, `get` returns latest value |

### Category 2: Byte-Range Reads (get_range)
| Test | What It Validates |
|------|-------------------|
| `test_s3_get_range_first_n_bytes` | Read first N bytes of an object |
| `test_s3_get_range_last_n_bytes` | Read last N bytes (footer simulation: `offset = file_len - 80, length = 80`) |
| `test_s3_get_range_middle_slice` | Read a middle slice |
| `test_s3_get_range_entire_object` | Read entire object via range (`offset=0, length=full_size`) |
| `test_s3_get_range_extends_beyond` | Range extending beyond object bounds — returns partial content |
| `test_s3_get_range_nonexistent_key` | Range on non-existent key returns `NotFound` |
| `test_s3_get_range_sequential_reads` | Multiple sequential `get_range` calls (SSTable open simulation: footer → bloom → index → data block) |
| `test_s3_get_range_zero_length` | Zero-length range returns empty `Bytes` |
| `test_s3_get_range_zero_length_nonexistent` | Zero-length range on non-existent key returns `NotFound` |
| `test_s3_get_range_offset_at_end` | Offset at exact file end with `length > 0` returns error |

### Category 3: Conditional Put (CAS)
| Test | What It Validates |
|------|-------------------|
| `test_s3_conditional_put_new_key` | `conditional_put` on new key succeeds |
| `test_s3_conditional_put_existing_key` | `conditional_put` on existing key returns `PreconditionFailed` |
| `test_s3_conditional_put_concurrent_two` | Two concurrent `conditional_put` calls — exactly one succeeds |
| `test_s3_conditional_put_then_get` | `conditional_put` followed by `get` returns written data |
| `test_s3_conditional_put_delete_retry` | `conditional_put` → `delete` → `conditional_put` succeeds again |
| `test_s3_unconditional_then_conditional` | `put` (unconditional) then `conditional_put` on same key fails |
| `test_s3_conditional_put_stress` | 10 concurrent writers — exactly 1 wins, 9 get `PreconditionFailed` |

### Category 4: List Prefix
| Test | What It Validates |
|------|-------------------|
| `test_s3_list_prefix_empty` | Empty prefix returns all keys under test scope |
| `test_s3_list_prefix_specific` | Specific prefix filters correctly |
| `test_s3_list_prefix_sorted` | Results are sorted lexicographically |
| `test_s3_list_prefix_pagination` | >1000 objects (S3 page boundary) — all objects returned, sorted |
| `test_s3_list_prefix_nonexistent` | Non-existent prefix returns empty vec |
| `test_s3_list_prefix_boundary` | `list_prefix("a")` matches `"ab"`, `"a/"`, `"abc"` — exact string-prefix semantics |
| `test_s3_list_prefix_concurrent_writes` | Concurrent list during writes — results always sorted |

### Category 5: S3 Path Convention Tests
| Test | What It Validates |
|------|-------------------|
| `test_s3_manifest_path` | `flushdb/{namespace}/manifests/00000000000000000001` key works |
| `test_s3_sstable_l0_path` | `flushdb/{namespace}/sstables/L0/{ulid}.sst` key works |
| `test_s3_sstable_l1_path` | `flushdb/{namespace}/sstables/L1/run-{ulid}/frag-0000.sst` works |
| `test_s3_slash_in_keys` | `/` in keys works correctly (S3 treats as regular character) |
| `test_s3_manifest_list_sorted` | `list_prefix("flushdb/ns/manifests/")` returns manifest files in sorted order |
| `test_s3_out_of_order_insertion_sorted_list` | Out-of-order insertion still produces lexicographic sort on list |
| `test_s3_manifest_discovery_protocol` | Write → list → find highest → conditional_put next (full CAS loop) |

### Category 6: Parity Tests (S3 vs LocalFs)
| Test | What It Validates |
|------|-------------------|
| `test_parity_put_get_round_trip` | Same behavior on both backends |
| `test_parity_get_nonexistent` | Both return `NotFound` |
| `test_parity_put_overwrite` | Both return latest value |
| `test_parity_get_range_partial` | Partial range returns same bytes |
| `test_parity_get_range_first_byte` | First byte read matches |
| `test_parity_get_range_full_object` | Full object via range matches |
| `test_parity_get_range_nonexistent` | Both return `NotFound` |
| `test_parity_get_range_zero_length` | Both return empty `Bytes` |
| `test_parity_get_range_zero_length_missing` | Both return `NotFound` |
| `test_parity_conditional_put_new` | Both succeed on new key |
| `test_parity_conditional_put_existing` | Both return `PreconditionFailed` |
| `test_parity_conditional_put_after_delete` | Both succeed after delete |
| `test_parity_delete_idempotent` | Both succeed on double delete |
| `test_parity_delete_then_get` | Both return `NotFound` after delete |
| `test_parity_list_prefix_sorted` | Both return sorted results |
| `test_parity_list_prefix_empty` | Both return empty vec for missing prefix |
| `test_parity_binary_data_round_trip` | All 256 byte values round-trip on both |
| `test_parity_nested_path_keys` | Deeply nested path keys work on both |

### Category 7: Error Handling and Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_s3_long_key_names` | Keys near S3's 1024-byte limit work |
| `test_s3_binary_data_values` | Non-UTF-8 binary data round-trips |
| `test_s3_empty_prefix_listing_empty_backend` | List on fresh backend returns empty |
| `test_s3_concurrent_read_write_same_key` | Concurrent reads and writes don't corrupt |
| `test_s3_zero_byte_value` | `Bytes::new()` value round-trips |
| `test_s3_special_url_chars_in_keys` | `+`, `%`, `@`, `=`, spaces, `#`, `?` in keys |
| `test_s3_overwrite_then_range_read` | Overwrite then range read sees new data |
| `test_s3_range_read_at_end_boundary` | Range read at exact end boundary |
| `test_s3_hash_prefix_distribution` | Objects distribute across 128 prefix shards |

---

## Done When

- [ ] MinIO container starts via testcontainers with OnceLock sharing
- [ ] Container cleanup handles stale containers from crashed runs
- [ ] Each test uses UUID-based prefix isolation
- [ ] All 7 TESTING.md categories pass against MinIO
- [ ] All 18 parity tests confirm S3 and LocalFs behavioral equivalence
- [ ] No tests use `#[ignore]` or conditional skips
- [ ] Concurrent S3 operations batched in groups of 50
- [ ] `cargo test -p flushdb-test --features s3 s3` passes with zero failures
