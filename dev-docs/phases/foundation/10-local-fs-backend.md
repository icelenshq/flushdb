# Task 10: LocalFsBackend — Filesystem Implementation

**Crate:** `flushdb-types`
**File:** `src/local_fs_backend.rs`
**Depends on:** Task 1 (workspace), Task 2 (FlushError), Task 9 (StorageBackend trait)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §23, TESTING.md §6 (Parity Tests), Phase 1 §3 (LocalFsBackend)

---

## Goal

Implement `StorageBackend` backed by the local filesystem. This is the primary backend for all unit and integration tests throughout the project. Every storage test will use this with `tempdir`.

---

## What to Build

### 10.1 LocalFsBackend Struct

```
LocalFsBackend {
  base_dir: PathBuf   // root directory for all objects
}
```

**Construction:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(base_dir: impl Into<PathBuf>) -> Self` | Creates backend rooted at the given directory. Does NOT create the directory — caller is responsible. |

### 10.2 Method Implementations

#### `put(key, value)`
1. Compute full path: `{base_dir}/{key}`
2. Create intermediate directories if they don't exist (`tokio::fs::create_dir_all` on parent)
3. Write the value to the file atomically:
   - Write to a temp file in the same directory (to ensure same filesystem for rename)
   - Rename temp file to final path
   - This prevents partial writes from being visible on crash
4. If key contains `/`, treat each segment as a directory component

#### `get(key)`
1. Compute full path: `{base_dir}/{key}`
2. Read file contents via `tokio::fs::read`
3. If file doesn't exist, return `FlushError::NotFound`
4. Return contents as `Bytes`

#### `get_range(key, offset, length)`
1. Compute full path: `{base_dir}/{key}`
2. If file doesn't exist, return `FlushError::NotFound`
3. If `length == 0`, return empty `Bytes` (don't even check offset validity)
4. Open file, seek to `offset`
5. Read up to `length` bytes (may return fewer if near EOF)
6. If `offset >= file_size` and `length > 0`, return an error (not empty — this indicates a bug in the caller)

#### `delete(key)`
1. Compute full path: `{base_dir}/{key}`
2. Remove the file via `tokio::fs::remove_file`
3. If file doesn't exist, return `Ok(())` — **idempotent**
4. Do NOT clean up empty parent directories (not worth the complexity)

#### `conditional_put(key, value)`
1. Compute full path: `{base_dir}/{key}`
2. Create intermediate directories if needed
3. Attempt to create the file with `O_CREAT | O_EXCL` flags (atomic create-if-not-exists)
4. If file already exists → return `FlushError::PreconditionFailed`
5. If created successfully → write value and return `Ok(())`

**Atomicity note:** On Unix, `O_CREAT | O_EXCL` is atomic — exactly one of two concurrent callers will succeed. This matches S3's `If-None-Match: *` semantics exactly.

#### `list_prefix(prefix)`
1. Walk the directory tree under `{base_dir}`
2. Collect all file paths (not directories)
3. Convert each path to a key by stripping the `{base_dir}/` prefix
4. Filter to keys that start with `prefix`
5. **Sort lexicographically** — this is critical
6. Return the sorted vec

**Implementation detail:** Use `tokio::fs::read_dir` recursively or `walkdir` (sync, wrapped in `spawn_blocking`). The recursive walk is needed because keys like `flushdb/ns/manifests/00001` create nested directory structures.

### 10.3 Path Handling

- Keys may contain `/` — each segment becomes a directory, last segment is the file
- Keys should not contain `..` (path traversal) — but we don't need to validate this in Phase 1 since all callers are internal
- Empty key is invalid (would resolve to `base_dir` itself)

### 10.4 Concurrency Considerations

- Multiple async tasks may access the same `LocalFsBackend` concurrently
- File operations must be safe under concurrent access:
  - `put` uses atomic write (temp file + rename)
  - `conditional_put` uses `O_CREAT | O_EXCL` (kernel-level atomicity)
  - `get` and `get_range` are read-only
  - `delete` is idempotent

---

## Tests

**File:** `crates/flushdb-types/tests/local_fs_backend_tests.rs`

All tests use `tempfile::tempdir()` for isolation.

### Basic CRUD
| Test | What It Validates |
|------|-------------------|
| `test_put_get_round_trip` | Put bytes, get returns exact bytes |
| `test_put_empty_value` | Zero-length value round-trips |
| `test_put_large_value` | 1MB value round-trips correctly |
| `test_get_not_found` | Get on non-existent key → `NotFound` |
| `test_put_overwrite` | Put same key twice, get returns latest value |
| `test_delete_existing` | Delete key, subsequent get → `NotFound` |
| `test_delete_idempotent` | Delete non-existent key → `Ok(())` |
| `test_delete_then_get` | Delete then get → `NotFound` |
| `test_put_with_slashes` | Key `a/b/c` creates nested dirs, round-trips |

### Byte-Range Reads
| Test | What It Validates |
|------|-------------------|
| `test_get_range_first_bytes` | Read first N bytes of a file |
| `test_get_range_last_bytes` | Read last N bytes (footer simulation: `offset = len - 80, length = 80`) |
| `test_get_range_middle` | Read a slice from the middle |
| `test_get_range_full` | Read entire object via range (`offset=0, length=full_size`) |
| `test_get_range_beyond_end` | Range extending past EOF returns partial content |
| `test_get_range_not_found` | Range on non-existent key → `NotFound` |
| `test_get_range_zero_length` | Zero-length range returns empty `Bytes` |
| `test_get_range_zero_length_not_found` | Zero-length range on missing key → `NotFound` |
| `test_get_range_offset_at_end` | Offset exactly at file size with length > 0 → error |
| `test_get_range_offset_beyond_end` | Offset past file size → error |
| `test_sequential_ranges` | Multiple sequential reads (SSTable open simulation) |

### Conditional Put (CAS)
| Test | What It Validates |
|------|-------------------|
| `test_conditional_put_new_key` | Succeeds on non-existent key |
| `test_conditional_put_existing_key` | Returns `PreconditionFailed` |
| `test_conditional_put_then_get` | Written data is readable |
| `test_conditional_put_after_delete` | Delete then conditional_put succeeds again |
| `test_put_then_conditional_put` | Unconditional put, then conditional_put fails |
| `test_concurrent_conditional_put` | Spawn 10 tasks — exactly 1 succeeds, 9 get `PreconditionFailed` |

### List Prefix
| Test | What It Validates |
|------|-------------------|
| `test_list_prefix_empty` | Empty backend returns empty vec |
| `test_list_prefix_all` | Empty prefix returns all keys |
| `test_list_prefix_filter` | Specific prefix filters correctly |
| `test_list_prefix_sorted` | Results are lexicographically sorted |
| `test_list_prefix_no_match` | Non-existent prefix returns empty vec |
| `test_list_prefix_boundary` | `list_prefix("a")` matches "ab", "a/x", "abc" |
| `test_list_prefix_nested` | Keys with nested paths listed correctly |
| `test_list_prefix_after_delete` | Deleted keys not listed |

### S3 Path Convention Tests
| Test | What It Validates |
|------|-------------------|
| `test_s3_manifest_path` | `flushdb/{ns}/manifests/00000000000000000001` works |
| `test_s3_sstable_path` | `flushdb/{ns}/sstables/L0/{ulid}.sst` works |
| `test_s3_nested_sstable_path` | `flushdb/{ns}/sstables/L1/run-{ulid}/frag-0000.sst` works |
| `test_manifest_discovery_protocol` | Write multiple manifests → list → find highest → conditional_put next |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_binary_data_round_trip` | All 256 byte values in value payload |
| `test_overwrite_then_range_read` | Overwrite file, range read sees new data |
| `test_deeply_nested_path` | Key with many `/` segments works |

---

## Done When

- [ ] All 6 `StorageBackend` methods implemented
- [ ] `put` uses atomic writes (temp file + rename)
- [ ] `conditional_put` uses `O_CREAT | O_EXCL` for atomicity
- [ ] `list_prefix` returns sorted results
- [ ] `delete` is idempotent
- [ ] `get_range` handles all boundary cases
- [ ] Concurrent `conditional_put` has exactly one winner
- [ ] All tests pass with `tempdir` isolation
