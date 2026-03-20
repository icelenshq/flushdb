# Task 9: StorageBackend Trait

**Crate:** `flushdb-types`
**File:** `src/storage_backend.rs`
**Depends on:** Task 1 (workspace), Task 2 (FlushError)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §23 (S3 Object Layout), Phase 1 §3

---

## Goal

Define the central abstraction for persistent storage. Every component — SSTable reader/writer, manifest manager, GC — builds against this trait. S3 is just one implementation; `LocalFsBackend` (Task 10) is the test implementation.

---

## What to Build

### 9.1 StorageBackend Trait

An async trait using `#[async_trait]` with six methods:

| Method | Signature | Semantics |
|--------|-----------|-----------|
| `put` | `(&self, key: &str, value: Bytes) -> FlushResult<()>` | Store bytes at key. Overwrite if exists. Creates intermediate path components (for filesystem backends). |
| `get` | `(&self, key: &str) -> FlushResult<Bytes>` | Retrieve bytes by exact key. Returns `FlushError::NotFound` if key doesn't exist. |
| `get_range` | `(&self, key: &str, offset: u64, length: u64) -> FlushResult<Bytes>` | Byte-range read starting at `offset` for `length` bytes. For S3, maps to `Range: bytes=offset-{offset+length-1}`. Returns `NotFound` if key doesn't exist. Returns empty `Bytes` if `length` is 0 (even for existing keys). If range extends beyond object, returns available bytes (partial content). If offset is at or beyond file end with length > 0, returns error. |
| `delete` | `(&self, key: &str) -> FlushResult<()>` | Remove object at key. **Idempotent** — deleting a non-existent key succeeds silently. |
| `conditional_put` | `(&self, key: &str, value: Bytes) -> FlushResult<()>` | CAS write with `If-None-Match: *` semantics — succeeds only if key does NOT exist. Returns `FlushError::PreconditionFailed` if key already exists. This is the foundation of the manifest CAS protocol. |
| `list_prefix` | `(&self, prefix: &str) -> FlushResult<Vec<String>>` | List all keys under prefix, **lexicographically sorted**. Returns empty vec if no keys match. Sorted results are critical — manifest recovery picks the highest lexicographic ID. |

### 9.2 Key Design Decisions

**Keys are strings (`&str`), not bytes:**
- S3 keys are UTF-8 strings
- Path components use `/` as logical separator (S3 treats it as a regular character)
- This matches the S3 path convention from STORAGE_DESIGN.md §23

**All methods are async:**
- S3 operations are inherently async (HTTP requests)
- LocalFs uses `tokio::fs` for async file I/O
- Uses `#[async_trait]` for async trait methods

**`conditional_put` semantics:**
- Maps to S3's `If-None-Match: *` header
- Returns `PreconditionFailed` on conflict, NOT a boolean
- This is the exact semantics needed for manifest CAS: "write this manifest version only if it doesn't exist yet"
- Race condition between two concurrent `conditional_put` calls: exactly one succeeds, the other gets `PreconditionFailed`

**`get_range` specifics:**
- Enables S3 byte-range reads for fetching individual SSTable data blocks without downloading entire files
- SSTable read pattern: `get_range(sst_path, block_offset, block_length)` — reads one 4KB block
- Footer read pattern: `get_range(sst_path, file_size - 80, 80)` — reads the last 80 bytes
- Zero-length range: returns empty `Bytes` (not an error) for existing keys
- Zero-length range on non-existent key: returns `NotFound`

**`list_prefix` sorting:**
- Results MUST be sorted lexicographically
- This is critical for manifest discovery: `list_prefix("flushdb/ns/manifests/")` → sorted list, last element is the current manifest
- S3 natively returns sorted results; filesystem implementations must sort explicitly

**`delete` is idempotent:**
- S3 DELETE on non-existent key returns 204 (success)
- Filesystem implementation should match: no error if file doesn't exist
- This simplifies GC — no need to check existence before delete

### 9.3 Object Trait Requirements

The trait should require:
- `Send + Sync` — implementations must be shareable across threads (async runtime)
- This is handled by `#[async_trait]` which adds `Send` bounds on futures

### 9.4 Trait Does NOT Include

- **No `exists` method** — use `get` and check for `NotFound`. Avoids TOCTOU races.
- **No batch operations** — batching is done at higher levels (manifest updates are single CAS operations)
- **No streaming reads** — `get_range` covers all read patterns. Full object reads use `get`.
- **No multipart upload** — this is an S3-specific optimization added in Phase 7's S3 backend, not part of the trait.

---

## Tests

No tests for the trait itself (it's abstract). All testing happens via `LocalFsBackend` in Task 10.

However, define a test helper function signature that implementations should satisfy:

```
// In the trait file, add a doc comment listing the test categories
// that every implementation must pass (from TESTING.md):
// 1. Basic CRUD (put/get round-trip, overwrite, delete idempotency)
// 2. Byte-range reads (partial, full, boundary cases)
// 3. Conditional put (CAS semantics, concurrent races)
// 4. List prefix (sorting, pagination, boundary matching)
// 5. Error handling (NotFound, edge cases)
```

---

## Done When

- [ ] Trait defined with all 6 methods
- [ ] All methods are async via `#[async_trait]`
- [ ] `conditional_put` returns `PreconditionFailed` on conflict
- [ ] `list_prefix` contract specifies sorted results
- [ ] `get_range` handles zero-length, partial content, and boundary cases
- [ ] `delete` is documented as idempotent
- [ ] Trait requires `Send + Sync`
