# Task 1: S3 StorageBackend

**Crate:** `flushdb-server`
**File:** `src/s3_backend.rs`
**Depends on:** Nothing (uses `StorageBackend` trait from flushdb-types)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §23 (S3 Object Layout), Phase 7 §7

---

## Goal

Implement the production `StorageBackend` targeting Amazon S3 via `aws-sdk-s3`. This is the source-of-truth storage layer — all durable state (SSTables, manifests) lives in S3. The implementation includes hash-based prefix sharding for throughput distribution and multipart upload for large SSTables.

---

## What to Build

### 1.1 S3StorageBackend Struct

```
S3StorageBackend {
    client:       aws_sdk_s3::Client
    bucket:       String
    prefix_count: u32          // default 128, must be power of 2
}
```

**Construction:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(client: aws_sdk_s3::Client, bucket: String) -> Self` | Creates backend with default 128 prefix shards |
| `with_prefix_count` | `(client: aws_sdk_s3::Client, bucket: String, prefix_count: u32) -> FlushResult<Self>` | Validates prefix_count is power of 2 |
| `from_env` | `(bucket: String) -> FlushResult<Self>` | Loads AWS config from environment (region, credentials) via `aws_config::load_defaults()` |

### 1.2 Hash-Based Prefix Sharding

All S3 object keys are sharded across `prefix_count` hash prefixes to distribute I/O:

```
actual_s3_key = format!("{:03}/{}", hash(logical_key) % prefix_count, logical_key)
```

- Hash function: use a fast, well-distributed hash (e.g., the first 4 bytes of a CRC32 of the logical key)
- 128 prefixes yield up to 448K PUTs/s and 704K GETs/s before S3 throttling
- The prefix is a zero-padded 3-digit number (000–127)

| Method | Signature | Behavior |
|--------|-----------|----------|
| `shard_key` | `(&self, logical_key: &str) -> String` | Computes the sharded S3 key with prefix |

### 1.3 StorageBackend Trait Implementation

| Trait Method | S3 Operation | Details |
|-------------|--------------|---------|
| `put` | `PutObject` | Standard unconditional write. For objects > 16 MB, use multipart upload (§1.4). |
| `get` | `GetObject` | Stream full body into `Bytes`. Map `NoSuchKey` → `FlushError::NotFound`. |
| `get_range` | `GetObject` with `Range` header | Set `Range: bytes={offset}-{offset+length-1}`. Zero length → return empty `Bytes` without issuing request. Map `NoSuchKey` → `NotFound`. Map `InvalidRange` → appropriate error. |
| `delete` | `DeleteObject` | Idempotent — S3 returns 204 for non-existent keys, so no error mapping needed. |
| `conditional_put` | `PutObject` with `If-None-Match: *` | Map HTTP 412 (Precondition Failed) → `FlushError::PreconditionFailed`. This is the CAS primitive for manifests. |
| `list_prefix` | `ListObjectsV2` with prefix | Paginate through all results (S3 returns max 1000 per page via `continuation_token`). Strip the shard prefix from returned keys. Results MUST be sorted lexicographically (S3 returns sorted natively, but verify after prefix stripping). |

### 1.4 Multipart Upload for Large Objects

For `put` operations where the value exceeds the multipart threshold (default 16 MB):

```
MULTIPART_THRESHOLD: usize = 16 * 1024 * 1024   // 16 MB
MULTIPART_PART_SIZE: usize = 16 * 1024 * 1024    // 16 MB per part
```

**Upload strategy:**
1. `CreateMultipartUpload` to get an `upload_id`
2. Split payload into `MULTIPART_PART_SIZE` chunks
3. Upload each part via `UploadPart`, collecting `ETag` values
4. `CompleteMultipartUpload` with all part ETags
5. On any failure, `AbortMultipartUpload` to clean up

**Double buffering:** Build part N+1 in memory while uploading part N. Peak memory: O(32 MB) regardless of SSTable size.

**Error handling:** If any part upload fails, abort the multipart upload and return the error. Do not leave orphaned multipart uploads.

### 1.5 list_prefix with Shard Fan-Out

Since keys are distributed across `prefix_count` shard prefixes, `list_prefix` must fan out:

1. For each shard prefix `i` in `0..prefix_count`:
   - Call `ListObjectsV2` with prefix `{i:03}/{logical_prefix}`
2. Strip the `{i:03}/` shard prefix from each returned key
3. Merge all results into a single sorted `Vec<String>`
4. Batch concurrent list operations to avoid fd exhaustion (max 50 concurrent)

This is unavoidable because the same logical prefix may have objects across all shards.

### 1.6 Validation Rules

| Check | Error |
|-------|-------|
| `prefix_count` is 0 | `FlushError::InvalidArgument { field: "prefix_count", reason: "must be > 0" }` |
| `prefix_count` is not power of 2 | `FlushError::InvalidArgument { field: "prefix_count", reason: "must be a power of 2" }` |
| `bucket` is empty | `FlushError::InvalidArgument { field: "bucket", reason: "must not be empty" }` |
| `get_range` with offset beyond object end and length > 0 | Map S3 `InvalidRange` error → `FlushError::InvalidArgument` |

### 1.7 Error Mapping

Map AWS SDK errors to `FlushError` variants:

| AWS Error | FlushError |
|-----------|------------|
| `NoSuchKey` | `NotFound` |
| HTTP 412 (on conditional_put) | `PreconditionFailed` |
| `InvalidRange` | `InvalidArgument` |
| Network/timeout errors | `StorageError` (or a new IO-related variant) |
| All other S3 errors | `StorageError` with context |

---

## Tests

**File:** `crates/flushdb-server/tests/s3_backend_tests.rs`

### Unit Tests (no S3 required)
| Test | What It Validates |
|------|-------------------|
| `test_shard_key_deterministic` | Same logical key always produces same shard prefix |
| `test_shard_key_distribution` | 1000 random keys distribute across all 128 shards (no shard gets 0) |
| `test_shard_key_format` | Output format is `{NNN}/{logical_key}` with zero-padded 3-digit prefix |
| `test_prefix_count_validation` | Rejects 0, rejects non-power-of-2, accepts 1/2/4/64/128/256 |
| `test_empty_bucket_rejected` | Empty bucket string → InvalidArgument |
| `test_multipart_threshold_boundary` | Values at exactly 16MB use single put, 16MB+1 uses multipart |

### Construction Tests
| Test | What It Validates |
|------|-------------------|
| `test_new_default_prefix_count` | Default constructor sets prefix_count to 128 |
| `test_with_custom_prefix_count` | Custom prefix counts (1, 64, 256) accepted |

---

## Done When

- [ ] `S3StorageBackend` implements all 6 `StorageBackend` trait methods
- [ ] Hash-based prefix sharding distributes keys across 128 prefixes
- [ ] `list_prefix` fans out across all shards and returns sorted, deduplicated results
- [ ] Multipart upload handles objects > 16 MB with abort-on-failure cleanup
- [ ] `conditional_put` maps S3 412 → `FlushError::PreconditionFailed`
- [ ] `get_range` with zero length returns empty `Bytes` without S3 call
- [ ] All AWS SDK errors are mapped to appropriate `FlushError` variants
- [ ] Unit tests pass for shard key generation and validation
