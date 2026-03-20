# Phase 7: gRPC Server Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Transform the embedded KV engine into a network-accessible, multi-tenant gRPC service with S3 backend, namespace isolation, partitioning, observability, and graceful lifecycle.

**Architecture:** The server layer (`flushdb-server`) wraps `Engine<B>` instances in `Partition` structs, groups partitions per namespace via `NamespaceManager`, and exposes four gRPC RPCs (PutItems, GetItems, DeleteItems, ScanItems) through tonic. An S3 `StorageBackend` replaces the local filesystem for production. DashMap provides concurrent namespace access without global mutexes.

**Tech Stack:** Rust, tonic 0.14, aws-sdk-s3, dashmap, metrics + metrics-exporter-prometheus, tracing-subscriber, testcontainers (MinIO)

**Spec documents:** `docs/phases/grpc-server/01-s3-storage-backend.md` through `13-e2e-integration-tests.md`

---

## Codebase Notes (READ FIRST)

These notes correct spec-vs-codebase mismatches. Follow these over the spec docs where they conflict.

### N1. `FlushError::InvalidArgument` has a single `message` field

The spec docs reference `FlushError::InvalidArgument { field: "...", reason: "..." }` but the actual variant is:
```rust
InvalidArgument { message: String }
```
Format validation errors as: `FlushError::InvalidArgument { message: format!("{}: {}", field, reason) }`.

### N2. No `FlushError::StorageError` variant exists

The spec's S3 error mapping mentions `StorageError`. It doesn't exist. For S3 SDK errors that aren't `NoSuchKey`/412/`InvalidRange`, wrap them as `FlushError::Io(std::io::Error::new(std::io::ErrorKind::Other, err.to_string()))`.

### N3. `StorageBackend` trait uses `#[async_trait]`

The `StorageBackend` trait in `flushdb-types/src/storage_backend.rs` is marked `#[async_trait]`. S3StorageBackend's impl block must also use `#[async_trait]`.

### N4. `SstConfig` import path

`SstConfig` is NOT re-exported from the top-level `flushdb_engine` crate. Import it as:
```rust
use flushdb_engine::sstable::SstConfig;
```
Or better: add `pub use sstable::SstConfig;` to `flushdb-engine/src/lib.rs`.

### N5. Engine read methods take `&self`, not `&mut self`

`Engine::get()`, `Engine::scan()`, `Engine::multi_get()` all take `&self`. Partition read delegation methods should also take `&self` (not `&mut self` as the spec shows). This is important for allowing concurrent reads.

### N6. `PartitionRouter` should NOT use `#[async_trait]`

The spec shows `#[async_trait]` on the `PartitionRouter` trait, but the `route()` method is synchronous. Do not apply `#[async_trait]` — just use a regular `trait PartitionRouter: Send + Sync`.

### N7. Proto `IdempotencyToken.token` is `bytes` — must validate length

The proto `token` field is arbitrary-length `bytes`. When converting to internal `IdempotencyToken::from_parts(gen_time, [u8; 16])`, validate that `token.len() == 16`. If not, return `FlushError::InvalidArgument { message: "idempotency token must be exactly 16 bytes" }`. If both `generation_time == 0` and `token` is empty, return `IdempotencyToken::none()`.

### N8. `PageToken` page_token conversion

The proto `Selection.page_token` field is `bytes`. The engine's `PageToken::from_base64()` takes `&str`. Convert proto bytes to UTF-8 string first: `std::str::from_utf8(&page_token_bytes)?`. Alternatively, use `PageToken::decode(&page_token_bytes)` directly if the engine exposes byte-level decode.

### N9. Import reference table

| Type | Import Path |
|------|------------|
| `SstConfig` | `flushdb_engine::sstable::SstConfig` |
| `FlushResult_` | `flushdb_engine::FlushResult_` |
| `Level` | `flushdb_engine::Level` |
| `CacheStats` | `flushdb_engine::CacheStats` |
| `WriteStallStatus` | `flushdb_engine::WriteStallStatus` |
| `CompactionResult` | `flushdb_engine::CompactionResult` |
| `EngineConfig` | `flushdb_engine::EngineConfig` |
| `MemtableConfig` | `flushdb_engine::MemtableConfig` |
| `FlushConfig` | `flushdb_engine::FlushConfig` |
| `CompactionConfig` | `flushdb_engine::CompactionConfig` |
| `ManifestConfig` | `flushdb_engine::ManifestConfig` |
| `CacheConfig` | `flushdb_engine::CacheConfig` |
| `ManifestId` | `flushdb_engine::ManifestId` |
| `GetResult` | `flushdb_engine::GetResult` |
| `MergeEntry` | `flushdb_engine::MergeEntry` |
| `RangeReadOptions` | `flushdb_engine::RangeReadOptions` |
| `RangeReadResult` | `flushdb_engine::RangeReadResult` |
| `PageToken` | `flushdb_engine::PageToken` |
| `Engine` | `flushdb_engine::Engine` |
| Proto types | `flushdb_proto::flushdb::v1::*` |
| Proto service trait | `flushdb_proto::flushdb::v1::flush_db_server::FlushDb` |
| Proto client | `flushdb_proto::flushdb::v1::flush_db_client::FlushDbClient` |

### N10. `EngineConfig.local_dir` is the partition's base directory

The engine creates its WAL at `config.local_dir.join("wal")`. For partitions, set `local_dir` to `{base_data_dir}/{namespace}/partition-{id:04}/` so the WAL ends up at `{base_data_dir}/{namespace}/partition-{id:04}/wal/`.

---

## Execution Strategy

Tasks are organized into parallel waves based on the dependency graph:

| Wave | Tasks | Can Parallelize |
|------|-------|-----------------|
| 1 | T3 (NamespaceConfig), T5 (VersionGenerator), T8 (Conversions), T11 (Observability) | Yes — all independent |
| 2 | T4 (PartitionRouter), T6 (PartitionInstance) | Yes — both depend only on T3 |
| 3 | T1 (S3Backend) | Independent but large; can overlap with Wave 2 |
| 4 | T7 (NamespaceManager) | Depends on T3, T4, T5, T6 |
| 5 | T9 (WriteHandlers), T10 (ReadHandlers) | Yes — both depend on T7, T8 |
| 6 | T12 (ServerBootstrap) | Depends on T7, T9, T10, T11 |
| 7 | T2 (S3 Integration Tests), T13 (E2E Tests) | Yes — both depend on T1, T12 |

**Note on T1 (S3Backend):** Moved to Wave 3 because it requires new AWS SDK dependencies and doesn't block Wave 1-2 work. T2 (S3 integration tests) and T13 (E2E tests) are deferred to Wave 7 since they need testcontainers + running server.

---

## Chunk 1: Foundation Setup + Dependencies

### Task 0: Workspace Dependency Setup

**Files:**
- Modify: `Cargo.toml` (workspace)
- Modify: `crates/flushdb-server/Cargo.toml`
- Modify: `crates/flushdb-test/Cargo.toml`

- [ ] **Step 1: Add workspace dependencies**

Add to `[workspace.dependencies]` in root `Cargo.toml`:
```toml
aws-sdk-s3 = "1"
aws-config = "1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
metrics = "0.24"
metrics-exporter-prometheus = "0.16"
# Note: check crates.io for latest compatible version; spec says 0.18 but verify API compat with metrics 0.24
parking_lot = "0.12"
dashmap = "6"
testcontainers = "0.23"
testcontainers-modules = { version = "0.11", features = ["minio"] }
fnv = "1"
futures = "0.3"
tokio-stream = "0.1"
```

- [ ] **Step 2: Update flushdb-server Cargo.toml**

```toml
[package]
name = "flushdb-server"
version = "0.1.0"
edition = "2021"

[dependencies]
flushdb-types = { path = "../flushdb-types" }
flushdb-engine = { path = "../flushdb-engine" }
flushdb-proto = { path = "../flushdb-proto" }
tonic = { workspace = true }
tracing = { workspace = true }
tokio = { workspace = true }
bytes = { workspace = true }
serde = { workspace = true }
serde_json = { workspace = true }
thiserror = { workspace = true }
async-trait = { workspace = true }
crc32fast = { workspace = true }
aws-sdk-s3 = { workspace = true }
aws-config = { workspace = true }
tracing-subscriber = { workspace = true }
metrics = { workspace = true }
metrics-exporter-prometheus = { workspace = true }
parking_lot = { workspace = true }
dashmap = { workspace = true }
fnv = { workspace = true }
futures = { workspace = true }
tokio-stream = { workspace = true }
prost = { workspace = true }

[dev-dependencies]
tempfile = { workspace = true }
rand = { workspace = true }
uuid = { workspace = true }
```

- [ ] **Step 3: Update flushdb-test Cargo.toml**

Add to `[dependencies]`:
```toml
bytes = { workspace = true }
serde_json = { workspace = true }
aws-sdk-s3 = { workspace = true }
aws-config = { workspace = true }
testcontainers = { workspace = true }
testcontainers-modules = { workspace = true }
uuid = { workspace = true }
futures = { workspace = true }
tonic = { workspace = true }
rand = { workspace = true }
```

- [ ] **Step 4: Verify workspace compiles**

Run: `cargo build --workspace`
Expected: Clean build with no errors

- [ ] **Step 5: Commit**

```
phase-7: add workspace dependencies for gRPC server phase
```

---

## Chunk 2: Wave 1 — Independent Foundation Tasks (T3, T5, T8, T11)

These four tasks have zero dependencies on each other and can be implemented by parallel subagents.

### Task 3: Namespace Configuration

**Files:**
- Create: `crates/flushdb-server/src/namespace_config.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/namespace_config_tests.rs`

**Spec:** `docs/phases/grpc-server/03-namespace-config.md`

**Context:** This defines all the configuration types for multi-tenancy. The key types are `NamespaceConfig`, `PartitionKeyStrategy`, and supporting enums. `NamespaceConfig` must produce an `EngineConfig` for the engine crate. The engine's config sub-types and their defaults:
- `MemtableConfig { size_threshold: 67_108_864, max_frozen_count: 3 }`
- `WalConfig { segment_size_target: 33_554_432, ... }` (all defaults fine)
- `FlushConfig { sst_config: SstConfig { bloom_bits_per_key: 10, ... }, ... }`
- `CompactionConfig { ... }` (all defaults fine)
- `ManifestConfig { base_path: String, ... }`
- `CacheConfig { ... }` (all defaults fine)

Key mapping from NamespaceConfig to EngineConfig:
- `memtable_size_threshold` → `MemtableConfig.size_threshold`
- `bloom_filter_fp_rate` → `SstConfig.bloom_bits_per_key` (convert: `bits = ceil(-log2(fp_rate) / ln(2))` — or use the approximation `ceil(-1.44 * log2(fp_rate))`)
- `s3_path_prefix` → `ManifestConfig.base_path`
- `namespace.name` → `EngineConfig.namespace`

- [ ] **Step 1: Write tests** — All construction, validation, serialization, immutability, and engine config conversion tests from spec §Tests
- [ ] **Step 2: Run tests to verify they fail** — `cargo test -p flushdb-server --test namespace_config_tests`
- [ ] **Step 3: Implement types** — All enums (`PartitionKeyStrategy`, `ConsistencyScope`, `ConsistencyTarget`, `WriteConsistency`, `StorageLayerType`), structs (`StorageLayerConfig`, `StorageLayer`, `NamespaceConfig`), validation, `engine_config()`, `can_update_from()`
- [ ] **Step 4: Add `pub mod namespace_config;` to lib.rs and re-export public types**
- [ ] **Step 5: Run tests to verify they pass** — `cargo test -p flushdb-server --test namespace_config_tests`
- [ ] **Step 6: Commit** — `phase-7: implement namespace configuration types`

### Task 5: OrderedKey Generator

**Files:**
- Create: `crates/flushdb-server/src/version_generator.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/version_generator_tests.rs`

**Spec:** `docs/phases/grpc-server/05-ordered-key-generator.md`

**Context:** Uses `OrderedKey` from flushdb-types. The `OrderedKey::new(timestamp_ms, node_id, sequence)` constructor exists. Uses `AtomicU16`/`AtomicU64` for lock-free generation. `Ordering::AcqRel` for timestamp, `Ordering::Relaxed` for sequence.

- [ ] **Step 1: Write tests** — Monotonicity, node ID, concurrency, overflow, edge case tests from spec
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement `VersionGenerator`** — `new()`, `with_node_id_from_env()`, `next_version()`, `node_id()`. Algorithm: compare-and-swap loop on timestamp, atomic increment on sequence, spin-wait on overflow
- [ ] **Step 4: Add to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test version_generator_tests`
- [ ] **Step 6: Commit** — `phase-7: implement OrderedKey version generator`

### Task 8: Proto-Engine Conversion Layer

**Files:**
- Create: `crates/flushdb-server/src/conversions.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/conversion_tests.rs`

**Spec:** `docs/phases/grpc-server/08-proto-engine-conversions.md`

**Context:** The proto types live at `flushdb_proto::flushdb::v1::*`. Engine types: `GetResult` (key: CompositeKey, value: Bytes, metadata: Bytes, sequence_number: u64), `MergeEntry` (composite_key, value, metadata, entry_type, sequence_number), `RangeReadResult` (entries: Vec<MergeEntry>, next_page_token: Option<PageToken>, ...), `RangeReadOptions` (page_size_bytes: usize, item_limit: Option<usize>, resume_from: Option<PageToken>), `PageToken` (has `to_base64()` / `from_base64()`).

Key constants: `MAX_RECORD_ID_LEN = 256`, `MAX_ITEM_KEY_LEN = 4096`.

Proto `IdempotencyToken`: `generation_time: u64, token: bytes`. Internal: `IdempotencyToken::from_parts(generation_time, token_16bytes)`.

Error: `FlushError` variants map to gRPC `Status` codes per the spec mapping table.

Define `ParsedPredicate` enum with `MatchKeys`, `MatchRange`, `MatchAll` variants.

- [ ] **Step 1: Write tests** — All conversion, predicate, selection, result formatting, error mapping, validation tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement conversions** — `proto_to_idempotency_token`, `idempotency_token_to_proto`, `ordered_key_to_proto`, `proto_to_ordered_key`, `parse_predicate`, `parse_selection`, `get_result_to_proto_item`, `merge_entry_to_proto_item`, `format_get_response`, `format_scan_response`, `flush_error_to_status`, `validate_namespace`, `validate_record_id`, `validate_items`
- [ ] **Step 4: Add to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test conversion_tests`
- [ ] **Step 6: Commit** — `phase-7: implement proto-engine conversion layer`

### Task 11: Observability (Metrics + Tracing)

**Files:**
- Create: `crates/flushdb-server/src/observability.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/observability_tests.rs`

**Spec:** `docs/phases/grpc-server/11-observability.md`

**Context:** Uses `metrics` crate macros (`counter!`, `histogram!`, `gauge!`) and `metrics-exporter-prometheus`. Uses `tracing-subscriber` with `EnvFilter`. The `CacheStats` struct from engine has fields: `hits`, `misses`, `insertions`, `evictions`, `weighted_size_bytes`, `entry_count`. The `Level` enum from manifest has `as_str()` returning "L0"/"L1"/"L2"/"L3".

Note: Verify the `metrics-exporter-prometheus` API for the version you install. Typical pattern: `PrometheusBuilder::new().with_http_listener(addr).install()` which returns a `Result`. The handle is the PrometheusHandle for rendering. Check crates.io for API compatibility with `metrics` 0.24.

- [ ] **Step 1: Write tests** — Tracing init, metrics setup, recording helpers, namespace independence tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement** — `init_tracing()`, `init_metrics()`, `MetricsHandle`, all `record_*` helpers, `update_cache_stats`, `update_level_stats`, `spawn_stats_collector`
- [ ] **Step 4: Add to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test observability_tests`
- [ ] **Step 6: Commit** — `phase-7: implement observability metrics and tracing`

---

## Chunk 3: Wave 2 — Routing + Partition (T4, T6)

### Task 4: Partition Key Router

**Files:**
- Create: `crates/flushdb-server/src/partition_router.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/partition_router_tests.rs`

**Spec:** `docs/phases/grpc-server/04-partition-key-router.md`

**Depends on:** Task 3 (NamespaceConfig, PartitionKeyStrategy)

**Context:** `PartitionKeyStrategy` variants from T3: `Simple`, `Composite { delimiter, field_indices }`, `Prefix { length }`, `CustomHash { hash_name }`. Use `crc32fast::hash` for the default partition hash. For `CustomHash`, support `"fnv"` (via `fnv` crate's `FnvHasher`) and `"crc32"`.

The `route()` method is synchronous: `fn route(&self, record_id: &str) -> FlushResult<u32>`. Do NOT apply `#[async_trait]` even though the spec shows it — see Note N6. Use a plain `trait PartitionRouter: Send + Sync`.

- [ ] **Step 1: Write tests** — Simple, Composite, Prefix, CustomHash, boundary, trait object tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement** — `PartitionRouter` trait, `LocalPartitionRouter` struct with `new()` and `route()`, `partition_hash()` helper, all 4 strategy implementations
- [ ] **Step 4: Add to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test partition_router_tests`
- [ ] **Step 6: Commit** — `phase-7: implement partition key router with 4 strategies`

### Task 6: Partition Instance & Lifecycle

**Files:**
- Create: `crates/flushdb-server/src/partition.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/partition_tests.rs`

**Spec:** `docs/phases/grpc-server/06-partition-instance.md`

**Depends on:** Task 3 (NamespaceConfig)

**Context:** `Engine<B>` takes `&mut self` for write methods (`put`, `delete`, `delete_range`, `maybe_flush`, `maybe_compact`, `close`) and `&self` for reads (`get`, `scan`, `multi_get`). See Note N5 — Partition read delegation should take `&self`, not `&mut self` as the spec shows. `Engine::open(backend, EngineConfig)` creates the engine. The `Partition` wraps an `Engine<B>` — it does NOT separately own WAL/memtable. See Note N10 for `local_dir` convention.

`PartitionState` state machine: `Starting → Active → Frozen → Draining → Stopped`. `stop()` works from any state.

For tests, use `LocalFsBackend` with `tempdir`. The `NamespaceConfig::engine_config()` from T3 produces the `EngineConfig`. Use `EngineConfig` with `local_dir` set to the partition's WAL directory parent.

Engine methods that need delegation:
- `put(&mut self, record_id, item_key, value, metadata, idempotency_token) -> FlushResult<u64>`
- `delete(&mut self, record_id, item_key) -> FlushResult<u64>`
- `delete_range(&mut self, record_id, start_key, end_key) -> FlushResult<u64>`
- `get(&self, record_id, item_key) -> FlushResult<Option<GetResult>>`
- `scan(&self, record_id, start_key, end_key, options) -> FlushResult<RangeReadResult>`
- `multi_get(&self, record_id, keys) -> FlushResult<Vec<Option<GetResult>>>`
- `maybe_flush(&mut self) -> FlushResult<Option<FlushResult_>>`
- `maybe_compact(&mut self) -> FlushResult<Vec<CompactionResult>>`
- `close(&mut self) -> FlushResult<()>`

Note: Engine.put takes `Option<IdempotencyToken>` not `IdempotencyToken` directly. The Partition can forward `Some(token)`.

- [ ] **Step 1: Write tests** — Lifecycle, write state guard, read state guard, WAL directory, integration tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement** — `PartitionState` enum, `Partition<B>` struct, `open()`, lifecycle methods (`freeze`, `drain`, `stop`), engine delegation methods, status methods
- [ ] **Step 4: Add to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test partition_tests`
- [ ] **Step 6: Commit** — `phase-7: implement partition instance with lifecycle state machine`

---

## Chunk 4: Wave 3 — S3 Backend (T1)

### Task 1: S3 StorageBackend

**Files:**
- Create: `crates/flushdb-server/src/s3_backend.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/s3_backend_tests.rs`

**Spec:** `docs/phases/grpc-server/01-s3-storage-backend.md`

**Context:** Implements `StorageBackend` trait from flushdb-types. The trait uses `#[async_trait]` — the impl block must also be annotated with `#[async_trait]` (see Note N3). Trait methods: `put`, `get`, `get_range`, `delete`, `conditional_put`, `list_prefix` — all async returning `FlushResult<T>`. For error mapping, see Notes N1 and N2. `S3StorageBackend` must derive `Clone` (the `aws_sdk_s3::Client` is internally Arc'd). `get_range` with `length == 0` must return empty `Bytes` without issuing an S3 request.

AWS SDK patterns:
- `aws_sdk_s3::Client` from `aws_config::load_defaults(BehaviorVersion::latest()).await`
- `PutObject`: `.bucket().key().body(ByteStream::from(bytes))`
- `GetObject`: `.bucket().key().send()` → `.body.collect().await?.into_bytes()`
- `GetObject` with range: `.range(format!("bytes={}-{}", offset, offset+length-1))`
- `DeleteObject`: `.bucket().key().send()` — always succeeds (204 for missing)
- `PutObject` conditional: `.if_none_match("*")` — 412 on conflict
- `ListObjectsV2`: `.bucket().prefix().continuation_token()` — paginate with `next_continuation_token`

Error mapping: AWS SDK errors arrive as `SdkError<E>`. Check service error variants (`NoSuchKey`, etc.) and HTTP status codes (412 for conditional put).

For `list_prefix` fan-out: spawn concurrent list tasks across all 128 shard prefixes, bounded by `futures::stream::buffer_unordered(50)`.

Unit tests (no S3 needed) cover shard key generation and validation only. Integration tests are in Task 2.

- [ ] **Step 1: Write unit tests** — `test_shard_key_deterministic`, `test_shard_key_distribution`, `test_shard_key_format`, `test_prefix_count_validation`, `test_empty_bucket_rejected`, `test_new_default_prefix_count`, `test_with_custom_prefix_count`
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement `S3StorageBackend`** — struct, constructors (`new`, `with_prefix_count`, `from_env`), `shard_key()`, `StorageBackend` trait impl with all 6 methods, multipart upload for >16MB, `list_prefix` with shard fan-out
- [ ] **Step 4: Add to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test s3_backend_tests`
- [ ] **Step 6: Commit** — `phase-7: implement S3 storage backend with hash-prefix sharding`

---

## Chunk 5: Wave 4 — Namespace Manager (T7)

### Task 7: Namespace Manager

**Files:**
- Create: `crates/flushdb-server/src/namespace_manager.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/namespace_manager_tests.rs`

**Spec:** `docs/phases/grpc-server/07-namespace-manager.md`

**Depends on:** T3 (NamespaceConfig), T4 (PartitionRouter), T5 (VersionGenerator), T6 (Partition)

**Context:** `DashMap<String, NamespaceState<B>>` for concurrent namespace access. `DashMap::get()` returns `Ref<K,V>` (read lock), `DashMap::get_mut()` returns `RefMut<K,V>` (write lock on that shard). Partition write methods need `&mut self`, so dispatch uses `get_mut()`.

`StorageBackend` must be `Clone` — `LocalFsBackend` implements `Clone` (wraps PathBuf). S3StorageBackend will implement `Clone` (wraps `aws_sdk_s3::Client` which is Arc'd internally).

For tests, use `LocalFsBackend` with `tempdir`. Create `NamespaceConfig` via `NamespaceConfig::new()`, open partitions via `Partition::open()`.

The `run_maintenance()` method iterates all partitions calling `maybe_flush()` and `maybe_compact()`. Use `DashMap::iter_mut()` to get mutable access to each namespace.

- [ ] **Step 1: Write tests** — CRUD, routing, namespace isolation, multi-partition, lifecycle, status tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement** — `NamespaceState<B>`, `NamespaceManager<B>`, `new()`, CRUD methods (`create_namespace`, `get_namespace_config`, `update_namespace_config`, `delete_namespace`, `list_namespaces`, `namespace_exists`), dispatch methods (`put`, `delete`, `delete_range`, `get`, `scan`, `multi_get`), maintenance methods (`run_maintenance`, `flush_all`, `stop_all`), `next_version()`, status methods
- [ ] **Step 4: Add to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test namespace_manager_tests`
- [ ] **Step 6: Commit** — `phase-7: implement namespace manager with DashMap-based registry`

---

## Chunk 6: Wave 5 — gRPC Handlers (T9, T10)

### Task 9: gRPC Write Handlers (PutItems + DeleteItems)

**Files:**
- Create: `crates/flushdb-server/src/handlers/mod.rs`
- Create: `crates/flushdb-server/src/handlers/write.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Create: `crates/flushdb-server/tests/write_handler_tests.rs`

**Spec:** `docs/phases/grpc-server/09-grpc-write-handlers.md`

**Depends on:** T5 (VersionGenerator), T7 (NamespaceManager), T8 (Conversions)

**Context:** Implements `flushdb_proto::flushdb::v1::flush_db_server::FlushDb` trait (tonic-generated). The `FlushDbService<B>` struct holds `Arc<NamespaceManager<B>>` and `Arc<VersionGenerator>`.

The trait has 4 methods; this task implements `put_items` and `delete_items`. The other 2 (`get_items`, `scan_items`) are implemented in T10. Since Rust traits require all methods implemented, stub `get_items` and `scan_items` with `Status::unimplemented()` here, then replace in T10. **Important:** You must also define the `ScanItemsStream` type alias in T9 (required by the trait even for the stub):
```rust
type ScanItemsStream = Pin<Box<dyn Stream<Item = Result<ScanItemsResponse, Status>> + Send>>;
```

Engine `put()` takes `Option<IdempotencyToken>` — pass `Some(token)` when token is not none, `None` when it is.

On `FlushError::DuplicateToken`, return success (idempotent retry), not error.

For `DeleteItems`:
- `MatchAll` → engine `delete(record_id, &[])` — empty item_key means record-level tombstone
- `MatchRange` → engine `delete_range(record_id, start, end)`
- `MatchKeys` → for each key, engine `delete(record_id, key)`

For tests: create a real `NamespaceManager` with `LocalFsBackend` + `tempdir`, create a namespace, then call handler methods directly (no need for tonic transport in unit tests). Use `tonic::Request::new(proto_request)` to wrap.

- [ ] **Step 1: Write tests** — PutItems, DeleteItems, idempotency, error handling tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement** — `FlushDbService<B>` struct, `FlushDb` trait impl with `put_items` and `delete_items`, tracing spans. Stub `get_items`/`scan_items` with `Status::unimplemented()`
- [ ] **Step 4: Add `pub mod handlers;` to lib.rs**
- [ ] **Step 5: Run tests** — `cargo test -p flushdb-server --test write_handler_tests`
- [ ] **Step 6: Commit** — `phase-7: implement gRPC write handlers (PutItems + DeleteItems)`

### Task 10: gRPC Read Handlers (GetItems + ScanItems)

**Files:**
- Modify: `crates/flushdb-server/src/handlers/write.rs` (replace stubs) OR create `crates/flushdb-server/src/handlers/read.rs` and move service struct
- Create: `crates/flushdb-server/tests/read_handler_tests.rs`

**Spec:** `docs/phases/grpc-server/10-grpc-read-handlers.md`

**Depends on:** T7 (NamespaceManager), T8 (Conversions)

**Context:** `GetItems` maps predicates to engine calls:
- `MatchKeys` → `multi_get(record_id, keys)` — returns `Vec<Option<GetResult>>`
- `MatchRange` → `scan(record_id, Some(start), Some(end), options)` — returns `RangeReadResult`
- `MatchAll` → `scan(record_id, None, None, options)` — returns `RangeReadResult`

`ScanItems` is server-streaming. Return type: `Pin<Box<dyn Stream<Item = Result<ScanItemsResponse, Status>> + Send>>`. Build via `tokio_stream::wrappers::ReceiverStream` or construct manually with `async_stream` pattern using channels.

SLO-aware pagination: check elapsed time against `namespace_config.target_latency_slo_ms`. If >= 80%, return partial results with page token.

`format_get_response` and `format_scan_response` from T8 handle the result conversion.

For `ScanItems`, the server loops internally: scan with page token → yield batch → scan with next token → yield batch → until done or client drops.

**Important implementation detail for the `FlushDb` trait:** The trait requires a type alias `type ScanItemsStream`. Define it as:
```rust
type ScanItemsStream = Pin<Box<dyn Stream<Item = Result<ScanItemsResponse, Status>> + Send>>;
```

- [ ] **Step 1: Write tests** — MatchKeys, MatchRange, MatchAll, pagination, SLO, exclude_values, ScanItems, error handling tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement** — `get_items` and `scan_items` methods on `FlushDbService`, `check_slo_budget()`, `check_grpc_deadline()`, remove stubs from T9
- [ ] **Step 4: Run tests** — `cargo test -p flushdb-server --test read_handler_tests`
- [ ] **Step 5: Run all handler tests** — `cargo test -p flushdb-server --test write_handler_tests --test read_handler_tests`
- [ ] **Step 6: Commit** — `phase-7: implement gRPC read handlers (GetItems + ScanItems)`

---

## Chunk 7: Wave 6 — Server Bootstrap (T12)

### Task 12: Server Bootstrap & Graceful Shutdown

**Files:**
- Create: `crates/flushdb-server/src/config.rs`
- Create: `crates/flushdb-server/src/server.rs`
- Create: `crates/flushdb-server/src/main.rs`
- Modify: `crates/flushdb-server/src/lib.rs`
- Modify: `crates/flushdb-server/Cargo.toml` (add `[[bin]]` section if needed)
- Create: `crates/flushdb-server/tests/server_tests.rs`

**Spec:** `docs/phases/grpc-server/12-server-bootstrap.md`

**Depends on:** T7 (NamespaceManager), T9 (WriteHandlers), T10 (ReadHandlers), T11 (Observability)

**Context:** Wire everything together. `ServerConfig` loaded from env vars with `FLUSHDB_` prefix. Backend selection: if `s3_bucket` is Some, use `S3StorageBackend`, else `LocalFsBackend`.

For the `FlushDbServer`, the type must be generic over `B: StorageBackend`. But `main()` needs to choose at runtime. Use an enum wrapper:
```rust
enum AnyBackend { Local(LocalFsBackend), S3(S3StorageBackend) }
impl StorageBackend for AnyBackend { /* delegate */ }
```
OR use dynamic dispatch with `Box<dyn StorageBackend>`. The simpler approach is the enum wrapper since the set is known.

Actually, looking at the engine, `Engine<B: StorageBackend + Clone + 'static>` — the backend must be `Clone`. Both `LocalFsBackend` and `S3StorageBackend` are `Clone`. The enum wrapper also needs `Clone`.

Signal handling: `tokio::signal::ctrl_c()` for SIGINT, `tokio::signal::unix::signal(SignalKind::terminate())` for SIGTERM.

Maintenance loop: `tokio::spawn` a task that calls `namespace_manager.run_maintenance()` every `maintenance_interval_ms`, stopping when `shutdown_rx` changes.

tonic graceful shutdown: `Server::builder().add_service(svc).serve_with_shutdown(addr, signal)`.

- [ ] **Step 1: Write tests** — Config defaults, env loading, server start/shutdown, maintenance loop, backend selection tests
- [ ] **Step 2: Run tests to verify they fail**
- [ ] **Step 3: Implement `ServerConfig`** — struct with serde defaults, `from_env()`, `from_file()`
- [ ] **Step 4: Implement `FlushDbServer`** — `new()`, `start()`, `shutdown()`, signal handling, maintenance loop, stats collector
- [ ] **Step 5: Implement `main.rs`**
- [ ] **Step 6: Add to lib.rs, update Cargo.toml with `[[bin]]` if needed**
- [ ] **Step 7: Run tests** — `cargo test -p flushdb-server --test server_tests`
- [ ] **Step 8: Verify server starts** — `cargo run -p flushdb-server` (CTRL-C to stop)
- [ ] **Step 9: Commit** — `phase-7: implement server bootstrap with graceful shutdown`

---

## Chunk 8: Wave 7 — Integration Tests (T2, T13)

### Task 2: S3 Integration Tests (Testcontainers/MinIO)

**Files:**
- Create: `crates/flushdb-test/src/s3_test_utils.rs`
- Create: `crates/flushdb-test/tests/s3_backend_tests.rs`
- Modify: `crates/flushdb-test/src/lib.rs`

**Spec:** `docs/phases/grpc-server/02-s3-integration-tests.md`

**Depends on:** T1 (S3StorageBackend)

**Context:** Uses `testcontainers` crate with MinIO. Container setup:
```rust
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::minio::MinIO;
let container = MinIO::default().start().await?;
let port = container.get_host_port_ipv4(9000).await?;
```

S3 client configured for MinIO: endpoint override to `http://localhost:{port}`, path-style access, region `us-east-1`, credentials `minioadmin`/`minioadmin`.

Test isolation: each test gets a UUID-prefixed key namespace. Cleanup removes all objects under that prefix.

All 7 test categories from TESTING.md. Parity tests run same operation on both `S3StorageBackend` (against MinIO) and `LocalFsBackend` (against tempdir), assert identical results.

Use feature gate `#[cfg(feature = "s3")]` if S3 tests should be opt-in, or run unconditionally if Docker is assumed available.

- [ ] **Step 1: Implement test utilities** — MinIO container setup with `OnceLock`, `create_test_s3_backend()`, `cleanup_test_prefix()`
- [ ] **Step 2: Write Category 1-3 tests** — Basic CRUD, byte-range reads, conditional put
- [ ] **Step 3: Run and verify** — `cargo test -p flushdb-test --test s3_backend_tests`
- [ ] **Step 4: Write Category 4-5 tests** — List prefix, S3 path conventions
- [ ] **Step 5: Write Category 6 tests** — Parity tests (S3 vs LocalFs)
- [ ] **Step 6: Write Category 7 tests** — Error handling and edge cases
- [ ] **Step 7: Run full suite** — `cargo test -p flushdb-test --test s3_backend_tests`
- [ ] **Step 8: Commit** — `phase-7: add S3 integration tests with MinIO testcontainers`

### Task 13: End-to-End Integration Tests

**Files:**
- Create: `crates/flushdb-test/tests/e2e_tests.rs`
- Modify: `crates/flushdb-test/src/lib.rs` (add test server harness)

**Spec:** `docs/phases/grpc-server/13-e2e-integration-tests.md`

**Depends on:** T1 (S3StorageBackend), T12 (ServerBootstrap)

**Context:** The `TestServer` harness starts a full gRPC server in-process on a random port, connects a tonic client. Uses `tokio::net::TcpListener::bind("127.0.0.1:0")` to get a random available port.

For the gRPC client: `flushdb_proto::flushdb::v1::flush_db_client::FlushDbClient::connect(format!("http://127.0.0.1:{}", port))`.

Test helpers construct proto request messages.

S3 E2E tests use the same MinIO container from T2.

LocalFs E2E tests use `tempdir` — no Docker required.

- [ ] **Step 1: Implement `TestServer` harness** — `start()`, `start_with_namespaces()`, `client()`, `shutdown()`
- [ ] **Step 2: Implement test helpers** — `make_put_request`, `make_get_request`, `make_delete_request`, `make_scan_request`, `collect_scan_stream`
- [ ] **Step 3: Write CRUD + namespace isolation tests**
- [ ] **Step 4: Write partition routing + idempotency tests**
- [ ] **Step 5: Write pagination + ScanItems streaming tests**
- [ ] **Step 6: Write version, SLO, error handling, graceful shutdown tests**
- [ ] **Step 7: Write S3 backend E2E tests** (if Docker available)
- [ ] **Step 8: Run full suite** — `cargo test -p flushdb-test --test e2e_tests`
- [ ] **Step 9: Commit** — `phase-7: add end-to-end integration tests`

---

## Chunk 9: Final Validation

### Task 14: Phase-Level Validation

- [ ] **Step 1: Run full workspace build** — `cargo build --workspace` (zero warnings)
- [ ] **Step 2: Run full test suite** — `cargo test --workspace` (all pass)
- [ ] **Step 3: Run clippy** — `cargo clippy --workspace` (no warnings)
- [ ] **Step 4: Verify acceptance criteria** — Check each item from `docs/phases/grpc-server/README.md` §Acceptance Criteria
- [ ] **Step 5: Final commit if any fixes needed**

---

## File Structure Summary

```
crates/flushdb-server/
├── Cargo.toml                          (updated: new deps)
├── src/
│   ├── lib.rs                          (module declarations + re-exports)
│   ├── main.rs                         (T12: server entry point)
│   ├── config.rs                       (T12: ServerConfig)
│   ├── server.rs                       (T12: FlushDbServer lifecycle)
│   ├── namespace_config.rs             (T3: NamespaceConfig + strategy types)
│   ├── partition_router.rs             (T4: PartitionRouter trait + LocalPartitionRouter)
│   ├── version_generator.rs            (T5: VersionGenerator)
│   ├── partition.rs                    (T6: Partition + PartitionState)
│   ├── namespace_manager.rs            (T7: NamespaceManager + NamespaceState)
│   ├── conversions.rs                  (T8: proto ↔ engine conversions)
│   ├── s3_backend.rs                   (T1: S3StorageBackend)
│   ├── observability.rs                (T11: metrics + tracing)
│   └── handlers/
│       ├── mod.rs                      (T9: FlushDbService struct + mod declarations)
│       ├── write.rs                    (T9: put_items + delete_items)
│       └── read.rs                     (T10: get_items + scan_items)
└── tests/
    ├── namespace_config_tests.rs       (T3)
    ├── partition_router_tests.rs       (T4)
    ├── version_generator_tests.rs      (T5)
    ├── partition_tests.rs              (T6)
    ├── namespace_manager_tests.rs      (T7)
    ├── conversion_tests.rs             (T8)
    ├── s3_backend_tests.rs             (T1)
    ├── observability_tests.rs          (T11)
    ├── write_handler_tests.rs          (T9)
    ├── read_handler_tests.rs           (T10)
    └── server_tests.rs                 (T12)

crates/flushdb-test/
├── Cargo.toml                          (updated: new deps)
├── src/
│   ├── lib.rs                          (test utilities)
│   └── s3_test_utils.rs               (T2: MinIO container + helpers)
└── tests/
    ├── s3_backend_tests.rs             (T2)
    └── e2e_tests.rs                    (T13)
```
