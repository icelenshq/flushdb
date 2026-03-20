# Task 7: Namespace Manager

**Crate:** `flushdb-server`
**File:** `src/namespace_manager.rs`
**Depends on:** Task 3 (NamespaceConfig), Task 4 (PartitionRouter), Task 5 (VersionGenerator), Task 6 (Partition)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §20 (Multi-Tenancy), Phase 7 §2, §4

---

## Goal

Build the `NamespaceManager` — the central registry that manages multiple namespaces, each with its own partition set, routing, and engine instances. This is the single entry point that the gRPC handlers call to dispatch operations to the correct partition. Each namespace is fully isolated: its own manifest, WAL, compaction lifecycle, and S3 path prefix.

---

## What to Build

### 7.1 NamespaceState Struct

```
NamespaceState<B: StorageBackend> {
    config:     NamespaceConfig,
    router:     LocalPartitionRouter,
    partitions: Vec<Partition<B>>,
}
```

Owns all partitions for a single namespace. The `partitions` vec is indexed by `partition_id`.

### 7.2 NamespaceManager Struct

```
NamespaceManager<B: StorageBackend> {
    namespaces:     DashMap<String, NamespaceState<B>>,
    backend:        B,
    base_data_dir:  PathBuf,
    version_gen:    Arc<VersionGenerator>,
}
```

**`DashMap`** provides concurrent access to the namespace registry without a global mutex. Namespace creation/deletion takes a write shard lock; request routing takes a read shard lock — no contention on the hot path.

### 7.3 Construction

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(backend: B, base_data_dir: PathBuf, version_gen: Arc<VersionGenerator>) -> Self` | Creates empty manager, ready for namespace registration |

### 7.4 Namespace CRUD

| Method | Signature | Behavior |
|--------|-----------|----------|
| `create_namespace` | `async (&self, config: NamespaceConfig) -> FlushResult<()>` | Validates config, creates all partitions (0..partition_count), registers namespace. Returns error if namespace already exists. |
| `get_namespace_config` | `(&self, namespace: &str) -> FlushResult<NamespaceConfig>` | Returns config clone. `NotFound` if missing. |
| `update_namespace_config` | `(&self, config: NamespaceConfig) -> FlushResult<()>` | Validates via `can_update_from()`, applies mutable field updates. `NotFound` if missing. |
| `delete_namespace` | `async (&self, namespace: &str) -> FlushResult<()>` | Stops all partitions, removes from registry. `NotFound` if missing. |
| `list_namespaces` | `(&self) -> Vec<String>` | Returns sorted list of namespace names. |
| `namespace_exists` | `(&self, namespace: &str) -> bool` | Check existence without error. |

### 7.5 Request Dispatch

These methods route a request to the correct partition based on the namespace's partition key strategy:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `put` | `async (&self, namespace: &str, record_id: &str, item_key: &[u8], value: Bytes, metadata: Bytes, idempotency_token: IdempotencyToken) -> FlushResult<u64>` | Routes to partition, calls `partition.put()`. Returns sequence number. |
| `delete` | `async (&self, namespace: &str, record_id: &str, item_key: &[u8]) -> FlushResult<u64>` | Routes to partition, calls `partition.delete()`. |
| `delete_range` | `async (&self, namespace: &str, record_id: &str, start_key: &[u8], end_key: &[u8]) -> FlushResult<u64>` | Routes to partition, calls `partition.delete_range()`. |
| `get` | `async (&self, namespace: &str, record_id: &str, item_key: &[u8]) -> FlushResult<Option<GetResult>>` | Routes to partition, calls `partition.get()`. |
| `scan` | `async (&self, namespace: &str, record_id: &str, start_key: Option<&[u8]>, end_key: Option<&[u8]>, options: RangeReadOptions) -> FlushResult<RangeReadResult>` | Routes to partition, calls `partition.scan()`. |
| `multi_get` | `async (&self, namespace: &str, record_id: &str, keys: &[&[u8]]) -> FlushResult<Vec<Option<GetResult>>>` | Routes to partition, calls `partition.multi_get()`. |

**Routing logic:**
1. Look up namespace in `DashMap` → `FlushError::NotFound` if missing
2. Call `router.route(record_id)` → get `partition_id`
3. Index into `partitions[partition_id]` → call the appropriate engine method

### 7.6 Background Maintenance

| Method | Signature | Behavior |
|--------|-----------|----------|
| `run_maintenance` | `async (&self) -> FlushResult<()>` | Iterates all partitions across all namespaces, calls `maybe_flush()` and `maybe_compact()` on each. |
| `flush_all` | `async (&self) -> FlushResult<()>` | Forces flush on all active partitions. Used during graceful shutdown. |
| `stop_all` | `async (&self) -> FlushResult<()>` | Stops all partitions in all namespaces. Calls freeze → drain → stop on each. |

### 7.7 Version Generation

| Method | Signature | Behavior |
|--------|-----------|----------|
| `next_version` | `(&self) -> OrderedKey` | Delegates to the shared `VersionGenerator` |

### 7.8 Status Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `namespace_count` | `(&self) -> usize` | Number of registered namespaces |
| `partition_count` | `(&self, namespace: &str) -> FlushResult<u32>` | Number of partitions for a namespace |
| `total_partition_count` | `(&self) -> usize` | Total partitions across all namespaces |

### 7.9 Design Decisions

- **`DashMap` for namespace registry** — Concurrent reads (request dispatch) don't block on concurrent writes (namespace creation). This is not `Arc<Mutex<>>` — `DashMap` uses sharded locks internally.
- **`Vec<Partition>` per namespace** — Partitions are indexed by `partition_id` (0-based). Direct indexing is O(1).
- **Single `VersionGenerator` shared across all namespaces** — Version monotonicity is global, not per-namespace. This simplifies ordering across namespaces.
- **`backend` is cloneable** — The S3StorageBackend wraps an `aws_sdk_s3::Client` which is already `Clone` (internally Arc'd). Each partition gets a clone of the backend.

### 7.10 Concurrency Considerations

- **DashMap read vs write** — `get()` (read) and `insert()` (write) on DashMap use sharded locks. Request dispatch (read path) will almost never contend with namespace creation (write path).
- **Partition mutability** — `DashMap<String, NamespaceState>` returns a `RefMut` guard when mutating partitions. The guard is held only for the duration of the engine operation. Since each partition is independent, there's no cross-partition contention.
- **Note on `&self` for mutation:** The dispatch methods take `&self` because `DashMap` allows interior mutation. The actual engine mutation happens through `DashMap::get_mut()` which provides `RefMut<NamespaceState>`.

---

## Tests

**File:** `crates/flushdb-server/tests/namespace_manager_tests.rs`

### Namespace CRUD Tests
| Test | What It Validates |
|------|-------------------|
| `test_create_namespace` | Creating a namespace succeeds, `namespace_exists` returns true |
| `test_create_duplicate_rejected` | Creating same namespace twice → error |
| `test_get_namespace_config` | Config retrievable after creation |
| `test_update_mutable_fields` | Updating memtable_size and page_size succeeds |
| `test_update_immutable_fields_rejected` | Changing partition_count → error |
| `test_delete_namespace` | Delete removes namespace, `namespace_exists` returns false |
| `test_delete_nonexistent_rejected` | Deleting missing namespace → `NotFound` |
| `test_list_namespaces_sorted` | List returns namespace names in sorted order |

### Request Routing Tests
| Test | What It Validates |
|------|-------------------|
| `test_put_routes_correctly` | `put` to namespace reaches correct partition |
| `test_get_after_put` | Data written via `put` is readable via `get` in the same namespace |
| `test_put_nonexistent_namespace` | Put to non-existent namespace → `NotFound` |
| `test_scan_routes_correctly` | `scan` returns data from correct partition |

### Namespace Isolation Tests
| Test | What It Validates |
|------|-------------------|
| `test_namespace_isolation_writes` | Write to namespace A, read from namespace B → no data visible |
| `test_namespace_isolation_deletes` | Delete in namespace A doesn't affect namespace B |
| `test_independent_manifests` | Each namespace has independent manifest versioning |

### Multi-Partition Tests
| Test | What It Validates |
|------|-------------------|
| `test_partitions_created_for_count` | Namespace with partition_count=4 creates 4 partitions |
| `test_different_records_route_to_different_partitions` | Multiple record_ids distribute across partitions |
| `test_same_record_always_same_partition` | Same record_id always routes to same partition |

### Lifecycle Tests
| Test | What It Validates |
|------|-------------------|
| `test_stop_all_stops_all_partitions` | `stop_all()` transitions every partition to Stopped |
| `test_flush_all_flushes_all_partitions` | `flush_all()` triggers flush on all partitions |
| `test_delete_namespace_stops_partitions` | Deleting a namespace stops its partitions first |

### Status Tests
| Test | What It Validates |
|------|-------------------|
| `test_namespace_count` | Correct count after create/delete |
| `test_partition_count_per_namespace` | Returns correct partition count |
| `test_total_partition_count` | Sum across all namespaces |

---

## Done When

- [ ] `NamespaceManager` creates and manages multiple namespaces with `DashMap`
- [ ] Each namespace has its own partition set, router, and config
- [ ] Request dispatch routes to correct partition via `PartitionRouter`
- [ ] Namespace isolation — no data leaks between namespaces
- [ ] CRUD operations on namespaces work (create, get config, update mutable fields, delete)
- [ ] Immutable config changes rejected
- [ ] Background maintenance (flush, compact) covers all partitions
- [ ] `stop_all()` gracefully shuts down all partitions
- [ ] All tests pass
