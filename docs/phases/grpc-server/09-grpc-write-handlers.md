# Task 9: gRPC Write Handlers (PutItems + DeleteItems)

**Crate:** `flushdb-server`
**File:** `src/handlers/write.rs`
**Depends on:** Task 5 (VersionGenerator), Task 7 (NamespaceManager), Task 8 (Proto-Engine Conversions)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §19.1 (PutItems), §19.3 (DeleteItems), §19.5 (Idempotency), Phase 7 §1, §5

---

## Goal

Implement the `PutItems` and `DeleteItems` gRPC handlers. These are the write path entry points that validate requests, check idempotency, generate version keys, route to the correct partition, and call the engine's write methods. Each write operation returns a monotonically increasing `OrderedKey` version to the client.

---

## What to Build

### 9.1 FlushDbService Struct

The tonic service implementation struct that holds all shared state:

```
FlushDbService<B: StorageBackend> {
    namespace_manager: Arc<NamespaceManager<B>>,
    version_generator: Arc<VersionGenerator>,
}
```

This struct implements `flushdb_proto::flushdb::v1::flush_db_server::FlushDb` (the tonic-generated service trait).

### 9.2 PutItems Handler

```
async fn put_items(&self, request: Request<PutItemsRequest>) -> Result<Response<PutItemsResponse>, Status>
```

**Flow:**
1. Extract request fields: `namespace`, `id` (record_id), `items`, `idempotency_token`
2. Validate via `validate_namespace()`, `validate_record_id()`, `validate_items()`
3. Convert `idempotency_token` via `proto_to_idempotency_token()`
4. For each item in `items`:
   a. Call `namespace_manager.put(namespace, record_id, item.key, item.value, item.metadata, idempotency_token)`
   b. The engine handles dedup internally — `DuplicateToken` error means the write was already applied
5. Generate version via `version_generator.next_version()`
6. Return `PutItemsResponse { version: ordered_key_to_proto(&version) }`

**Idempotency behavior:**
- If the token is all-zero (`IdempotencyToken::none()`), bypass dedup entirely
- If the token matches an existing write within retention window, the engine returns `FlushError::DuplicateToken`
- On `DuplicateToken`, return success with a new version (the write is idempotent — the data is already there)

**Multi-item writes:**
- All items in a single `PutItemsRequest` share the same `record_id` and `idempotency_token`
- Items are written sequentially to the same partition (same record_id = same partition)
- All items get the same version in the response

### 9.3 DeleteItems Handler

```
async fn delete_items(&self, request: Request<DeleteItemsRequest>) -> Result<Response<DeleteItemsResponse>, Status>
```

**Flow:**
1. Extract request fields: `namespace`, `id` (record_id), `predicate`, `idempotency_token`
2. Validate namespace and record_id
3. Parse predicate via `parse_predicate()`
4. Convert idempotency token
5. Execute delete based on predicate type:
   - **`MatchAll`** → `namespace_manager.delete(namespace, record_id, &[])` — writes a record-level tombstone (empty item_key = entire record)
   - **`MatchRange`** → `namespace_manager.delete_range(namespace, record_id, start_key, end_key)` — writes a range tombstone
   - **`MatchKeys`** → for each key, `namespace_manager.delete(namespace, record_id, key)` — per-item tombstones
6. Generate version via `version_generator.next_version()`
7. Return `DeleteItemsResponse { version: ordered_key_to_proto(&version) }`

**Delete semantics by predicate:**

| Predicate | Engine Call | Tombstone Type |
|-----------|------------|----------------|
| `MatchAll` | `delete(record_id, &[])` with empty item_key | Record-level tombstone (constant latency) |
| `MatchRange` | `delete_range(record_id, start, end)` | Single range tombstone covering `[start, end)` |
| `MatchKeys` | `delete(record_id, key)` per key | Per-item tombstones |

### 9.4 Error Handling

All errors from the engine are caught and converted to gRPC Status via `flush_error_to_status()`:

| Scenario | Behavior |
|----------|----------|
| Namespace not found | `Status::not_found("namespace not found: {name}")` |
| Invalid record_id | `Status::invalid_argument(details)` |
| Duplicate idempotency token | Return success (write is idempotent) |
| Write stall (ResourceExhausted) | `Status::resource_exhausted("write stall: {details}")` |
| Engine error | `Status::internal(details)` |

### 9.5 Tracing

Add `tracing` spans to both handlers:
- Span name: `put_items` / `delete_items`
- Span fields: `namespace`, `record_id`, `item_count` (for put), `predicate_type` (for delete)
- Log at `debug` level on entry, `info` on completion with version

---

## Tests

**File:** `crates/flushdb-server/tests/write_handler_tests.rs`

### PutItems Tests
| Test | What It Validates |
|------|-------------------|
| `test_put_items_single_item` | Single item write returns version |
| `test_put_items_multiple_items` | Multiple items in one request all written |
| `test_put_items_empty_namespace_rejected` | Empty namespace → `INVALID_ARGUMENT` |
| `test_put_items_empty_record_id_rejected` | Empty record_id → `INVALID_ARGUMENT` |
| `test_put_items_no_items_rejected` | Empty items list → `INVALID_ARGUMENT` |
| `test_put_items_version_monotonic` | Sequential puts return increasing versions |
| `test_put_items_idempotent_retry` | Same token twice → success both times (not error) |
| `test_put_items_bypass_dedup` | All-zero token → no dedup check |
| `test_put_items_nonexistent_namespace` | Put to missing namespace → `NOT_FOUND` |
| `test_put_items_with_metadata` | Items with metadata field preserved in storage |
| `test_put_items_large_value` | 1MB value write succeeds |

### DeleteItems Tests
| Test | What It Validates |
|------|-------------------|
| `test_delete_match_all` | MatchAll writes record-level tombstone |
| `test_delete_match_range` | MatchRange writes range tombstone |
| `test_delete_match_keys` | MatchKeys writes per-item tombstones |
| `test_delete_no_predicate_rejected` | Missing predicate → `INVALID_ARGUMENT` |
| `test_delete_confirms_data_gone` | Delete → Get returns no data |
| `test_delete_returns_version` | Delete returns a valid version |
| `test_delete_nonexistent_namespace` | Delete from missing namespace → `NOT_FOUND` |

### Idempotency Tests
| Test | What It Validates |
|------|-------------------|
| `test_idempotent_put_same_data` | Duplicate token with same data → success |
| `test_idempotent_put_no_double_write` | Duplicate token doesn't write data twice |
| `test_bypass_token_allows_duplicates` | All-zero token allows multiple identical writes |

### Error Handling Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_stall_returns_resource_exhausted` | Engine in write stall → proper gRPC status |
| `test_invalid_key_returns_invalid_argument` | Oversized record_id → `INVALID_ARGUMENT` |

---

## Done When

- [ ] `PutItems` validates, converts, routes, writes, and returns version
- [ ] `DeleteItems` handles all 3 predicate types correctly
- [ ] Idempotent retries return success, not error
- [ ] All-zero token bypasses dedup
- [ ] Versions are monotonically increasing across calls
- [ ] All engine errors map to correct gRPC status codes
- [ ] Tracing spans cover both handlers
- [ ] All tests pass
