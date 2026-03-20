# Task 13: End-to-End Integration Tests (Testcontainers/MinIO)

**Crate:** `flushdb-test`
**File:** `tests/e2e_tests.rs`
**Depends on:** Task 1 (S3StorageBackend), Task 12 (Server Bootstrap)
**Estimated complexity:** L
**Design reference:** Phase 7 §Done When, docs/TESTING.md

---

## Goal

Build comprehensive end-to-end tests that exercise the full system over gRPC against real S3 (MinIO via testcontainers). These tests validate the complete request lifecycle: gRPC client → server → namespace manager → partition router → engine → S3 backend → response. This is where all the pieces from tasks 1-12 are validated together.

---

## What to Build

### 13.1 Test Server Harness

A reusable test utility that starts a full FlushDbServer in-process for testing:

```
TestServer {
    server_addr:  SocketAddr,
    client:       FlushDbClient<Channel>,
    shutdown_tx:  watch::Sender<bool>,
    server_handle: JoinHandle<()>,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `start` | `async (backend_type: BackendType) -> Self` | Starts server with random available port, creates gRPC client connected to it. `BackendType` is `LocalFs` or `S3(MinioContainer)`. |
| `start_with_namespaces` | `async (backend_type: BackendType, namespaces: Vec<NamespaceConfig>) -> Self` | Starts server and pre-creates namespaces |
| `client` | `(&self) -> &FlushDbClient<Channel>` | Returns the gRPC client for making requests |
| `shutdown` | `async (self)` | Triggers server shutdown and waits for completion |

### 13.2 Test Helpers

| Function | Signature | Behavior |
|----------|-----------|----------|
| `make_put_request` | `(namespace, record_id, items: Vec<(key, value)>) -> PutItemsRequest` | Constructs a PutItems request with auto-generated idempotency token |
| `make_get_request` | `(namespace, record_id, predicate: Predicate) -> GetItemsRequest` | Constructs a GetItems request |
| `make_delete_request` | `(namespace, record_id, predicate: Predicate) -> DeleteItemsRequest` | Constructs a DeleteItems request |
| `make_scan_request` | `(namespace, record_id, predicate: Predicate) -> ScanItemsRequest` | Constructs a ScanItems request |
| `collect_scan_stream` | `async (stream: Streaming<ScanItemsResponse>) -> Vec<Item>` | Collects all items from a scan stream |

---

## Tests

**File:** `crates/flushdb-test/tests/e2e_tests.rs`

### Full CRUD Cycle Tests
| Test | What It Validates |
|------|-------------------|
| `test_crud_put_get_delete_get` | PutItems → GetItems (sees data) → DeleteItems → GetItems (confirms deletion). Full lifecycle. |
| `test_crud_put_overwrite` | PutItems same record+key twice → GetItems returns latest value |
| `test_crud_put_multiple_items` | PutItems with 10 items → GetItems MatchAll returns all 10 |
| `test_crud_delete_match_all` | PutItems 5 items → DeleteItems MatchAll → GetItems returns empty |
| `test_crud_delete_match_range` | PutItems items a-z → DeleteItems MatchRange(c,f) → only c,d,e deleted |
| `test_crud_delete_match_keys` | PutItems items a,b,c → DeleteItems MatchKeys(a,c) → only b remains |

### Namespace Isolation Tests
| Test | What It Validates |
|------|-------------------|
| `test_namespace_isolation_writes` | Write to ns-A, read from ns-B → empty. Namespaces are fully isolated. |
| `test_namespace_isolation_deletes` | Delete in ns-A, read from ns-B → data unaffected |
| `test_namespace_independent_manifests` | Each namespace has independent flush/manifest lifecycle |
| `test_namespace_independent_compaction` | Compaction in ns-A doesn't affect ns-B |

### Partition Routing Tests
| Test | What It Validates |
|------|-------------------|
| `test_partition_simple_routing` | Simple strategy: same record always routes to same partition |
| `test_partition_distribution` | 100 different record_ids distribute across partitions (not all to one) |
| `test_partition_composite_locality` | Composite strategy: records with same tenant prefix co-locate |
| `test_partition_prefix_grouping` | Prefix strategy: records with same prefix co-locate |

### Idempotency Tests
| Test | What It Validates |
|------|-------------------|
| `test_idempotent_put_returns_success` | Same request with same token → success both times |
| `test_idempotent_put_no_double_data` | After idempotent retry, GetItems returns data once (not duplicated) |
| `test_bypass_token_allows_duplicates` | All-zero token → multiple identical writes allowed |

### Pagination Tests
| Test | What It Validates |
|------|-------------------|
| `test_pagination_full_traversal` | Write 100 items → paginate with small page_size → all items returned across pages with no gaps or duplicates |
| `test_pagination_item_limit` | item_limit=5 → returns at most 5 items per page |
| `test_pagination_token_continuity` | Page 1 token → page 2 starts where page 1 ended |
| `test_pagination_byte_budget` | Large values → fewer items per page to stay within byte budget |

### ScanItems Streaming Tests
| Test | What It Validates |
|------|-------------------|
| `test_scan_streams_all_items` | Write 50 items → ScanItems returns all 50 across stream batches |
| `test_scan_match_range_streaming` | ScanItems with MatchRange returns correct subset |
| `test_scan_match_all_streaming` | ScanItems with MatchAll returns entire record |
| `test_scan_empty_record` | ScanItems on empty record → stream completes immediately |
| `test_scan_large_record` | Write 1000 items → ScanItems streams all in multiple batches |

### OrderedKey Version Tests
| Test | What It Validates |
|------|-------------------|
| `test_versions_monotonically_increasing` | Sequential puts return increasing versions |
| `test_versions_contain_node_id` | Returned versions include the configured node_id |
| `test_versions_unique_across_requests` | No two writes return the same version |

### SLO-Aware Pagination Tests
| Test | What It Validates |
|------|-------------------|
| `test_slo_partial_return` | Under time pressure, returns partial results with page token |
| `test_slo_page_token_resumes_correctly` | After SLO-triggered partial return, next page continues correctly |

### Error Handling Tests
| Test | What It Validates |
|------|-------------------|
| `test_nonexistent_namespace_not_found` | Operations on missing namespace → `NOT_FOUND` |
| `test_empty_namespace_invalid` | Empty namespace string → `INVALID_ARGUMENT` |
| `test_empty_record_id_invalid` | Empty record_id → `INVALID_ARGUMENT` |
| `test_no_predicate_invalid` | GetItems without predicate → `INVALID_ARGUMENT` |
| `test_put_no_items_invalid` | PutItems with empty items → `INVALID_ARGUMENT` |

### Graceful Shutdown Tests
| Test | What It Validates |
|------|-------------------|
| `test_graceful_shutdown_preserves_data` | Write data → shutdown → restart → data still readable |
| `test_shutdown_rejects_new_writes` | During shutdown, new writes → `UNAVAILABLE` |
| `test_shutdown_completes_inflight_reads` | In-flight reads complete during shutdown (not dropped) |

### S3 Backend E2E Tests (Testcontainers/MinIO)
| Test | What It Validates |
|------|-------------------|
| `test_e2e_crud_with_s3` | Full CRUD cycle using S3StorageBackend against MinIO |
| `test_e2e_namespace_isolation_s3` | Namespace isolation with S3 backend |
| `test_e2e_flush_to_s3` | Data survives memtable flush to S3 SSTable |
| `test_e2e_s3_conditional_put_manifests` | Manifest CAS works correctly against S3 |
| `test_e2e_compaction_with_s3` | Compaction runs correctly with S3 backend |
| `test_e2e_recovery_from_s3` | After restart, engine recovers from S3 manifests + WAL |
| `test_e2e_hash_prefix_sharding` | S3 objects distributed across 128 hash prefixes |
| `test_e2e_large_sstable_multipart` | SSTable >16MB uploaded via multipart to S3 |

---

## Done When

- [ ] Test server harness starts/stops reliably with both LocalFs and S3 backends
- [ ] Full CRUD cycle passes over gRPC
- [ ] Namespace isolation verified — no data leaks between namespaces
- [ ] Partition routing distributes data correctly for Simple strategy
- [ ] Composite and Prefix partition strategies route correctly
- [ ] Idempotency enforcement rejects duplicate tokens
- [ ] Pagination traverses all items without gaps or duplicates
- [ ] ScanItems streams all items across multiple batches
- [ ] Versions are monotonically increasing and unique
- [ ] SLO-aware pagination returns partial results under deadline pressure
- [ ] Graceful shutdown preserves data and completes in-flight requests
- [ ] S3 E2E tests pass against MinIO via testcontainers
- [ ] S3 manifest CAS, flush, compaction, and recovery work end-to-end
- [ ] No tests use `#[ignore]` or conditional skips
- [ ] `cargo test -p flushdb-test --features s3` passes with zero failures
