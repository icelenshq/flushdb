# Task 10: gRPC Read Handlers (GetItems + ScanItems)

**Crate:** `flushdb-server`
**File:** `src/handlers/read.rs`
**Depends on:** Task 7 (NamespaceManager), Task 8 (Proto-Engine Conversions)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §19.2 (GetItems), §19.4 (ScanItems), §13 (Pagination), Phase 7 §1, §9

---

## Goal

Implement the `GetItems` and `ScanItems` gRPC handlers. `GetItems` is the paginated read path with SLO-aware early return. `ScanItems` is the server-side streaming variant that continuously streams batches until the predicate is exhausted or the client cancels. Both handlers translate between proto predicates and engine read operations.

---

## What to Build

### 10.1 GetItems Handler

```
async fn get_items(&self, request: Request<GetItemsRequest>) -> Result<Response<GetItemsResponse>, Status>
```

**Flow:**
1. Extract `namespace`, `id` (record_id), `predicate`, `selection`
2. Validate namespace and record_id
3. Parse predicate via `parse_predicate()`
4. Parse selection via `parse_selection()` (apply namespace defaults and caps)
5. Record SLO start time from `namespace_config.target_latency_slo_ms`
6. Execute read based on predicate type:
   - **`MatchKeys`** → call `namespace_manager.multi_get(namespace, record_id, keys)`
   - **`MatchRange`** → call `namespace_manager.scan(namespace, record_id, start, end, options)`
   - **`MatchAll`** → call `namespace_manager.scan(namespace, record_id, None, None, options)`
7. Format results via `format_get_response()` or `format_scan_response()`
8. Check SLO: if elapsed time approaching `target_latency_slo_ms`, return partial results with page token
9. Return `GetItemsResponse { items, next_page_token }`

**Predicate → Engine call mapping:**

| Predicate | Engine Method | Result Type |
|-----------|--------------|-------------|
| `MatchKeys` | `multi_get(record_id, keys)` | `Vec<Option<GetResult>>` |
| `MatchRange` | `scan(record_id, start, end, options)` | `RangeReadResult` |
| `MatchAll` | `scan(record_id, None, None, options)` | `RangeReadResult` |

### 10.2 SLO-Aware Pagination

**Byte-based pagination** — controlled by `Selection.page_size_bytes`:
- The engine's `RangeReadOptions.page_size_bytes` already implements byte budgets
- The engine returns `RangeReadResult.is_partial` and `next_page_token` when budget is met

**SLO-aware early return** — stops even before byte budget if latency SLO is at risk:
- Track elapsed time since request start
- If elapsed >= 80% of `target_latency_slo_ms` and items have been collected, stop and return with page token
- If gRPC deadline (from request metadata) is nearly exhausted (< 10% remaining), stop immediately

**Implementation:**

| Function | Signature | Behavior |
|----------|-----------|----------|
| `check_slo_budget` | `(start: Instant, target_slo_ms: u64) -> bool` | Returns `true` if >80% of SLO budget consumed |
| `check_grpc_deadline` | `(request: &Request<T>) -> bool` | Returns `true` if gRPC deadline < 10% remaining |

### 10.3 MatchKeys Handling

For `MatchKeys` predicate, use the engine's `multi_get`:

1. Call `namespace_manager.multi_get(namespace, record_id, keys)`
2. Result is `Vec<Option<GetResult>>` — positional, matching input keys
3. Filter out `None` entries (missing keys)
4. Format remaining entries as proto Items
5. No pagination for `MatchKeys` — all results returned in one response (the number of keys is bounded by the request)

### 10.4 ScanItems Handler (Server Streaming)

```
async fn scan_items(&self, request: Request<ScanItemsRequest>) -> Result<Response<Self::ScanItemsStream>, Status>
```

**Return type:** `Self::ScanItemsStream` is a `Pin<Box<dyn Stream<Item = Result<ScanItemsResponse, Status>> + Send>>`

**Flow:**
1. Extract `namespace`, `id` (record_id), `predicate`, `signals`
2. Validate namespace and record_id
3. Parse predicate
4. Stream items in batches:
   - Each batch: call engine scan with `page_size_bytes` (use default from namespace config)
   - Yield `ScanItemsResponse { items: batch }` on the stream
   - If `next_page_token` is non-empty, continue scanning with that token
   - If `next_page_token` is empty, stream is complete
5. If client cancels (stream dropped), stop scanning

**Batch size:** Use `namespace_config.default_page_size_bytes` as the per-batch byte budget.

**No page tokens to client:** Unlike `GetItems`, `ScanItems` does not expose page tokens. The streaming protocol handles continuation internally.

### 10.5 exclude_values Mode

When `Selection.exclude_values = true` (metadata-only mode):
- `GetItems`: strip value bytes from all Items in the response (keep key + metadata)
- `ScanItems`: same stripping per batch

This is handled by `format_get_response()` and `format_scan_response()` from Task 8.

### 10.6 Error Handling

| Scenario | Behavior |
|----------|----------|
| Namespace not found | `Status::not_found("namespace not found: {name}")` |
| Invalid record_id | `Status::invalid_argument(details)` |
| Invalid predicate | `Status::invalid_argument(details)` |
| Invalid page token | `Status::invalid_argument("invalid page token")` |
| Engine error during scan | Stream yields `Err(Status::internal(details))` then terminates |

### 10.7 Tracing

Add `tracing` spans:
- `get_items`: fields `namespace`, `record_id`, `predicate_type`, `page_size_bytes`
- `scan_items`: fields `namespace`, `record_id`, `predicate_type`
- Log at `debug` level: per-page item counts, elapsed time
- Log at `info` level: total items returned, SLO status (met/exceeded)

---

## Tests

**File:** `crates/flushdb-server/tests/read_handler_tests.rs`

### GetItems — MatchKeys Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_match_keys_single` | Single key lookup returns correct item |
| `test_get_match_keys_multi` | Multiple key lookup returns all matching items |
| `test_get_match_keys_missing_key` | Missing key omitted from results (not error) |
| `test_get_match_keys_all_missing` | All keys missing → empty items list, no error |
| `test_get_match_keys_empty_rejected` | Empty keys list → `INVALID_ARGUMENT` |

### GetItems — MatchRange Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_match_range_inclusive` | Range with start_inclusive=true, end_inclusive=false |
| `test_get_match_range_full_record` | Range covering all items in record |
| `test_get_match_range_empty_result` | Range with no matching items → empty list |
| `test_get_match_range_with_pagination` | Large result set paginated with page token |
| `test_get_match_range_resume_from_token` | Second request with page_token continues from last item |

### GetItems — MatchAll Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_match_all_returns_all` | All items in record returned |
| `test_get_match_all_empty_record` | Empty record → empty items list |
| `test_get_match_all_with_pagination` | Large record paginated correctly |

### GetItems — Pagination Tests
| Test | What It Validates |
|------|-------------------|
| `test_pagination_byte_budget` | Stops at page_size_bytes budget |
| `test_pagination_item_limit` | Stops at item_limit |
| `test_pagination_token_continuity` | Multiple pages cover all items without gaps or duplicates |
| `test_pagination_respects_max_page_size` | page_size_bytes capped at namespace max |

### GetItems — SLO Tests
| Test | What It Validates |
|------|-------------------|
| `test_slo_aware_partial_return` | Returns partial results when approaching SLO deadline |
| `test_slo_returns_page_token_on_early_stop` | Partial page includes valid page_token for continuation |

### GetItems — exclude_values Tests
| Test | What It Validates |
|------|-------------------|
| `test_exclude_values_strips_value` | With exclude_values=true, item values are empty |
| `test_exclude_values_preserves_keys_metadata` | Keys and metadata still present |

### ScanItems Tests
| Test | What It Validates |
|------|-------------------|
| `test_scan_streams_all_items` | ScanItems returns all items across multiple stream messages |
| `test_scan_match_range` | Streaming with MatchRange predicate returns correct subset |
| `test_scan_match_all` | Streaming with MatchAll returns entire record |
| `test_scan_empty_record` | Empty record → stream completes with no messages |
| `test_scan_large_record_multiple_batches` | Large record streamed in batches (not one giant message) |
| `test_scan_batch_size_respects_config` | Batch size matches namespace default_page_size_bytes |

### Error Handling Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_nonexistent_namespace` | Read from missing namespace → `NOT_FOUND` |
| `test_get_no_predicate` | Missing predicate → `INVALID_ARGUMENT` |
| `test_get_invalid_page_token` | Corrupted page_token → `INVALID_ARGUMENT` |
| `test_scan_nonexistent_namespace` | Scan of missing namespace → `NOT_FOUND` |

---

## Done When

- [ ] `GetItems` handles MatchKeys, MatchRange, and MatchAll predicates
- [ ] Byte-based pagination with `page_size_bytes` and `item_limit` works correctly
- [ ] Page tokens allow seamless continuation across multiple requests
- [ ] SLO-aware early return triggers when approaching latency deadline
- [ ] `ScanItems` streams batches until predicate exhausted
- [ ] `exclude_values` strips value bytes from responses
- [ ] All engine errors map to correct gRPC status codes
- [ ] Tracing spans cover both handlers with relevant fields
- [ ] All tests pass
