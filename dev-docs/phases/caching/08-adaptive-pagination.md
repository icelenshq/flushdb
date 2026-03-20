# Task 8: Adaptive Pagination

**Crate:** `flushdb-engine`
**Files:** `src/cache/pagination.rs`, `src/read_path.rs`
**Depends on:** Task 2 (CachingBlockFetcher — reads observe item sizes)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §13 (Byte-Based Pagination), Phase 6 §9 (Adaptive Pagination)

---

## Goal

Improve pagination accuracy by tracking average item sizes per namespace and using them to estimate how many items to read for a given byte budget. The first page uses a server-side estimate; subsequent pages use the actual average observed from the previous page (embedded in the page token). This reduces over-reading (fetching too many items) and under-reading (fetching too few items, requiring additional StorageBackend calls).

---

## What to Build

### 8.1 NamespaceSizeEstimator

Tracks running average item sizes per namespace:

```
NamespaceSizeEstimator {
    averages: HashMap<String, RunningAverage>,
}
```

```
RunningAverage {
    total_bytes: u64,
    total_items: u64,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty estimator |
| `record_items` | `(&mut self, namespace: &str, total_bytes: usize, item_count: usize)` | Updates running average for the namespace. `total_bytes` is the sum of key + value + metadata for all items. |
| `estimate_avg_item_size` | `(&self, namespace: &str) -> Option<usize>` | Returns `total_bytes / total_items` for the namespace, or None if no data recorded |
| `estimate_item_count` | `(&self, namespace: &str, page_size_bytes: usize) -> usize` | Returns `page_size_bytes / avg_item_size`. Falls back to `page_size_bytes / DEFAULT_ITEM_SIZE` (1024 bytes) if no data. Minimum return: 1. |
| `reset` | `(&mut self, namespace: &str)` | Clears running average for the namespace (e.g., after schema changes) |

### 8.2 PageToken Extension

Extend the existing `PageToken` with an optional average item size field:

Current `PageToken`:
```
PageToken {
    last_composite_key: CompositeKey,
    last_sequence_number: u64,
}
```

Extended:
```
PageToken {
    last_composite_key: CompositeKey,
    last_sequence_number: u64,
    avg_item_size_bytes: Option<u32>,   // NEW: observed average from previous page
}
```

**Binary format extension:**
```
[offset 0..4]           key_len: u32 (little-endian)
[offset 4..4+key_len]   composite_key: bytes
[offset 4+key_len..+8]  sequence_number: u64 (little-endian)
[offset +8..+9]         has_avg_item_size: u8 (0 or 1)    NEW
[offset +9..+13]        avg_item_size_bytes: u32 (le)       NEW (only if has_avg_item_size == 1)
```

The `encode()` and `decode()` methods must be updated to handle the new field. Old tokens without the field are still decodable (backwards compatible) — `has_avg_item_size` defaults to 0 if the token is too short.

### 8.3 Read Path Integration

Modify `ReadPath::range_read()` to use adaptive pagination:

**First page (no resume_from):**
1. Use `NamespaceSizeEstimator::estimate_item_count(namespace, page_size_bytes)` to set an internal hint for how many items to expect
2. Read until byte budget met (current behavior)
3. After building the result, compute actual avg item size: `total_bytes / entries.len()`
4. Store `avg_item_size_bytes` in the returned `PageToken`
5. Call `estimator.record_items(namespace, total_bytes, entries.len())` to update the running average

**Subsequent pages (with resume_from):**
1. If `resume_from.avg_item_size_bytes` is Some, use it instead of the estimator for a more accurate read plan
2. Read until byte budget met (current behavior)
3. Compute actual avg for this page and store in the new `PageToken`
4. Update estimator with observed data

**Note:** The adaptive logic currently only affects the estimator and page token. A future optimization (Phase 7 SLO-awareness) can use the estimate to pre-size buffers and plan StorageBackend reads more efficiently.

### 8.4 Constants

```
DEFAULT_ITEM_SIZE: usize = 1024    // Default assumed avg item size when no data is available
MIN_ITEMS_PER_PAGE: usize = 1      // Always read at least 1 item
```

### 8.5 Design Decisions

- **Running average, not windowed:** The average is cumulative over all observed reads. This is simpler and good enough — item sizes within a namespace tend to be stable. A future enhancement could use an exponentially weighted moving average (EWMA) for workloads with shifting item sizes.
- **Page token carries observed average:** This is more accurate than the global estimator for the specific pagination sequence because it reflects the actual data distribution seen in the previous page.
- **Backwards-compatible encoding:** Old page tokens without `avg_item_size_bytes` are still valid. This ensures rolling upgrades don't break in-flight paginations.
- **Estimator is per-namespace:** Different namespaces can have very different item sizes (e.g., small metadata vs large blobs). Per-namespace tracking prevents cross-contamination.

### 8.6 Future-Proofing

- **Server-side caching (Phase 7):** The server will maintain a persistent cache of per-namespace average item sizes. The `NamespaceSizeEstimator` should expose `averages()` for serialization and `load()` for deserialization so the server can persist this across restarts.
- **Item size feed hook (Phase 7):** The server needs to observe item sizes during reads. The `record_items` method is the hook — the server calls it after each read operation.

---

## Tests

**File:** `crates/flushdb-engine/tests/adaptive_pagination_tests.rs`

### NamespaceSizeEstimator
| Test | What It Validates |
|------|-------------------|
| `test_empty_estimator_returns_none` | estimate_avg_item_size for unknown namespace returns None |
| `test_record_and_estimate` | Record 10 items totaling 5000 bytes, estimate returns 500 |
| `test_estimate_item_count` | With avg 500 bytes, page_size 2048 → estimate_item_count returns 4 |
| `test_estimate_item_count_default` | Unknown namespace, page_size 2048 → uses DEFAULT_ITEM_SIZE → returns 2 |
| `test_multiple_namespaces_independent` | Record data for ns "A" and "B", estimates are independent |
| `test_running_average_updates` | Record 1000B/1 item (avg 1000), then 2000B/4 items → avg is (3000/5) = 600 |
| `test_reset` | Record items, reset, estimate returns None |

### PageToken Extension
| Test | What It Validates |
|------|-------------------|
| `test_page_token_with_avg_round_trip` | Encode token with avg_item_size_bytes=Some(512), decode, verify avg preserved |
| `test_page_token_without_avg_round_trip` | Encode token with avg_item_size_bytes=None, decode, verify avg is None |
| `test_page_token_backwards_compatible` | Decode a token encoded with old format (no avg field) → avg is None, key/seq intact |
| `test_page_token_base64_round_trip_with_avg` | to_base64 / from_base64 round-trip preserves avg_item_size_bytes |

### Adaptive Read Path
| Test | What It Validates |
|------|-------------------|
| `test_first_page_updates_estimator` | Range read first page, verify estimator has data for the namespace |
| `test_page_token_carries_avg` | First page returns token with avg_item_size_bytes populated |
| `test_second_page_uses_token_avg` | First page → token with avg 200. Second page uses that avg for planning. |
| `test_pagination_accuracy_improves` | Compare first page vs third page item count accuracy — should be closer to page_size_bytes target |

---

## Done When

- [ ] `NamespaceSizeEstimator` tracks running average item sizes per namespace
- [ ] `PageToken` extended with optional `avg_item_size_bytes` field
- [ ] Token encoding is backwards-compatible with old format
- [ ] Range reads update the estimator after each page
- [ ] Subsequent pages use observed averages from previous page tokens
- [ ] All tests pass
