# Task 8: Proto-Engine Conversion Layer

**Crate:** `flushdb-server`
**File:** `src/conversions.rs`
**Depends on:** Nothing (uses types from flushdb-proto and flushdb-types/flushdb-engine)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §19 (API), Phase 7 §1

---

## Goal

Build bidirectional conversion functions between proto-generated types (`flushdb_proto::flushdb::v1::*`) and engine-internal types. This layer handles all the translation between the gRPC wire format and the engine's internal representation — predicate parsing, selection mapping, result formatting, and error conversion to gRPC status codes.

---

## What to Build

### 8.1 IdempotencyToken Conversion

| Function | Signature | Behavior |
|----------|-----------|----------|
| `proto_to_idempotency_token` | `(proto: Option<proto::IdempotencyToken>) -> FlushResult<IdempotencyToken>` | Converts proto token to internal type. If `None` or both fields are zero, returns `IdempotencyToken::none()` (bypass dedup). Validates token length. |
| `idempotency_token_to_proto` | `(token: &IdempotencyToken) -> proto::IdempotencyToken` | Converts internal token to proto message. |

### 8.2 OrderedKey Conversion

| Function | Signature | Behavior |
|----------|-----------|----------|
| `ordered_key_to_proto` | `(key: &OrderedKey) -> proto::OrderedKey` | Maps `timestamp_ms`, `node_id` (u16 → u32), `sequence` (u16 → u32). |
| `proto_to_ordered_key` | `(proto: &proto::OrderedKey) -> FlushResult<OrderedKey>` | Maps back. Validates `node_id` and `sequence` fit in u16. Returns `FlushError::InvalidArgument` if overflow. |

### 8.3 Predicate Parsing

Translate the proto `Predicate` oneof into engine-compatible scan parameters:

| Function | Signature | Behavior |
|----------|-----------|----------|
| `parse_predicate` | `(predicate: Option<proto::Predicate>) -> FlushResult<ParsedPredicate>` | Extracts the predicate variant. Returns error if no predicate specified. |

```
ParsedPredicate {
    MatchKeys { keys: Vec<Bytes> },
    MatchRange { start_key: Option<Bytes>, end_key: Option<Bytes>, start_inclusive: bool, end_inclusive: bool },
    MatchAll,
}
```

**Validation rules:**

| Check | Error |
|-------|-------|
| No predicate set | `FlushError::InvalidArgument { field: "predicate", reason: "must specify a predicate" }` |
| `MatchKeys` with empty keys list | `FlushError::InvalidArgument { field: "match_keys.keys", reason: "must specify at least one key" }` |
| `MatchRange` with start > end (when both set and both non-empty) | `FlushError::InvalidArgument { field: "match_range", reason: "start_key must be <= end_key" }` |

### 8.4 Selection Parsing

| Function | Signature | Behavior |
|----------|-----------|----------|
| `parse_selection` | `(selection: Option<proto::Selection>, namespace_config: &NamespaceConfig) -> RangeReadOptions` | Maps proto Selection to engine RangeReadOptions. Uses namespace defaults when fields are 0/unset. |

**Mapping:**
- `page_size_bytes`: if 0, use `namespace_config.default_page_size_bytes`; cap at `namespace_config.max_page_size_bytes`
- `item_limit`: if 0, treat as unlimited
- `page_token`: if non-empty, decode via `PageToken::from_base64()`
- `exclude_values`: pass through (used at response formatting, not at engine level)

### 8.5 Result Formatting

| Function | Signature | Behavior |
|----------|-----------|----------|
| `get_result_to_proto_item` | `(result: &GetResult) -> proto::Item` | Maps key, value, metadata fields. Sets chunk=0. |
| `merge_entry_to_proto_item` | `(entry: &MergeEntry) -> proto::Item` | Maps MergeEntry from scan results to proto Item. Extracts item_key from CompositeKey. |
| `format_get_response` | `(results: Vec<Option<GetResult>>, exclude_values: bool) -> Vec<proto::Item>` | Filters None results, applies exclude_values (clears value bytes if true). |
| `format_scan_response` | `(result: &RangeReadResult, exclude_values: bool) -> (Vec<proto::Item>, Vec<u8>)` | Converts entries to Items, returns `(items, next_page_token)`. |

### 8.6 Error to gRPC Status Conversion

| Function | Signature | Behavior |
|----------|-----------|----------|
| `flush_error_to_status` | `(err: FlushError) -> tonic::Status` | Maps engine errors to gRPC status codes. |

**Mapping table:**

| FlushError | gRPC Status Code | Details |
|------------|-------------------|---------|
| `NotFound` | `NOT_FOUND` | Namespace or record not found |
| `InvalidKey` | `INVALID_ARGUMENT` | Bad key encoding |
| `KeyTooLong` | `INVALID_ARGUMENT` | Key exceeds limits |
| `InvalidArgument` | `INVALID_ARGUMENT` | Validation failure |
| `DuplicateToken` | `ALREADY_EXISTS` | Idempotent retry detected |
| `PreconditionFailed` | `FAILED_PRECONDITION` | CAS conflict |
| `ResourceExhausted` | `RESOURCE_EXHAUSTED` | Write stall, partition frozen |
| `EpochFenced` | `ABORTED` | Zombie writer fenced |
| `CorruptedData` | `INTERNAL` | Data corruption |
| `CrcMismatch` | `INTERNAL` | Checksum failure |
| All other errors | `INTERNAL` | Unexpected error |

### 8.7 Request Validation

| Function | Signature | Behavior |
|----------|-----------|----------|
| `validate_namespace` | `(namespace: &str) -> FlushResult<()>` | Namespace must not be empty |
| `validate_record_id` | `(record_id: &str) -> FlushResult<()>` | Record ID must not be empty, must not contain null bytes, must not exceed `MAX_RECORD_ID_LEN` |
| `validate_items` | `(items: &[proto::Item]) -> FlushResult<()>` | At least one item required for PutItems. Each item key must not exceed `MAX_ITEM_KEY_LEN`. |

---

## Tests

**File:** `crates/flushdb-server/tests/conversion_tests.rs`

### IdempotencyToken Conversion Tests
| Test | What It Validates |
|------|-------------------|
| `test_proto_to_token_valid` | Valid proto token converts correctly |
| `test_proto_to_token_none` | `None` proto → `IdempotencyToken::none()` |
| `test_proto_to_token_all_zeros` | All-zero proto → bypass token |
| `test_token_round_trip` | Internal → proto → internal produces same token |

### OrderedKey Conversion Tests
| Test | What It Validates |
|------|-------------------|
| `test_ordered_key_to_proto` | Internal OrderedKey → proto preserves fields |
| `test_proto_to_ordered_key_valid` | Proto → internal with valid u16 values |
| `test_proto_to_ordered_key_overflow` | Proto with node_id > 65535 → `InvalidArgument` |
| `test_ordered_key_round_trip` | Internal → proto → internal round-trip |

### Predicate Parsing Tests
| Test | What It Validates |
|------|-------------------|
| `test_parse_match_keys` | MatchKeys predicate extracts key list |
| `test_parse_match_range` | MatchRange extracts start/end with inclusivity |
| `test_parse_match_all` | MatchAll flag parsed correctly |
| `test_parse_no_predicate` | None predicate → `InvalidArgument` |
| `test_parse_match_keys_empty` | Empty keys list → `InvalidArgument` |
| `test_parse_match_range_inverted` | start > end → `InvalidArgument` |

### Selection Parsing Tests
| Test | What It Validates |
|------|-------------------|
| `test_parse_selection_defaults` | Zero/unset fields use namespace defaults |
| `test_parse_selection_capped` | `page_size_bytes` capped at `max_page_size_bytes` |
| `test_parse_selection_with_page_token` | Non-empty page_token decoded via `PageToken::from_base64` |
| `test_parse_selection_none` | None selection → all defaults |

### Result Formatting Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_result_to_item` | GetResult fields map to Item correctly |
| `test_merge_entry_to_item` | MergeEntry's CompositeKey properly extracts item_key |
| `test_format_excludes_values` | `exclude_values=true` clears value bytes |
| `test_format_scan_includes_page_token` | Partial scan result produces non-empty page token |
| `test_format_scan_final_page_empty_token` | Complete scan result produces empty page token |

### Error Mapping Tests
| Test | What It Validates |
|------|-------------------|
| `test_not_found_maps_correctly` | `FlushError::NotFound` → `Status::not_found()` |
| `test_invalid_key_maps_correctly` | `FlushError::InvalidKey` → `Status::invalid_argument()` |
| `test_duplicate_token_maps_correctly` | `FlushError::DuplicateToken` → `Status::already_exists()` |
| `test_resource_exhausted_maps_correctly` | `FlushError::ResourceExhausted` → `Status::resource_exhausted()` |
| `test_epoch_fenced_maps_correctly` | `FlushError::EpochFenced` → `Status::aborted()` |
| `test_corrupted_data_maps_internal` | `FlushError::CorruptedData` → `Status::internal()` |

### Request Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_validate_empty_namespace` | Empty namespace → error |
| `test_validate_empty_record_id` | Empty record_id → error |
| `test_validate_record_id_null_bytes` | Null byte in record_id → error |
| `test_validate_record_id_too_long` | Oversized record_id → error |
| `test_validate_items_empty_list` | Empty items list → error |
| `test_validate_item_key_too_long` | Oversized item key → error |

---

## Done When

- [ ] Bidirectional conversions for IdempotencyToken and OrderedKey
- [ ] Predicate parsing handles all 3 variants with validation
- [ ] Selection parsing applies namespace defaults and caps
- [ ] Result formatting handles both GetResult and MergeEntry
- [ ] `exclude_values` strips value bytes from responses
- [ ] All FlushError variants map to correct gRPC status codes
- [ ] Request validation catches empty/invalid namespace, record_id, and items
- [ ] All tests pass
