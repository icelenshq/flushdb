# Task 6: IdempotencyToken — Dedup Token

**Crate:** `flushdb-types`
**File:** `src/idempotency_token.rs`
**Depends on:** Task 1 (workspace), Task 2 (FlushError)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §5.2 (WAL Entry Wire Format), §19.5 (Idempotency), Phase 1 §1.6

---

## Goal

Implement the 24-byte idempotency token used for at-least-once delivery deduplication. This token is written into WAL entries and SSTable dedup blocks — its format is a **persistent wire format** and must be stable.

---

## What to Build

### 6.1 IdempotencyToken Struct

Fixed-size 24-byte token:

```
IdempotencyToken {
  generation_time: u64    // 8 bytes — client monotonic timestamp (milliseconds)
  token:           [u8; 16] // 16 bytes — UUID v7 nonce
}
```

**Total size:** 24 bytes, always. No variable-length encoding.

**Derives:** `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`

### 6.2 Wire Format

```
Offset  Size   Field
0       8      generation_time (big-endian u64)
8       16     token (raw UUID v7 bytes)
```

Big-endian for `generation_time` so that tokens sort by creation time when compared as raw bytes.

### 6.3 Construction Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(generation_time: u64) -> Self` | Creates a new token with given timestamp and a fresh UUID v7 nonce |
| `none` | `() -> Self` | Returns the "no idempotency" sentinel — all 24 bytes zero |
| `from_bytes` | `(bytes: &[u8]) -> FlushResult<Self>` | Parse from exactly 24 bytes. Returns `InvalidArgument` if length != 24. |
| `from_parts` | `(generation_time: u64, token: [u8; 16]) -> Self` | Direct construction from components (for testing, deserialization) |

### 6.4 Accessor Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `generation_time` | `(&self) -> u64` | Returns the timestamp component |
| `token_bytes` | `(&self) -> &[u8; 16]` | Returns the UUID nonce component |
| `is_none` | `(&self) -> bool` | Returns true if all 24 bytes are zero (no-idempotency sentinel) |
| `to_bytes` | `(&self) -> [u8; 24]` | Serialize to fixed 24-byte array |
| `as_bytes` | `(&self) -> &[u8]` | Returns a reference to the 24-byte representation |

### 6.5 Clock Drift Validation

Tokens with `generation_time` too far in the future should be detectable, but validation is NOT done in the token itself — it's done at the write path level (Phase 2+). The token type is a pure data type with no policy.

However, provide a helper for downstream use:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `is_within_drift` | `(&self, now_ms: u64, max_drift_ms: u64) -> bool` | Returns true if `abs(generation_time - now_ms) <= max_drift_ms` or if token is none |

Default drift threshold: 5000ms (5 seconds), but this is a policy decision for the caller.

### 6.6 Persistent Format Warning

This format is written to:
- WAL entries (24 bytes per entry)
- SSTable dedup blocks (128-bit hash of the token)

Changing the format requires WAL and SSTable format migration. The 24-byte layout and all-zero bypass convention must remain stable across all versions.

---

## Tests

**File:** `crates/flushdb-types/tests/idempotency_token_tests.rs`

| Test | What It Validates |
|------|-------------------|
| `test_new_creates_unique_tokens` | Two calls to `new(same_time)` produce different tokens (UUID uniqueness) |
| `test_none_is_all_zeros` | `IdempotencyToken::none().to_bytes()` is 24 zero bytes |
| `test_none_is_detected` | `IdempotencyToken::none().is_none()` returns true |
| `test_non_none_is_not_none` | `IdempotencyToken::new(1234).is_none()` returns false |
| `test_round_trip_bytes` | `from_bytes(token.to_bytes())` equals original |
| `test_from_bytes_wrong_length` | 23 bytes and 25 bytes both return `InvalidArgument` |
| `test_generation_time_accessor` | Returns the timestamp passed to constructor |
| `test_token_bytes_accessor` | Returns the 16-byte UUID portion |
| `test_from_parts` | Constructs from explicit generation_time + token bytes |
| `test_is_within_drift_valid` | Token within 5s drift returns true |
| `test_is_within_drift_expired` | Token 10s in the past with 5s drift returns false |
| `test_is_within_drift_future` | Token 10s in the future with 5s drift returns false |
| `test_is_within_drift_none_always_valid` | None token always passes drift check |
| `test_copy_semantics` | Token is `Copy` — assigning doesn't move |
| `test_wire_format_big_endian` | Verify generation_time is stored big-endian in bytes |

---

## Done When

- [ ] 24-byte fixed format with big-endian generation_time
- [ ] All-zero sentinel for "no idempotency"
- [ ] Round-trip encode/decode lossless
- [ ] UUID v7 nonce generation produces unique tokens
- [ ] Drift check helper works correctly
- [ ] Token is `Copy` (stack-allocated, no heap)
- [ ] All tests pass
