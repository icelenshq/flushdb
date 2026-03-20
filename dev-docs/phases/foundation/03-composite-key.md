# Task 3: CompositeKey — Key Encoding

**Crate:** `flushdb-types`
**File:** `src/composite_key.rs`
**Depends on:** Task 1 (workspace), Task 2 (FlushError)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §3 (Composite Key Encoding)

---

## Goal

Implement the composite key encoding that the entire storage engine operates on. Every data structure — WAL, memtable, SSTable — depends on this encoding being correct. Sort order correctness is the single most critical property.

---

## What to Build

### 3.1 Constants

```
MAX_RECORD_ID_LEN: usize = 256
MAX_ITEM_KEY_LEN: usize = 4096
MAX_COMPOSITE_KEY_LEN: usize = 4353   // 256 + 1 + 4096
SEPARATOR: u8 = 0x00
RANGE_TOMBSTONE_PREFIX: u8 = 0xFF
```

### 3.2 CompositeKey Struct

A struct wrapping `Bytes` that holds the encoded composite key. The internal representation is the binary format directly — no separate fields for record_id and item_key.

**Binary format:**
```
[record_id_bytes] [0x00] [item_key_bytes]
```

**Properties:**
- Immutable after construction
- Cheaply cloneable (backed by `Bytes` reference counting)
- Implements `Ord`, `PartialOrd`, `Eq`, `PartialEq`, `Hash`, `Clone`, `Debug`
- Ordering is raw byte comparison (`memcmp`) — this is the **critical invariant**

### 3.3 Construction Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(record_id: &[u8], item_key: &[u8]) -> FlushResult<Self>` | Validates lengths, checks record_id for null bytes, encodes |
| `from_record_only` | `(record_id: &[u8]) -> FlushResult<Self>` | Shortcut for empty item_key (simple KV pattern): `[record_id][0x00]` |
| `range_tombstone_key` | `(record_id: &[u8], start_key: &[u8]) -> FlushResult<Self>` | Creates `[record_id][0x00][0xFF][start_key]` for range tombstone entries |
| `min_key_for_record` | `(record_id: &[u8]) -> FlushResult<Self>` | Creates `[record_id][0x00]` — smallest possible key for this record |
| `max_key_for_record` | `(record_id: &[u8]) -> FlushResult<Self>` | Creates `[record_id][0x00][0xFF]` — sorts after all data keys but at/before tombstone keys |

### 3.4 Accessor Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `record_id` | `(&self) -> &[u8]` | Returns everything before the first `0x00` |
| `item_key` | `(&self) -> &[u8]` | Returns everything after the first `0x00` |
| `as_bytes` | `(&self) -> &[u8]` | Returns the raw composite key bytes |
| `into_bytes` | `(self) -> Bytes` | Consumes self, returns the inner `Bytes` |
| `is_range_tombstone` | `(&self) -> bool` | Returns true if item_key starts with `0xFF` |
| `is_empty_item_key` | `(&self) -> bool` | Returns true if item_key portion is empty |

### 3.5 Validation Rules

Validation happens at construction time, not on access:

| Check | Error |
|-------|-------|
| `record_id` is empty (0 bytes) | `FlushError::InvalidKey { reason: "record_id must not be empty" }` |
| `record_id` contains `0x00` | `FlushError::InvalidKey { reason: "record_id must not contain null bytes" }` |
| `record_id` length > 256 bytes | `FlushError::KeyTooLong { field: "record_id", actual: N, max: 256 }` |
| `item_key` length > 4096 bytes | `FlushError::KeyTooLong { field: "item_key", actual: N, max: 4096 }` |

### 3.6 Sort Order Guarantee

The `Ord` implementation MUST be equivalent to raw byte comparison. This is achieved by either:
- Deriving `Ord` on the inner `Bytes` (which already does lexicographic comparison), or
- Implementing `Ord` manually with `self.as_bytes().cmp(other.as_bytes())`

**Why this matters:** Every layer (memtable skip list, SSTable binary search, merge iterator) relies on this ordering. If `CompositeKey::cmp` diverges from `memcmp`, the entire database produces incorrect results silently.

**Sort order examples:**
```
("aaa", "x") < ("aab", "a")     // record_id ordering dominates
("aaa", "a") < ("aaa", "b")     // same record_id, item_key ordering
("aaa", "")  < ("aaa", "a")     // empty item_key sorts first
("aaa", [0x00]) < ("aaa", [0x01])  // 0x00 in item_key is valid, sorts correctly
("aaa", "z") < ("aaa", [0xFF, ...]) // range tombstone prefix sorts after all data keys
```

### 3.7 Parsing from Raw Bytes

| Method | Signature | Behavior |
|--------|-----------|----------|
| `from_bytes` | `(bytes: Bytes) -> FlushResult<Self>` | Validates that the bytes contain at least one `0x00` separator. Does NOT re-validate length limits (the bytes may come from trusted internal sources like WAL replay). |

This is needed for deserializing composite keys from WAL entries and SSTable blocks.

---

## Tests

**File:** `crates/flushdb-types/tests/composite_key_tests.rs`

### Round-trip Tests
| Test | What It Validates |
|------|-------------------|
| `test_round_trip_basic` | `new("user:123", "name")` → record_id = "user:123", item_key = "name" |
| `test_round_trip_empty_item_key` | `new("user:123", "")` → item_key is empty, encoding is `user:123\x00` |
| `test_round_trip_binary_item_key` | Item key with arbitrary bytes including `0x00` |
| `test_round_trip_max_length_keys` | record_id = 256 bytes, item_key = 4096 bytes |
| `test_round_trip_unicode_record_id` | UTF-8 multibyte record IDs (emoji, CJK characters) |
| `test_from_record_only` | `from_record_only("rec")` → empty item_key |

### Sort Order Tests
| Test | What It Validates |
|------|-------------------|
| `test_sort_different_record_ids` | `("a", "z") < ("b", "a")` — record_id dominates |
| `test_sort_same_record_different_items` | `("r", "a") < ("r", "b")` |
| `test_sort_empty_item_key_first` | `("r", "") < ("r", "a")` |
| `test_sort_null_byte_in_item_key` | `("r", "\x00") < ("r", "\x01")` |
| `test_sort_range_tombstone_after_data` | `("r", "z") < ("r", "\xFF...")` — tombstone sorts last |
| `test_sort_memcmp_equivalence` | Sort a large vec of CompositeKeys, compare against sorting the raw byte vecs |
| `test_sort_cross_record_boundary` | All keys for "aaa" sort before any key for "aab" |

### Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_rejects_empty_record_id` | `new("", "key")` → `InvalidKey` |
| `test_rejects_null_in_record_id` | `new("a\x00b", "key")` → `InvalidKey` |
| `test_rejects_oversized_record_id` | 257-byte record_id → `KeyTooLong` |
| `test_rejects_oversized_item_key` | 4097-byte item_key → `KeyTooLong` |
| `test_accepts_max_size_record_id` | Exactly 256 bytes → succeeds |
| `test_accepts_max_size_item_key` | Exactly 4096 bytes → succeeds |

### Range Tombstone Tests
| Test | What It Validates |
|------|-------------------|
| `test_range_tombstone_key_encoding` | `range_tombstone_key("r", "start")` encodes with `0xFF` prefix |
| `test_range_tombstone_is_detected` | `is_range_tombstone()` returns true for tombstone keys |
| `test_data_key_not_tombstone` | `is_range_tombstone()` returns false for regular keys |
| `test_range_tombstone_sorts_after_all_data` | Tombstone key sorts after all regular item keys within same record |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_from_bytes_valid` | Parse raw bytes back to CompositeKey |
| `test_from_bytes_no_separator` | Raw bytes without `0x00` → error |
| `test_single_byte_record_id` | `new("a", "b")` works correctly |
| `test_clone_is_cheap` | Cloning doesn't deep-copy the underlying buffer |

---

## Done When

- [ ] All construction methods work with valid inputs
- [ ] All validation rules reject invalid inputs with correct error variants
- [ ] Sort order is `memcmp`-equivalent (verified by comparison test)
- [ ] Range tombstone keys sort after all data keys within a record
- [ ] Round-trip encode/decode is lossless
- [ ] All tests pass
