# Task 4: Item & EntryType — Data Model Types

**Crate:** `flushdb-types`
**Files:** `src/item.rs`, `src/entry_type.rs`
**Depends on:** Task 1 (workspace)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §2.1 (Two-Level Map), Phase 1 §1.2, §1.4

---

## Goal

Define the fundamental data model types: `Item` (the user-facing unit of data) and `EntryType` (the operation type discriminator used across WAL, memtable, and SSTable).

---

## What to Build

### 4.1 Item Struct

The fundamental unit of data in the two-level map. This is what clients send and receive via the gRPC API.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `key` | `Bytes` | Sort key within the record. Arbitrary bytes. |
| `value` | `Bytes` | Payload. Arbitrary bytes. |
| `metadata` | `Bytes` | Optional metadata (content type, schema version, etc.). Empty `Bytes` means no metadata. |
| `chunk` | `u32` | Chunk index for large values. `0` for non-chunked items (the common case). |

**Derives:** `Debug`, `Clone`, `PartialEq`, `Eq`

**Design decisions:**
- All byte fields use `Bytes` (not `Vec<u8>`) for zero-copy sharing
- No `Option` for metadata — use empty `Bytes` instead. Simpler, avoids None vs Some(empty) ambiguity.
- `chunk` is a plain `u32`, not `Option<u32>`. Value `0` means "not chunked" (or "first/only chunk").
- No validation on Item itself — validation happens at the CompositeKey level (record_id + item_key limits) and at the API boundary (protobuf layer)

**Constructor:**

```
Item::new(key: Bytes, value: Bytes) -> Self
```
Creates an item with empty metadata and chunk = 0 (the common case).

```
Item::with_metadata(key: Bytes, value: Bytes, metadata: Bytes) -> Self
```
Creates an item with explicit metadata, chunk = 0.

```
Item::with_all(key: Bytes, value: Bytes, metadata: Bytes, chunk: u32) -> Self
```
Full constructor with all fields.

### 4.2 EntryType Enum

Three operation types that flow through WAL, memtable, and SSTable:

| Variant | Discriminant | Description |
|---------|-------------|-------------|
| `Put` | `0` | Standard put — item_key is the key, item_value is the value |
| `Delete` | `1` | Point tombstone — item_key is deleted, item_value empty |
| `RangeDelete` | `2` | Range tombstone — item_key is start (inclusive), item_value encodes end (exclusive), scoped to record_id |

**Derives:** `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`

**Representation:** Use `#[repr(u8)]` so the enum maps directly to the single-byte discriminant used in WAL wire format and SSTable encoding.

**Conversion methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `from_u8` | `(value: u8) -> FlushResult<Self>` | Convert from wire format byte. Returns `FlushError::CorruptedData` for unknown discriminants. |
| `as_u8` | `(&self) -> u8` | Convert to wire format byte. |

**Why `#[repr(u8)]`:** The discriminant value is written into WAL entries (1 byte) and SSTable entries. It is part of a persistent format. Changing the mapping would require a WAL format migration. Using `#[repr(u8)]` with explicit discriminants makes the mapping visible and stable.

---

## Tests

**File:** `crates/flushdb-types/tests/item_entry_type_tests.rs`

### Item Tests
| Test | What It Validates |
|------|-------------------|
| `test_item_new_defaults` | `Item::new(key, value)` has empty metadata and chunk=0 |
| `test_item_with_metadata` | metadata is set, chunk=0 |
| `test_item_with_all` | all fields including non-zero chunk |
| `test_item_empty_key_and_value` | Empty bytes work (simple KV pattern) |
| `test_item_clone` | Clone produces equal item |
| `test_item_equality` | Two items with same fields are equal |
| `test_item_inequality` | Items with different fields are not equal |

### EntryType Tests
| Test | What It Validates |
|------|-------------------|
| `test_entry_type_put_value` | `Put.as_u8() == 0` |
| `test_entry_type_delete_value` | `Delete.as_u8() == 1` |
| `test_entry_type_range_delete_value` | `RangeDelete.as_u8() == 2` |
| `test_entry_type_round_trip` | `from_u8(variant.as_u8()) == Ok(variant)` for all variants |
| `test_entry_type_invalid_u8` | `from_u8(3)` → `CorruptedData` error |
| `test_entry_type_invalid_u8_255` | `from_u8(255)` → `CorruptedData` error |
| `test_entry_type_is_copy` | EntryType can be copied (not just cloned) |

---

## Done When

- [ ] `Item` struct with all fields and constructors
- [ ] `EntryType` enum with `#[repr(u8)]` and explicit discriminants
- [ ] Stable `u8` conversion in both directions
- [ ] Unknown discriminants produce `CorruptedData` error
- [ ] All tests pass
