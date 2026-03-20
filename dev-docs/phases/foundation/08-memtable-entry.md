# Task 8: MemtableEntry — Internal Entry

**Crate:** `flushdb-types`
**File:** `src/memtable_entry.rs`
**Depends on:** Task 1 (workspace), Task 3 (CompositeKey), Task 4 (EntryType), Task 5 (EntryValue), Task 6 (IdempotencyToken)
**Estimated complexity:** S
**Design reference:** Phase 1 §1.3

---

## Goal

Define the internal representation for entries that flow through the WAL and memtable. This struct is the backbone of the entire write path — WAL entries serialize from it, memtable nodes store it, and SSTable flush reads from it.

---

## What to Build

### 8.1 MemtableEntry Struct

```
MemtableEntry {
  composite_key:    CompositeKey       // record_id + 0x00 + item_key
  value:            Bytes              // item value (empty for DELETE, end_key encoding for RANGE_DELETE)
  metadata:         Bytes              // item metadata (empty if none)
  idempotency_key:  IdempotencyToken   // 24-byte dedup token
  sequence_number:  u64                // WAL sequence number for ordering
  entry_type:       EntryType          // PUT | DELETE | RANGE_DELETE
}
```

**Derives:** `Debug`, `Clone`, `PartialEq`, `Eq`

### 8.2 Design Decisions

- **`composite_key` is a `CompositeKey`, not raw bytes** — ensures encoding invariants are always upheld
- **`value` is `Bytes`** — not `EntryValue`, because at the memtable level values are always inline. `EntryValue::BlobRef` only appears at the SSTable level during value separation.
- **`sequence_number` is always present** — assigned by the WAL when the entry is written. Value `0` means "not yet assigned" (entry is constructed before WAL write). The WAL writer fills this in.
- **No `namespace` field** — namespaces are a routing-layer concept. Within a single partition's memtable, all entries share the same namespace. The WAL includes namespace for recovery routing, but the memtable entry doesn't need it.

### 8.3 Construction

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(composite_key: CompositeKey, value: Bytes, metadata: Bytes, idempotency_key: IdempotencyToken, entry_type: EntryType) -> Self` | Creates entry with sequence_number = 0 (not yet assigned) |
| `with_sequence` | `(composite_key: CompositeKey, value: Bytes, metadata: Bytes, idempotency_key: IdempotencyToken, sequence_number: u64, entry_type: EntryType) -> Self` | Creates entry with explicit sequence number (for WAL replay) |

### 8.4 Accessor Methods

All fields are public (no getters needed) since this is an internal data structure, not a public API boundary.

However, provide these convenience methods:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `record_id` | `(&self) -> &[u8]` | Delegates to `self.composite_key.record_id()` |
| `item_key` | `(&self) -> &[u8]` | Delegates to `self.composite_key.item_key()` |
| `is_tombstone` | `(&self) -> bool` | Returns true if entry_type is `Delete` or `RangeDelete` |
| `is_put` | `(&self) -> bool` | Returns true if entry_type is `Put` |

### 8.5 Ordering in Memtable

MemtableEntry does NOT implement `Ord` — the memtable sorts entries by their `composite_key` (the skip list key), with `sequence_number` used as a tiebreaker for entries with the same composite key. The ordering logic lives in the memtable implementation (Phase 3), not in this struct.

### 8.6 Relationship to Other Types

```
WAL Entry (wire format, Phase 2)
  ↓ deserialize
MemtableEntry (this struct)
  ↓ insert into
Memtable skip list (Phase 3)
  ↓ flush to
SSTable entry (Phase 4) — converts value to EntryValue::Inline or BlobRef
```

The `MemtableEntry` is the central type that connects the write path (WAL → memtable) with the persistence path (memtable → SSTable).

---

## Tests

**File:** `crates/flushdb-types/tests/memtable_entry_tests.rs`

| Test | What It Validates |
|------|-------------------|
| `test_new_defaults_sequence_to_zero` | `new(...)` sets sequence_number = 0 |
| `test_with_sequence` | `with_sequence(...)` sets the provided sequence number |
| `test_record_id_delegation` | `entry.record_id()` returns same as `entry.composite_key.record_id()` |
| `test_item_key_delegation` | `entry.item_key()` returns same as `entry.composite_key.item_key()` |
| `test_is_tombstone_delete` | `Delete` entry returns true |
| `test_is_tombstone_range_delete` | `RangeDelete` entry returns true |
| `test_is_tombstone_put` | `Put` entry returns false |
| `test_is_put` | `Put` returns true, `Delete`/`RangeDelete` return false |
| `test_put_entry_has_value` | PUT entry has non-empty value |
| `test_delete_entry_empty_value` | DELETE entry has empty value (convention) |
| `test_range_delete_entry_value_is_end_key` | RANGE_DELETE entry's value contains the end key |
| `test_clone_preserves_all_fields` | All fields match after clone |
| `test_equality` | Two entries with same fields are equal |

---

## Done When

- [ ] All fields present and correctly typed
- [ ] `new()` initializes sequence_number to 0
- [ ] `with_sequence()` accepts explicit sequence numbers
- [ ] Convenience accessors delegate correctly
- [ ] Tombstone detection works for both Delete and RangeDelete
- [ ] All tests pass
