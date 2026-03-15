# Task 5: EntryValue — SSTable Entry Format

**Crate:** `flushdb-types`
**File:** `src/entry_value.rs`
**Depends on:** Task 1 (workspace)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §14.2 (Value Separation), Phase 1 §1.5

---

## Goal

Define the `EntryValue` enum that represents how a value is stored in SSTables. The `BlobRef` variant must be defined now — even though only `Inline` is used until value separation is implemented — to avoid SSTable format migration later.

---

## What to Build

### 5.1 EntryValue Enum

Two variants representing how a value is stored:

```
EntryValue {
  Inline(Bytes)                                    // value stored directly in SSTable
  BlobRef { blob_id: Bytes, offset: u64, size: u32 } // pointer to separated blob
}
```

| Variant | Fields | Description |
|---------|--------|-------------|
| `Inline` | `Bytes` | Value stored directly in the SSTable data block. This is the only variant used in Phase 1-6. |
| `BlobRef` | `blob_id: Bytes`, `offset: u64`, `size: u32` | Pointer to a value stored in a separate blob file on S3. Used for values >= 32KB (value separation, future work). |

**Derives:** `Debug`, `Clone`, `PartialEq`, `Eq`

### 5.2 Design Decisions

- **`blob_id` is `Bytes`, not `String` or ULID** — at the type level we don't impose format. The blob_id will be a ULID in practice, but the type layer doesn't need to know that. This keeps `flushdb-types` free of the `ulid` crate dependency.
- **Phase 1 only uses `Inline`** — but `BlobRef` must be fully defined (all three fields) because the SSTable binary format needs to know the exact layout to encode/decode entries. Adding fields later would change the format.
- **No serialization in this task** — the SSTable encoder (Phase 4) handles binary encoding. This task just defines the in-memory representation.

### 5.3 Accessor Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `is_inline` | `(&self) -> bool` | Returns true for `Inline` variant |
| `is_blob_ref` | `(&self) -> bool` | Returns true for `BlobRef` variant |
| `inline_value` | `(&self) -> Option<&Bytes>` | Returns the inline value if `Inline`, None otherwise |
| `as_inline` | `(self) -> Option<Bytes>` | Consumes self, returns Bytes if Inline |
| `inline_size` | `(&self) -> usize` | Returns byte size for Inline, 0 for BlobRef |

### 5.4 Binary Encoding Format

Define the discriminant byte values for future use by SSTable encoder:

| Variant | Discriminant | Encoding |
|---------|-------------|----------|
| `Inline` | `0x00` | `[0x00] [value_bytes]` |
| `BlobRef` | `0x01` | `[0x01] [blob_id_len: u16] [blob_id] [offset: u64] [size: u32]` |

These constants should be defined as associated constants on `EntryValue`:
- `INLINE_TAG: u8 = 0x00`
- `BLOB_REF_TAG: u8 = 0x01`

The actual encoding/decoding methods are implemented in Phase 4 (SSTable), but the tag constants are defined here to keep the format definition co-located with the type.

---

## Tests

**File:** `crates/flushdb-types/tests/entry_value_tests.rs`

| Test | What It Validates |
|------|-------------------|
| `test_inline_creation` | `Inline(bytes)` stores the value |
| `test_blob_ref_creation` | `BlobRef { blob_id, offset, size }` stores all fields |
| `test_is_inline` | Returns true for Inline, false for BlobRef |
| `test_is_blob_ref` | Returns true for BlobRef, false for Inline |
| `test_inline_value_accessor` | Returns `Some` for Inline, `None` for BlobRef |
| `test_as_inline_consumes` | Consumes and returns Bytes for Inline |
| `test_inline_size` | Returns correct byte count |
| `test_inline_empty_value` | `Inline(Bytes::new())` — empty value is valid (tombstone payload) |
| `test_blob_ref_zero_offset` | offset=0 is valid (first entry in blob file) |
| `test_clone_inline` | Cloning preserves value |
| `test_clone_blob_ref` | Cloning preserves all fields |
| `test_tag_constants` | `INLINE_TAG == 0x00`, `BLOB_REF_TAG == 0x01` |

---

## Done When

- [ ] Both variants defined with correct fields
- [ ] Accessor methods work correctly
- [ ] Tag constants defined for future SSTable encoding
- [ ] `BlobRef` variant is fully specified (not a placeholder)
- [ ] All tests pass
