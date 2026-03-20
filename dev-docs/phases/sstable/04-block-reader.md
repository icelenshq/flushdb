# Task 4: BlockReader — Data Block Decoding

**Crate:** `flushdb-engine`
**File:** `src/sstable/block_reader.rs`
**Depends on:** Task 1 (CompressionType, varint, constants), Task 3 (block encoding format)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §7.2, Phase 4 §2.2

---

## Goal

Read and decode a compressed data block, validate its CRC32 integrity, and iterate over entries with record_id carry-forward. The reader handles both `EntryValue::Inline` and `EntryValue::BlobRef` parsing for forward compatibility — even though Phase 4 only writes Inline values.

---

## What to Build

### 4.1 BlockEntry Struct

Represents a single decoded entry from a data block:

```
BlockEntry {
    composite_key: CompositeKey,
    value: EntryValue,              // Inline or BlobRef
    metadata: Bytes,
    entry_type: EntryType,
    sequence_number: u64,
}
```

**Derives:** `Debug`, `Clone`

### 4.2 decode_block Function

| Function | Signature | Behavior |
|----------|-----------|----------|
| `decode_block` | `(data: &[u8], compression: CompressionType) -> FlushResult<Vec<BlockEntry>>` | Decompress → validate CRC32 → parse all entries → return vec. Convenience wrapper over `BlockEntryIterator`. |

### 4.3 BlockEntryIterator

For memory-efficient iteration without materializing all entries at once:

```
BlockEntryIterator {
    data: Bytes,                     // decompressed block data (CRC stripped)
    offset: usize,
    last_record_id: Option<Bytes>,   // carry-forward for record_id dedup
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(raw_block: &[u8], compression: CompressionType) -> FlushResult<Self>` | Decompress, validate CRC32, strip CRC (last 4 bytes), initialize iterator at offset 0 |

Implements `Iterator<Item = FlushResult<BlockEntry>>`.

Each `next()` call:
1. If `offset >= data.len()`, return `None`
2. Decode `record_id_len` varint
3. If `record_id_len == 0`, use `last_record_id` (error if None — first entry must have full record_id)
4. Otherwise read `record_id_len` bytes as record_id, update `last_record_id`
5. Decode `item_key_len` varint, read item_key bytes
6. Construct `CompositeKey::from_bytes(...)` from record_id + separator + item_key
7. Decode `value_tag` (1 byte), `value_data_len` varint, read value_data bytes
8. Parse `EntryValue` from tag + data (see §4.5)
9. Decode `metadata_len` varint, read metadata bytes
10. Decode `entry_type` (1 byte) via `EntryType::from_u8()`
11. Decode `sequence_number` varint
12. Return `BlockEntry`

### 4.4 Decompression

| Type | Method | Notes |
|------|--------|-------|
| None | identity (no-op) | Raw bytes used directly |
| Snappy | `snap::raw::Decoder::decompress_vec()` | Use `snap::raw::decompress_len()` for output size hint |
| Zstd | `zstd::stream::decode_all()` | Reads from `&[u8]` as `Read`, auto-detects frame size |

Decompression failures → `FlushError::CorruptedData { message: "decompression failed: {details}" }`.

### 4.5 CRC32 Validation

After decompression, the decompressed buffer has the structure: `[entry_data...][crc32: 4 bytes LE]`.

1. Split: `payload = decompressed[..len-4]`, `stored_crc = u32 from decompressed[len-4..len]`
2. Compute: `computed_crc = crc32fast::hash(payload)`
3. If `stored_crc != computed_crc` → `FlushError::CrcMismatch { expected: stored_crc, actual: computed_crc }`
4. Strip CRC bytes — iterator operates on `payload` only

### 4.6 EntryValue Parsing

The value field in a block entry is encoded as:

```
[value_tag: u8]           // 0x00 = Inline, 0x01 = BlobRef
[value_data_len: varint]
[value_data: bytes]
```

| Tag | Parsing | Result |
|-----|---------|--------|
| `0x00` (INLINE_TAG) | Read `value_data` as raw bytes | `EntryValue::Inline(Bytes::copy_from_slice(value_data))` |
| `0x01` (BLOB_REF_TAG) | Parse from `value_data`: `[blob_id_len: u16 LE][blob_id: bytes][offset: u64 LE][size: u32 LE]` | `EntryValue::BlobRef { blob_id, offset, size }` |
| Other | — | `FlushError::CorruptedData { message: "unknown entry value tag: {tag}" }` |

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_block_reader_tests.rs`

### Round-trip Tests (Builder → Reader)
| Test | What It Validates |
|------|-------------------|
| `test_round_trip_single_entry` | Build block with 1 Put entry, decode — composite_key, value, metadata, entry_type, sequence_number all match |
| `test_round_trip_multiple_entries` | Build block with 10 entries, decode all — field-by-field comparison for each |
| `test_round_trip_all_entry_types` | Put, Delete, RangeDelete entries survive encode/decode with correct entry_type |
| `test_round_trip_empty_value` | DELETE entry with empty value round-trips as `EntryValue::Inline(empty Bytes)` |
| `test_round_trip_empty_metadata` | Entry with empty metadata round-trips |
| `test_round_trip_large_value` | Entry with 32KB value round-trips correctly |

### Record ID Carry-forward Tests
| Test | What It Validates |
|------|-------------------|
| `test_record_id_dedup_carry_forward` | 5 entries with same record_id — all decoded entries have correct `composite_key.record_id()` |
| `test_record_id_changes_mid_block` | Entries with record_ids [A, A, B, B, A] — each decoded entry has correct record_id |
| `test_first_entry_always_has_record_id` | First decoded entry always has correct record_id (never relies on carry-forward) |

### Compression Round-trip Tests
| Test | What It Validates |
|------|-------------------|
| `test_round_trip_compression_none` | Uncompressed block: build → decode → all entries match |
| `test_round_trip_compression_snappy` | Snappy block: build → decode → all entries match |
| `test_round_trip_compression_zstd` | Zstd block: build → decode → all entries match |

### CRC Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_crc_corruption_detected` | Build uncompressed block, flip one byte in the data portion → `CrcMismatch` error on decode |
| `test_crc_valid_for_good_block` | Unmodified block passes CRC check without error |

### EntryValue Parsing Tests
| Test | What It Validates |
|------|-------------------|
| `test_inline_value_parsed` | Standard entry's value returns `EntryValue::Inline` with correct bytes |
| `test_blob_ref_value_parsed` | Manually construct raw block bytes with BlobRef tag (0x01) + blob_id + offset + size → decoded as `EntryValue::BlobRef` with matching fields |

### Iterator Tests
| Test | What It Validates |
|------|-------------------|
| `test_iterator_yields_all_entries` | Iterator produces exactly `entry_count` items |
| `test_iterator_empty_after_exhaustion` | After all entries consumed, `next()` returns `None` repeatedly |

---

## Done When

- [ ] `BlockEntryIterator` decodes all entries written by `BlockBuilder`
- [ ] Record_id carry-forward correctly restores record_id for dedup'd entries
- [ ] All three compression types decompress correctly
- [ ] CRC32 corruption is detected with `CrcMismatch` error
- [ ] `EntryValue::Inline` parsed correctly from tag 0x00
- [ ] `EntryValue::BlobRef` parsed correctly from tag 0x01 (forward compat)
- [ ] Unknown value tag produces `CorruptedData` error
- [ ] Iterator exhaustion returns `None`
- [ ] All tests pass
