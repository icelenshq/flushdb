# Task 3: BlockBuilder — Data Block Construction

**Crate:** `flushdb-engine`
**File:** `src/sstable/block_builder.rs`
**Depends on:** Task 1 (CompressionType, varint, constants)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §7.2, Phase 4 §2.1

---

## Goal

Build data blocks incrementally as entries are added in sorted order. Each block is independently compressed with CRC32 integrity, uses record ID deduplication for consecutive same-record entries, and tracks metadata (first/last key, record IDs, idempotency tokens) for downstream consumers (bloom filter, dedup block, index block).

---

## What to Build

### 3.1 FinishedBlock Struct

```
FinishedBlock {
    data: Bytes,                            // compressed block bytes (includes CRC before compression)
    first_key: CompositeKey,
    last_key: CompositeKey,
    entry_count: u32,
    uncompressed_size: u32,                 // buffer size before compression (including CRC)
    record_ids: HashSet<Bytes>,             // unique record_ids in this block
    idempotency_tokens: Vec<IdempotencyToken>, // non-none tokens for dedup block
}
```

**Derives:** `Debug`

### 3.2 BlockBuilder Struct

```
BlockBuilder {
    buffer: Vec<u8>,                        // uncompressed entry data
    entry_count: u32,
    block_size_target: usize,
    first_key: Option<CompositeKey>,
    last_key: Option<CompositeKey>,
    record_ids_seen: HashSet<Bytes>,
    idempotency_tokens: Vec<IdempotencyToken>,
    last_record_id: Option<Bytes>,          // for record_id dedup
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(block_size_target: usize) -> Self` | Creates empty builder with given target size |
| `add_entry` | `(&mut self, key: &CompositeKey, value: &[u8], metadata: &[u8], entry_type: EntryType, sequence_number: u64, idempotency_token: IdempotencyToken)` | Encodes entry into buffer using varint format. If `key.record_id()` matches `last_record_id`, writes `record_id_len=0` (dedup). Updates first/last key, record_ids, tokens. |
| `is_full` | `(&self) -> bool` | Returns `self.buffer.len() >= self.block_size_target` |
| `is_empty` | `(&self) -> bool` | Returns `self.entry_count == 0` |
| `finish` | `(self, compression: CompressionType) -> FlushResult<FinishedBlock>` | Appends CRC32 (4 bytes LE) to buffer, compresses, returns `FinishedBlock`. Returns `FlushError::InvalidArgument` if empty. |
| `estimated_size` | `(&self) -> usize` | Returns current buffer length |
| `entry_count` | `(&self) -> u32` | Returns entry count |
| `reset` | `(&mut self)` | Clears buffer, counters, first/last key, record_ids, tokens, last_record_id — ready for reuse |

### 3.3 Entry Encoding Format

Each entry appended to the buffer:

```
[record_id_len: varint]   // 0 means "same as previous entry's record_id"
[record_id: bytes]        // omitted when record_id_len == 0
[item_key_len: varint]
[item_key: bytes]
[value_tag: u8]           // EntryValue discriminant: 0x00=Inline, 0x01=BlobRef
[value_data_len: varint]  // length of value data bytes (after tag)
[value_data: bytes]
[metadata_len: varint]
[metadata: bytes]
[entry_type: u8]          // 0=Put, 1=Delete, 2=RangeDelete
[sequence_number: varint]
```

The value is encoded with an `EntryValue::Inline` tag:
- `value_tag` = `0x00` (EntryValue::INLINE_TAG)
- `value_data_len` = length of raw value bytes
- `value_data` = raw value bytes

This format allows the BlockReader to distinguish Inline from BlobRef without format migration when value separation is added.

### 3.4 Record ID Deduplication

- The **first entry in a block** ALWAYS writes the full `record_id` regardless of `last_record_id`
- Subsequent entries: if `key.record_id() == last_record_id`, write `record_id_len = 0` and omit record_id bytes
- `last_record_id` is updated after each entry
- Empty record_ids are forbidden at the API layer (enforced by `CompositeKey` validation), making `record_id_len = 0` an unambiguous dedup signal
- Can reduce block size by 30-50% for wide records

### 3.5 Block Finalization

1. Compute CRC32 over the entire uncompressed buffer
2. Append CRC32 as 4 bytes (little-endian) to buffer
3. Compress the buffer+CRC based on compression type:
   - **None:** no transformation — buffer used as-is
   - **Snappy:** `snap::raw::Encoder::compress_vec(&buffer)` — fast, ~2:1 ratio
   - **Zstd:** `zstd::bulk::compress(&buffer, 3)` — better ratio, default level 3
4. Wrap compressed result in `Bytes`
5. Return `FinishedBlock` with all tracked metadata

Compression errors are wrapped as `FlushError::Io`.

### 3.6 Metadata Tracking

During `add_entry`:
- `record_ids_seen`: insert `Bytes::copy_from_slice(key.record_id())` for every entry (ALL types: Put, Delete, RangeDelete)
- `idempotency_tokens`: push token only if `!token.is_none()`
- `first_key`: set on first entry only
- `last_key`: updated on every entry

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_block_builder_tests.rs`

### Basic Building Tests
| Test | What It Validates |
|------|-------------------|
| `test_single_entry_block` | Add one entry, finish → `FinishedBlock` has entry_count=1, correct first/last key |
| `test_multiple_entries_block` | Add 5 entries with different keys, entry_count=5, first_key is first added, last_key is last added |
| `test_empty_block_finish_errors` | `finish()` on empty builder → `InvalidArgument` error |
| `test_entry_with_empty_value` | DELETE entry with `value = &[]` encodes correctly |
| `test_entry_with_empty_metadata` | Entry with `metadata = &[]` encodes correctly |
| `test_all_entry_types` | Put, Delete, RangeDelete entries all encode without error |

### Record ID Dedup Tests
| Test | What It Validates |
|------|-------------------|
| `test_first_entry_always_full_record_id` | First entry has non-zero record_id_len in encoded bytes |
| `test_consecutive_same_record_dedup` | Second entry with same record_id stores `record_id_len=0` — verify by checking buffer size is smaller than two full entries |
| `test_different_record_id_resets_dedup` | Entry with different record_id stores full record_id — buffer size reflects two full record_ids |
| `test_dedup_reduces_block_size` | Block with 10 same-record entries is measurably smaller than block with 10 distinct-record entries of same value sizes |

### Block Size & Rotation Tests
| Test | What It Validates |
|------|-------------------|
| `test_is_full_at_target` | Add entries until `buffer.len() >= target`, `is_full()` returns true |
| `test_is_full_below_target` | Single small entry, `is_full()` returns false |
| `test_estimated_size_grows` | `estimated_size()` increases monotonically with each `add_entry` |

### Compression Tests
| Test | What It Validates |
|------|-------------------|
| `test_finish_compression_none` | Uncompressed: `FinishedBlock.data.len() == buffer.len() + 4` (CRC appended, no compression) |
| `test_finish_compression_snappy` | Snappy-compressed: data can be decompressed with `snap::raw::Decoder` and last 4 bytes of decompressed match CRC32 of preceding bytes |
| `test_finish_compression_zstd` | Zstd-compressed: data can be decompressed with `zstd::bulk::decompress` and CRC validates |

### Metadata Tracking Tests
| Test | What It Validates |
|------|-------------------|
| `test_record_ids_collected` | 5 entries with 3 distinct record_ids → `record_ids.len() == 3` |
| `test_idempotency_tokens_collected` | 3 entries with tokens + 2 with none → `idempotency_tokens.len() == 3` |
| `test_reset_clears_state` | After `reset()`, `is_empty() == true`, `estimated_size() == 0`, `entry_count() == 0`, record_ids and tokens empty |

---

## Done When

- [ ] Entries encode with varint lengths and record_id dedup within blocks
- [ ] First entry per block always includes full record_id
- [ ] CRC32 appended (LE) before compression
- [ ] All three compression types produce valid output that can be decompressed
- [ ] `FinishedBlock` tracks correct first/last key, entry_count, record_ids, tokens
- [ ] `is_full()` triggers at `block_size_target`
- [ ] `reset()` clears all state for builder reuse
- [ ] All tests pass
