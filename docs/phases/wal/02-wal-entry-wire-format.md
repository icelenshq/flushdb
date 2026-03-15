# Task 2: WAL Entry Wire Format

**Crate:** `flushdb-wal`
**File:** `src/entry.rs`
**Depends on:** Task 1 (constants)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §5.2; phase-2-wal.md §2

---

## Goal

Implement the WAL entry binary encoding and decoding with CRC32 integrity validation. This is the wire format that goes to disk — correctness here is critical for crash recovery. Every byte offset, length prefix, and CRC scope must match the specification exactly.

---

## What to Build

### 2.1 WalEntry Struct

The in-memory representation of a single WAL entry:

| Field | Type | Description |
|-------|------|-------------|
| `sequence_number` | `u64` | Monotonically increasing, assigned by WAL writer |
| `entry_type` | `EntryType` | Put, Delete, or RangeDelete (from flushdb-types) |
| `namespace` | `Bytes` | Namespace this entry belongs to |
| `record_id` | `Bytes` | Record ID (partition key) |
| `item_key` | `Bytes` | Item key within the record |
| `item_value` | `Bytes` | Item value (empty for Delete) |
| `item_metadata` | `Bytes` | Optional metadata |
| `idempotency_token` | `IdempotencyToken` | 24-byte deduplication token (from flushdb-types) |

Derives: `Debug`, `Clone`, `PartialEq`

### 2.2 Wire Format

All multi-byte integers are **little-endian**.

```
[offset 0..4]      entry_length: u32 LE — body size in bytes (sequence_number through idempotency_token)
--- body starts ---
[offset 4..12]     sequence_number: u64 LE
[offset 12]        entry_type: u8 (0=PUT, 1=DELETE, 2=RANGE_DELETE)
[offset 13..15]    namespace_len: u16 LE
[offset 15..]      namespace: bytes (namespace_len bytes)
[..]               record_id_len: u16 LE
[..]               record_id: bytes (record_id_len bytes)
[..]               item_key_len: u16 LE
[..]               item_key: bytes (item_key_len bytes)
[..]               item_value_len: u32 LE
[..]               item_value: bytes (item_value_len bytes)
[..]               item_metadata_len: u16 LE
[..]               item_metadata: bytes (item_metadata_len bytes)
[..]               idempotency_token: 24 bytes (fixed size)
--- body ends ---
[..]               crc32: u32 LE — CRC32 over all body bytes
```

**CRC scope:** The CRC32 (using `crc32fast`) covers all body bytes — from `sequence_number` through `idempotency_token`. It does NOT include `entry_length` or the CRC itself.

**entry_length semantics:** The value is the body size only. The reader reads 4 bytes for entry_length, then `entry_length` bytes for the body, then 4 bytes for the CRC. Total on-disk entry size = `4 + entry_length + 4`.

**Recovery parsing uses this layout:**
1. Read `entry_length` (4 bytes)
2. Read `entry_length + 4` bytes (body + CRC)
3. Validate CRC over body
4. If valid: parse body into WalEntry

### 2.3 Encoding Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `encode` | `(&self) -> Bytes` | Serializes to the complete wire format: entry_length + body + CRC. Returns all on-disk bytes. |
| `body_size` | `(&self) -> usize` | Calculates body size: `8 + 1 + 2 + namespace.len() + 2 + record_id.len() + 2 + item_key.len() + 4 + item_value.len() + 2 + item_metadata.len() + 24` |
| `total_size` | `(&self) -> usize` | Full on-disk footprint: `4 + body_size() + 4` |

### 2.4 Decoding Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `decode_body` | `(body: &[u8]) -> FlushResult<Self>` | Parses a WalEntry from body bytes (after entry_length, before CRC). CRC already validated by caller. |
| `validate_crc` | `(body: &[u8], expected_crc: u32) -> FlushResult<()>` | Computes CRC32 over body bytes and compares to expected. Returns `FlushError::CrcMismatch` on failure. |
| `read_entry_length` | `(data: &[u8]) -> FlushResult<u32>` | Reads first 4 bytes as LE u32. Returns error if fewer than 4 bytes available. |

### 2.5 Conversion Helpers

| Method | Signature | Behavior |
|--------|-----------|----------|
| `from_memtable_entry` | `(entry: &MemtableEntry, namespace: &[u8]) -> Self` | Creates WalEntry from MemtableEntry + namespace. Extracts record_id and item_key from CompositeKey. Extracts value bytes from EntryValue::Inline. Copies sequence_number, entry_type, idempotency_token. |
| `to_memtable_entry` | `(&self) -> FlushResult<MemtableEntry>` | Reconstructs MemtableEntry. Creates CompositeKey from record_id + item_key, wraps value in EntryValue::Inline. |

### 2.6 Validation Rules

| Check | Error |
|-------|-------|
| `data.len() < 4` in read_entry_length | `FlushError::CorruptedData { context: "WAL entry too short for length prefix" }` |
| Body shorter than minimum fixed-field size | `FlushError::CorruptedData { context: "WAL entry body too short" }` |
| Variable field length exceeds remaining body | `FlushError::CorruptedData { context: "WAL entry field length exceeds body" }` |
| CRC mismatch | `FlushError::CrcMismatch { expected, actual }` |
| Unknown entry_type discriminant | `FlushError::CorruptedData { context: "unknown WAL entry type: {value}" }` |

---

## Tests

**File:** `crates/flushdb-wal/tests/entry_tests.rs`

### Round-trip Tests
| Test | What It Validates |
|------|-------------------|
| `test_encode_decode_put_entry` | Encode a PUT entry, decode body, all fields match |
| `test_encode_decode_delete_entry` | DELETE entry with empty value round-trips correctly |
| `test_encode_decode_range_delete_entry` | RANGE_DELETE entry round-trips correctly |
| `test_encode_decode_empty_fields` | Entry with empty namespace, item_key, metadata |
| `test_encode_decode_large_value` | Entry with 1MB item_value round-trips correctly |
| `test_encode_decode_binary_data` | Fields containing all 256 byte values (non-UTF8) |
| `test_encode_decode_max_size_fields` | Fields at maximum lengths (256-byte record_id, 4096-byte item_key) |

### CRC Tests
| Test | What It Validates |
|------|-------------------|
| `test_crc_validates_for_valid_entry` | Encoded entry's CRC validates correctly |
| `test_crc_detects_corrupted_sequence_number` | Flip a bit in sequence_number → CrcMismatch |
| `test_crc_detects_corrupted_value` | Flip a byte in item_value → CrcMismatch |
| `test_crc_detects_corrupted_namespace` | Flip a byte in namespace → CrcMismatch |
| `test_crc_covers_all_body_bytes` | Corruption at any position in the body is detected |

### Size Calculation Tests
| Test | What It Validates |
|------|-------------------|
| `test_body_size_empty_variable_fields` | Entry with all empty variable fields → body_size matches fixed overhead (8+1+2+2+2+4+2+24 = 45) |
| `test_body_size_with_variable_fields` | body_size accounts for all variable field lengths |
| `test_total_size_includes_length_and_crc` | `total_size() == body_size() + 8` |
| `test_encoded_bytes_length_matches_total_size` | `encode().len() == total_size()` |

### Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_decode_rejects_truncated_body` | Body shorter than fixed field requirements → CorruptedData |
| `test_decode_rejects_unknown_entry_type` | entry_type = 42 → CorruptedData |
| `test_read_entry_length_too_short` | Fewer than 4 bytes → CorruptedData |
| `test_decode_rejects_field_length_overflow` | Field length that would read past body end → CorruptedData |

### Conversion Tests
| Test | What It Validates |
|------|-------------------|
| `test_from_memtable_entry_extracts_fields` | WalEntry from MemtableEntry has correct record_id, item_key, value |
| `test_to_memtable_entry_reconstructs_correctly` | MemtableEntry → WalEntry → MemtableEntry preserves all fields |
| `test_roundtrip_memtable_entry_preserves_sequence` | Sequence number survives conversion round-trip |

---

## Done When

- [ ] WalEntry encodes to exact wire format from STORAGE_DESIGN.md §5.2
- [ ] All multi-byte integers are little-endian
- [ ] CRC32 covers correct byte range (body only, not entry_length or CRC itself)
- [ ] Round-trip encode/decode is lossless for all entry types
- [ ] CRC validation detects single-bit corruption anywhere in the body
- [ ] Conversion to/from MemtableEntry is correct
- [ ] All tests pass
