# Task 3: Segment Header

**Crate:** `flushdb-wal`
**File:** `src/segment_header.rs`
**Depends on:** Task 1 (WAL_MAGIC, WAL_VERSION, SEGMENT_HEADER_SIZE constants)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §5.1; phase-2-wal.md §1, Future Work Considerations

---

## Goal

Define a self-contained segment header that makes each WAL segment independently parseable without state from prior segments. This is a future-proofing requirement — cluster mode will replicate individual segments to followers, so each segment must carry its own metadata.

---

## What to Build

### 3.1 SegmentHeader Struct

| Field | Type | Description |
|-------|------|-------------|
| `segment_number` | `u64` | Monotonically increasing segment ID, matches filename |
| `starting_sequence_number` | `u64` | First sequence number that may appear in this segment |
| `created_at_ms` | `u64` | Millisecond timestamp (since Unix epoch) when segment was created |

Derives: `Debug`, `Clone`, `PartialEq`, `Eq`

### 3.2 Wire Format

Fixed 32 bytes, all multi-byte integers little-endian:

```
[offset 0..4]    magic: [u8; 4] = b"FWAL"
[offset 4]       version: u8 = 1
[offset 5]       flags: u8 = 0 (reserved for future use)
[offset 6..8]    reserved: [u8; 2] = [0, 0]
[offset 8..16]   segment_number: u64 LE
[offset 16..24]  starting_sequence_number: u64 LE
[offset 24..32]  created_at_ms: u64 LE
```

Total: 32 bytes (naturally aligned for efficient I/O).

**Design decisions:**
- `flags` and `reserved` fields provide forward compatibility — future versions can use these without changing header size
- `version` allows detecting incompatible format changes
- `segment_number` in the header allows verification against the filename (detect renamed/copied segments)
- 32-byte alignment means the first WAL entry starts at an aligned offset

### 3.3 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(segment_number: u64, starting_sequence_number: u64) -> Self` | Creates header with current timestamp (`SystemTime::now()` → millis since epoch) |
| `encode` | `(&self) -> [u8; 32]` | Serializes to 32-byte array with magic, version, flags=0, reserved=0, then fields in LE |
| `decode` | `(data: &[u8]) -> FlushResult<Self>` | Parses from byte slice. Validates magic and version. Tolerates nonzero flags/reserved (forward compat). |

### 3.4 Validation Rules

| Check | Error |
|-------|-------|
| `data.len() < 32` | `FlushError::CorruptedData { context: "segment header too short" }` |
| Magic bytes ≠ `b"FWAL"` | `FlushError::CorruptedData { context: "invalid WAL segment magic" }` |
| Version ≠ 1 | `FlushError::CorruptedData { context: "unsupported WAL segment version: {v}" }` |

---

## Tests

**File:** `crates/flushdb-wal/tests/segment_header_tests.rs`

### Round-trip Tests
| Test | What It Validates |
|------|-------------------|
| `test_encode_decode_roundtrip` | Encode → decode preserves segment_number, starting_sequence_number, created_at_ms |
| `test_encode_decode_zero_values` | All fields zero round-trips correctly |
| `test_encode_decode_max_values` | All fields `u64::MAX` round-trips correctly |

### Wire Format Tests
| Test | What It Validates |
|------|-------------------|
| `test_encoded_size_is_32_bytes` | `encode().len() == 32` |
| `test_magic_bytes_at_offset_0` | First 4 bytes of encoded output are `b"FWAL"` |
| `test_version_at_offset_4` | Byte at offset 4 is `1` |
| `test_segment_number_little_endian` | Known segment_number encodes at correct offset in LE byte order |

### Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_decode_rejects_short_data` | 31 bytes → CorruptedData |
| `test_decode_rejects_wrong_magic` | `b"XWAL"` → CorruptedData |
| `test_decode_rejects_wrong_version` | Version = 2 → CorruptedData |
| `test_decode_accepts_nonzero_flags` | Flags byte = 0xFF → succeeds (forward compatibility) |
| `test_decode_accepts_nonzero_reserved` | Reserved bytes nonzero → succeeds (forward compatibility) |

---

## Done When

- [ ] SegmentHeader encodes to exactly 32 bytes
- [ ] Magic bytes and version are validated on decode
- [ ] Unknown flags/reserved bytes are tolerated (forward compatibility)
- [ ] Round-trip encode/decode is lossless
- [ ] All tests pass
