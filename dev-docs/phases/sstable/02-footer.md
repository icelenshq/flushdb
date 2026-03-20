# Task 2: Footer — SSTable Trailer

**Crate:** `flushdb-engine`
**File:** `src/sstable/footer.rs`
**Depends on:** Task 1 (CompressionType, constants)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §7.5, Phase 4 §6

---

## Goal

Implement the fixed-size 80-byte SSTable footer and 16-byte header. The footer is the entry point for all SSTable reads — the reader fetches the last 80 bytes first, validates magic and CRC, then uses the offsets to locate the bloom filter, index block, and dedup block. The header provides quick format identification at the file start.

---

## What to Build

### 2.1 SstFooter Struct

```
SstFooter {
    bloom_filter_offset: u64,
    bloom_filter_size: u32,
    index_block_offset: u64,
    index_block_size: u32,
    entry_count: u64,
    min_key: [u8; 16],
    max_key: [u8; 16],
    compression_type: CompressionType,
    format_version: u16,
    dedup_block_size: u32,
}
```

**Derives:** `Debug`, `Clone`, `PartialEq`, `Eq`

Note: `_padding` (offset 65), CRC32 (offset 68..72), and magic (offset 72..76) are computed/validated during encode/decode, not stored as fields.

**Design decision:** The `_reserved: [u8; 4]` from STORAGE_DESIGN.md is repurposed as `dedup_block_size: u32`. This enables 2-read dedup lookup (footer → dedup block) instead of requiring the index block to compute the dedup offset. The dedup block offset is derived as `bloom_filter_offset - dedup_block_size`. The LCP prefix optimization mentioned in STORAGE_DESIGN.md §7.5 can use the `_padding` byte if needed later.

**Binary layout (80 bytes, all little-endian):**
```
[0..8]    bloom_filter_offset: u64
[8..12]   bloom_filter_size: u32
[12..20]  index_block_offset: u64
[20..24]  index_block_size: u32
[24..32]  entry_count: u64
[32..48]  min_key: [u8; 16]
[48..64]  max_key: [u8; 16]
[64]      compression_type: u8
[65]      _padding: u8 (0x00)
[66..68]  format_version: u16
[68..72]  crc32: u32 (CRC over bytes 0..68)
[72..76]  magic: u32 (0x464C4442)
[76..80]  dedup_block_size: u32
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `encode` | `(&self) -> [u8; FOOTER_SIZE]` | Serializes all fields into 80 bytes. Computes CRC32 over bytes 0..68, writes magic at 72..76, dedup_block_size at 76..80 |
| `decode` | `(bytes: &[u8]) -> FlushResult<Self>` | Validates length == 80, checks magic at offset 72..76, validates CRC32 over 0..68, parses all fields. Returns `CorruptedData` on magic mismatch, `CrcMismatch` on CRC failure |
| `dedup_block_offset` | `(&self) -> u64` | Returns `self.bloom_filter_offset - self.dedup_block_size as u64` |
| `truncate_key` | `(key: &CompositeKey) -> [u8; 16]` | Copies first 16 bytes of `key.as_bytes()`, zero-pads if shorter than 16 bytes |
| `may_contain_key` | `(&self, key: &CompositeKey) -> bool` | Returns true if the truncated key falls within `[min_key, max_key]` inclusive range. Can produce false positives (truncation) but never false negatives |

### 2.2 SstHeader Struct

```
SstHeader {
    compression: CompressionType,
    entry_count: u64,
}
```

**Derives:** `Debug`, `Clone`, `PartialEq`, `Eq`

Magic and format_version are written as constants during encode, not configurable.

**Binary layout (16 bytes, little-endian):**
```
[0..4]    magic: u32 (0x464C4442)
[4..6]    format_version: u16
[6]       compression_type: u8
[7]       _padding: u8 (0x00)
[8..16]   entry_count: u64
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `encode` | `(&self) -> [u8; HEADER_SIZE]` | Serializes header with magic and format_version constants |
| `decode` | `(bytes: &[u8]) -> FlushResult<Self>` | Validates length == 16, checks magic, parses compression and entry_count |

### 2.3 Validation Rules

| Check | Error |
|-------|-------|
| Footer bytes length != 80 | `FlushError::CorruptedData { message: "footer must be exactly 80 bytes" }` |
| Footer magic != 0x464C4442 | `FlushError::CorruptedData { message: "invalid SSTable magic" }` |
| Footer CRC32 mismatch | `FlushError::CrcMismatch { expected, actual }` |
| Unknown compression type | `FlushError::CorruptedData { message: "unknown compression type: {value}" }` |
| Header bytes length != 16 | `FlushError::CorruptedData { message: "header must be exactly 16 bytes" }` |
| Header magic != 0x464C4442 | `FlushError::CorruptedData { message: "invalid SSTable header magic" }` |

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_footer_tests.rs`

### Round-trip Tests
| Test | What It Validates |
|------|-------------------|
| `test_footer_round_trip` | Encode → decode preserves all fields exactly |
| `test_footer_round_trip_all_compression_types` | Round-trip with None, Snappy, Zstd — each produces correct compression_type |
| `test_footer_encode_produces_80_bytes` | Output length is exactly 80 bytes |
| `test_header_round_trip` | Encode → decode preserves compression and entry_count |
| `test_header_encode_produces_16_bytes` | Output length is exactly 16 bytes |

### Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_footer_rejects_wrong_magic` | Modify magic bytes in encoded footer → `CorruptedData` |
| `test_footer_rejects_corrupted_crc` | Modify one data byte in encoded footer → `CrcMismatch` with correct expected/actual values |
| `test_footer_rejects_short_input` | 79 bytes → `CorruptedData` |
| `test_header_rejects_wrong_magic` | Modified magic → `CorruptedData` |

### Key Truncation Tests
| Test | What It Validates |
|------|-------------------|
| `test_truncate_short_key` | 5-byte key → 5 bytes + 11 zero-pad bytes |
| `test_truncate_exact_16_key` | 16-byte key → copied exactly, no padding |
| `test_truncate_long_key` | 100-byte key → first 16 bytes only |
| `test_may_contain_key_in_range` | Key with truncated form between min/max → true |
| `test_may_contain_key_outside_range` | Key with truncated form outside [min, max] → false |

### Derived Fields Tests
| Test | What It Validates |
|------|-------------------|
| `test_dedup_block_offset` | `dedup_block_offset()` == `bloom_filter_offset - dedup_block_size` for various field values |

---

## Done When

- [ ] Footer encode/decode is lossless for all field combinations
- [ ] Footer is exactly 80 bytes
- [ ] Header encode/decode is lossless
- [ ] Header is exactly 16 bytes
- [ ] Magic validation rejects wrong magic
- [ ] CRC validation catches single-byte corruption
- [ ] Key truncation zero-pads short keys and truncates long keys
- [ ] `may_contain_key` produces no false negatives
- [ ] `dedup_block_offset` computed correctly from footer fields
- [ ] All tests pass
