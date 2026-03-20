# Task 1: SSTable Types & Constants

**Crate:** `flushdb-engine`
**Files:** `src/sstable/types.rs`, `src/sstable/varint.rs`, `src/sstable/hash.rs`
**Depends on:** Nothing (uses `flushdb-types` for `FlushError`, `FlushResult`)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §7.1, §7.2

---

## Goal

Define the foundational types, constants, and encoding utilities that every SSTable component depends on. `CompressionType` determines block encoding, varint utilities provide compact integer encoding for the block entry format, `MurmurHash3` provides hashing for bloom filters and dedup blocks, and `SstConfig` centralizes SSTable build parameters.

---

## What to Build

### 1.1 CompressionType Enum

```
CompressionType {
    None = 0,
    Snappy = 1,
    Zstd = 2,
}
```

**Derives:** `Copy`, `Clone`, `Debug`, `PartialEq`, `Eq`

| Method | Signature | Behavior |
|--------|-----------|----------|
| `from_u8` | `(value: u8) -> FlushResult<Self>` | Returns variant or `FlushError::CorruptedData { message: "unknown compression type: {value}" }` for unknown discriminant |
| `as_u8` | `(&self) -> u8` | Returns discriminant value |

### 1.2 SSTable Constants

| Constant | Type | Value | Purpose |
|----------|------|-------|---------|
| `SSTABLE_MAGIC` | `u32` | `0x464C4442` | "FLDB" — file format identification |
| `FORMAT_VERSION` | `u16` | `1` | Current format version |
| `DEFAULT_BLOCK_SIZE` | `usize` | `4096` | 4 KB target block size |
| `FOOTER_SIZE` | `usize` | `80` | Fixed footer size in bytes |
| `HEADER_SIZE` | `usize` | `16` | Fixed header size in bytes |
| `MIN_MAX_KEY_TRUNCATION_LEN` | `usize` | `16` | Footer min/max key truncation length |
| `DEFAULT_BLOOM_BITS_PER_KEY` | `u32` | `10` | ~1% false positive rate |
| `DEFAULT_BLOOM_HASH_COUNT` | `u32` | `7` | Optimal k for 10 bits/key |
| `DEDUP_HASH_SIZE` | `usize` | `16` | 128-bit hash per dedup token |

### 1.3 SstConfig Struct

```
SstConfig {
    block_size_target: usize,          // default DEFAULT_BLOCK_SIZE (4096)
    compression: CompressionType,      // default CompressionType::Snappy
    bloom_bits_per_key: u32,           // default DEFAULT_BLOOM_BITS_PER_KEY (10)
}
```

**Derives:** `Clone`, `Debug`

| Method | Signature | Behavior |
|--------|-----------|----------|
| `default` | `() -> Self` | Returns config with default values |
| `with_block_size` | `(self, size: usize) -> Self` | Builder pattern — sets block_size_target |
| `with_compression` | `(self, compression: CompressionType) -> Self` | Builder pattern — sets compression |
| `with_bloom_bits_per_key` | `(self, bits: u32) -> Self` | Builder pattern — sets bloom_bits_per_key |

### 1.4 Varint Encoding (`varint.rs`)

Variable-length unsigned integer encoding (LEB128):
- Values 0–127: 1 byte (high bit clear)
- Values 128–16383: 2 bytes
- Up to 10 bytes for u64::MAX

Each byte stores 7 data bits. The high bit indicates whether more bytes follow (1 = more, 0 = final).

| Function | Signature | Behavior |
|----------|-----------|----------|
| `encode_varint` | `(value: u64, buf: &mut Vec<u8>)` | Appends LEB128 bytes to buffer |
| `decode_varint` | `(buf: &[u8]) -> FlushResult<(u64, usize)>` | Returns `(value, bytes_consumed)`. Returns `FlushError::CorruptedData` if buffer too short or varint exceeds 10 bytes |
| `varint_len` | `(value: u64) -> usize` | Returns encoded byte count without writing |

### 1.5 MurmurHash3 Utility (`hash.rs`)

Safe Rust implementation of MurmurHash3 x64 128-bit variant. Used by bloom filter (Task 5) for double-hashing and dedup block (Task 6) for token hashing.

| Function | Signature | Behavior |
|----------|-----------|----------|
| `murmurhash3_x64_128` | `(data: &[u8], seed: u32) -> (u64, u64)` | Returns 128-bit hash as two `u64` values (h1, h2) |

**Design decision:** Implemented in-crate as a private module (`hash.rs` not re-exported) rather than adding an external dependency. MurmurHash3 is a well-documented algorithm (~50 lines of safe Rust) and avoids an unlisted dependency.

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_types_tests.rs`

### Varint Round-trip Tests
| Test | What It Validates |
|------|-------------------|
| `test_varint_zero` | Encode/decode 0 → 1 byte, value preserved |
| `test_varint_single_byte_max` | Encode/decode 127 → 1 byte |
| `test_varint_two_byte_min` | Encode/decode 128 → 2 bytes |
| `test_varint_medium_value` | Encode/decode 300 → 2 bytes |
| `test_varint_u32_max` | Encode/decode `u32::MAX` → correct byte count |
| `test_varint_u64_max` | Encode/decode `u64::MAX` → 10 bytes |
| `test_varint_len_matches_encode` | `varint_len(v)` == actual encoded length for 0, 1, 127, 128, 300, u32::MAX, u64::MAX |
| `test_varint_decode_empty_buffer` | Empty slice → `CorruptedData` error |
| `test_varint_decode_truncated` | Truncated multi-byte varint → `CorruptedData` error |

### CompressionType Tests
| Test | What It Validates |
|------|-------------------|
| `test_compression_type_round_trip` | All three variants: `from_u8(as_u8()) == original` |
| `test_compression_type_invalid` | `from_u8(3)` → `CorruptedData` error |
| `test_compression_type_discriminants` | None=0, Snappy=1, Zstd=2 — explicit check |

### Config Tests
| Test | What It Validates |
|------|-------------------|
| `test_config_defaults` | Default values: block_size=4096, compression=Snappy, bloom_bits=10 |
| `test_config_builder_pattern` | `with_block_size` and `with_compression` chain correctly |

### MurmurHash3 Tests
| Test | What It Validates |
|------|-------------------|
| `test_murmurhash3_deterministic` | Same input + seed → same output on repeated calls |
| `test_murmurhash3_different_seeds` | Same input, different seeds → different output |
| `test_murmurhash3_empty_input` | Empty slice doesn't panic, returns consistent hash |
| `test_murmurhash3_avalanche` | Single bit change in input produces significantly different hash (check at least 32 bits differ) |

---

## Done When

- [ ] `CompressionType` round-trips through `u8` for all three variants
- [ ] Unknown discriminant produces `CorruptedData` error
- [ ] Varint encode/decode is lossless for all u64 values including boundaries
- [ ] Varint decode rejects truncated and empty input
- [ ] `varint_len` matches actual encoded length
- [ ] MurmurHash3 produces deterministic, well-distributed 128-bit hashes
- [ ] `SstConfig` defaults match documented constant values
- [ ] All tests pass
