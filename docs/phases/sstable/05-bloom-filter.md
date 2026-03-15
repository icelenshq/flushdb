# Task 5: BloomFilter — Record ID Filter

**Crate:** `flushdb-engine`
**File:** `src/sstable/bloom_filter.rs`
**Depends on:** Task 1 (MurmurHash3, constants)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §7.3, Phase 4 §3

---

## Goal

Build and query a bloom filter over record_id values. The bloom filter is the first line of defense in point lookups — it eliminates SSTables that definitely don't contain the target record before any data block I/O. Must support serialization for embedding in the SSTable and independent loading for cache pinning (Phase 6). Wrapped in a `FilterBlock` enum to enable future Ribbon filter swap (Phase 5e).

---

## What to Build

### 5.1 FilterBlock Enum

Wrapper enum for future Ribbon filter support:

```
FilterBlock {
    Bloom(BloomFilter),
    // Future: Ribbon(RibbonFilter)
}
```

**Derives:** `Debug`

| Method | Signature | Behavior |
|--------|-----------|----------|
| `maybe_contains` | `(&self, record_id: &[u8]) -> bool` | Delegates to inner filter's `maybe_contains` |
| `serialize` | `(&self) -> Bytes` | Serializes with a 1-byte type prefix: `0x00` = Bloom. Prefix followed by inner filter serialization. |
| `deserialize` | `(data: &[u8]) -> FlushResult<Self>` | Reads 1-byte type prefix, dispatches to inner filter's `deserialize`. Unknown prefix → `CorruptedData` error. |

### 5.2 BloomFilterBuilder

```
BloomFilterBuilder {
    record_ids: HashSet<Bytes>,        // deduplicated record_ids
    bits_per_key: u32,                 // default 10
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(bits_per_key: u32) -> Self` | Creates builder with given bits/key ratio |
| `add` | `(&mut self, record_id: &[u8])` | Adds record_id to the set (deduplicates via HashSet) |
| `add_all` | `(&mut self, record_ids: &HashSet<Bytes>)` | Bulk add from a block's `record_ids` set |
| `build` | `(self) -> BloomFilter` | Computes optimal bit count and hash count, inserts all record_ids into bit array, returns filter |
| `estimated_size_bytes` | `(&self) -> usize` | Returns estimated filter size: `(record_ids.len() * bits_per_key) / 8` |

### 5.3 BloomFilter

```
BloomFilter {
    bits: Vec<u8>,                    // bit array as byte vector
    num_bits: u64,                    // total number of bits (= bits.len() * 8)
    num_hash_functions: u32,          // k
    num_keys: u64,                    // number of inserted keys (for diagnostics)
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `maybe_contains` | `(&self, record_id: &[u8]) -> bool` | Compute k hash positions via double-hashing, check all k bits. Returns `true` if all bits set (possibly present), `false` if any bit clear (definitely absent). **Zero false negatives.** |
| `serialize` | `(&self) -> Bytes` | Encodes: `[num_bits: u64 LE][num_hash_functions: u32 LE][num_keys: u64 LE][bits: bytes]` |
| `deserialize` | `(data: &[u8]) -> FlushResult<Self>` | Parses header fields (20 bytes), reads remaining bytes as bit array, validates `bits.len() * 8 >= num_bits` |
| `size_bytes` | `(&self) -> usize` | Returns `self.bits.len()` |
| `false_positive_rate` | `(&self) -> f64` | Returns theoretical FPR: `(1 - e^(-k*n/m))^k` where k=hash functions, n=keys, m=bits |

### 5.4 Hash Function: Double-Hashing with MurmurHash3

For each record_id, compute two 64-bit hashes using the MurmurHash3 implementation from Task 1:

```
(h1, h2) = murmurhash3_x64_128(record_id, seed=0)
```

For hash function k (0..num_hash_functions):
```
bit_index = (h1.wrapping_add(k as u64 * h2)) % num_bits
```

This produces `k` bit positions from a single 128-bit hash computation, avoiding `k` independent hash calls.

### 5.5 Sizing Formulas

Given `n` keys and `bits_per_key`:
- `num_bits = max(n * bits_per_key, 64)` — minimum 64 bits (8 bytes) to avoid degenerate filters
- Round `num_bits` up to nearest multiple of 8
- `num_hash_functions = max((num_bits / max(n, 1)) * ln(2), 1)` — for 10 bits/key this gives ~7

If `n == 0`, create a minimal filter (64 bits, 1 hash function) that always returns `false` for any input.

### 5.6 Critical Invariant

The bloom filter indexes record_ids from **ALL** entry types — PUTs, DELETEs, and range tombstones alike. If a record has only tombstones in an SSTable, the bloom filter must still report it as possibly present. This ensures the read path finds tombstones that shadow older data in lower-level SSTables.

The `BloomFilterBuilder.add()` and `add_all()` methods accept record_ids without filtering by entry type — the caller (BlockBuilder/SSTableWriter) is responsible for feeding all record_ids.

### 5.7 Trait Does NOT Include

- **No partitioned filter support** — STORAGE_DESIGN.md mentions partitioned filters for large SSTables. This is a future optimization. Phase 4 uses a single monolithic filter per SSTable.
- **No prefix bloom filters** — The per-record item_key prefix optimization is a future concern.

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_bloom_filter_tests.rs`

### No False Negatives Tests
| Test | What It Validates |
|------|-------------------|
| `test_no_false_negatives_1000_keys` | Insert 1000 unique record_ids, check all 1000 — every one returns `true` |
| `test_no_false_negatives_single_key` | Insert 1 key, `maybe_contains` → `true` |
| `test_no_false_negatives_after_serialize` | Serialize → deserialize → all 1000 inserted keys still return `true` |

### False Positive Rate Tests
| Test | What It Validates |
|------|-------------------|
| `test_false_positive_rate_under_threshold` | Insert 10,000 keys, check 10,000 absent keys — FPR < 2% (generous margin over 1% theoretical) |
| `test_empty_filter_always_false` | Filter built from 0 keys → `maybe_contains` returns `false` for any key |

### Serialization Tests
| Test | What It Validates |
|------|-------------------|
| `test_serialize_deserialize_round_trip` | All fields preserved: num_bits, num_hash_functions, num_keys, bit array content |
| `test_serialize_deterministic` | Same set of record_ids produces identical serialized bytes on two separate builds |
| `test_deserialize_rejects_truncated` | Input shorter than 20-byte header → `CorruptedData` error |

### FilterBlock Enum Tests
| Test | What It Validates |
|------|-------------------|
| `test_filter_block_bloom_round_trip` | `FilterBlock::Bloom(filter)` serializes and deserializes correctly |
| `test_filter_block_serialize_has_type_prefix` | First byte of serialized `FilterBlock` is `0x00` (Bloom tag) |
| `test_filter_block_deserialize_unknown_type` | Data with prefix byte `0xFF` → `CorruptedData` error |

### Builder Tests
| Test | What It Validates |
|------|-------------------|
| `test_builder_deduplicates_record_ids` | Adding same record_id 100 times → filter has same FPR as adding once |
| `test_builder_add_all_from_hashset` | `add_all` produces identical filter to individual `add` calls |
| `test_builder_estimated_size` | `estimated_size_bytes()` within 2x of actual `size_bytes()` |

### Hash Distribution Tests
| Test | What It Validates |
|------|-------------------|
| `test_hash_positions_within_bounds` | All computed bit positions `< num_bits` for 1000 record_ids |
| `test_different_keys_different_positions` | Two distinct record_ids ("key_a", "key_b") produce at least one different bit position |

---

## Done When

- [ ] Zero false negatives for any inserted record_id
- [ ] False positive rate < 2% at 10 bits/key with 10,000 keys
- [ ] Serialize/deserialize round-trip preserves all state and lookup correctness
- [ ] `FilterBlock` enum wraps `BloomFilter` with type prefix byte
- [ ] `FilterBlock` rejects unknown type prefixes
- [ ] Double-hashing with MurmurHash3 produces positions within `[0, num_bits)`
- [ ] Empty filter (0 keys) always returns `false`
- [ ] Builder deduplicates record_ids
- [ ] All tests pass
