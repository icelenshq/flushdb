# Task 6: DedupBlock — Idempotency Token Storage

**Crate:** `flushdb-engine`
**File:** `src/sstable/dedup_block.rs`
**Depends on:** Task 1 (MurmurHash3, DEDUP_HASH_SIZE constant)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §19.5, Phase 4 §4

---

## Goal

Build and query a compact hash set of idempotency tokens persisted in the SSTable. This enables cross-SSTable deduplication — when a retry arrives and the memtable dedup misses, the server checks L0/L1 dedup blocks (pinned in DRAM) before accepting the write. The dedup block is loadable independently of data blocks via a 2-read path: footer → compute offset → fetch dedup block.

---

## What to Build

### 6.1 DedupBlockBuilder

```
DedupBlockBuilder {
    token_hashes: HashSet<[u8; 16]>,     // 128-bit hashes, deduplicated
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty builder |
| `add` | `(&mut self, token: &IdempotencyToken)` | Hash full 24-byte token to 128 bits via MurmurHash3, insert into set. Skips none tokens (`token.is_none()` → no-op). |
| `add_all` | `(&mut self, tokens: &[IdempotencyToken])` | Bulk add, skipping none tokens |
| `build` | `(self) -> DedupBlock` | Sort hashes lexicographically, return `DedupBlock` |
| `is_empty` | `(&self) -> bool` | Returns true if no non-none tokens added |
| `len` | `(&self) -> usize` | Number of unique token hashes |

### 6.2 DedupBlock

```
DedupBlock {
    hashes: Vec<[u8; 16]>,              // sorted array of 128-bit hashes
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `contains` | `(&self, token: &IdempotencyToken) -> bool` | Hash token to 128 bits, binary search in sorted array. Returns `false` for none tokens without searching. |
| `len` | `(&self) -> usize` | Number of token hashes |
| `is_empty` | `(&self) -> bool` | Returns `self.hashes.is_empty()` |
| `serialize` | `(&self) -> Bytes` | Encodes: `[count: u32 LE][hash_0: [u8; 16]][hash_1: [u8; 16]]...` |
| `deserialize` | `(data: &[u8]) -> FlushResult<Self>` | Parses count (4 bytes LE), reads `count * 16` bytes as hashes, validates data length. Does NOT re-validate sort order (trusted input from SSTable). |
| `size_bytes` | `(&self) -> usize` | Returns `4 + self.hashes.len() * 16` |

### 6.3 Token Hashing

Hash the full 24-byte token (`IdempotencyToken::as_bytes()`) to 128 bits:

```
(h1, h2) = murmurhash3_x64_128(token.as_bytes(), seed=0x42)
hash = [h1 as 8 bytes LE][h2 as 8 bytes LE]   // 16 bytes total
```

Seed `0x42` is used (different from bloom filter seed `0x00`) to avoid hash correlation between bloom filter and dedup block.

### 6.4 Binary Search

Hashes are stored sorted by lexicographic byte comparison on `[u8; 16]`. Lookup uses `hashes.binary_search(&target_hash)`:
- `Ok(_)` → token is present, return `true`
- `Err(_)` → token is absent, return `false`

### 6.5 Serialization Format

```
[count: u32 LE]                    // number of hashes
[hash_0: [u8; 16]]               // first hash (smallest)
[hash_1: [u8; 16]]               // second hash
...
[hash_{count-1}: [u8; 16]]       // last hash (largest)
```

Total size: `4 + count * 16` bytes.

Empty dedup block: `[count=0]` → 4 bytes.

### 6.6 Validation on Deserialize

| Check | Error |
|-------|-------|
| Data length < 4 | `FlushError::CorruptedData { message: "dedup block too short" }` |
| `(data.len() - 4) % 16 != 0` | `FlushError::CorruptedData { message: "dedup block size not aligned to 16-byte hashes" }` |
| `(data.len() - 4) / 16 != count` | `FlushError::CorruptedData { message: "dedup block count mismatch" }` |

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_dedup_block_tests.rs`

### Lookup Tests
| Test | What It Validates |
|------|-------------------|
| `test_contains_present_token` | Add 1 token, build, `contains()` → `true` |
| `test_contains_absent_token` | Build with token A, `contains(B)` → `false` |
| `test_contains_none_token_returns_false` | `contains(IdempotencyToken::none())` → `false` regardless of block contents |
| `test_multiple_tokens_all_found` | Add 100 distinct tokens, all found via `contains()` |

### Builder Tests
| Test | What It Validates |
|------|-------------------|
| `test_builder_skips_none_tokens` | `add(IdempotencyToken::none())` → `len() == 0` |
| `test_builder_deduplicates` | Add same token twice → `len() == 1` |
| `test_builder_add_all` | `add_all` produces identical result to individual `add` calls |

### Serialization Tests
| Test | What It Validates |
|------|-------------------|
| `test_serialize_deserialize_round_trip` | 50 tokens: serialize → deserialize → all findable via `contains()` |
| `test_serialize_empty_block` | Empty dedup block serializes as 4 bytes `[count=0]`, deserializes to empty block |
| `test_deserialize_rejects_invalid_length` | Data length `4 + 15` (not aligned to 16) → `CorruptedData` error |

### Ordering Tests
| Test | What It Validates |
|------|-------------------|
| `test_hashes_sorted_after_build` | After `build()`, internal hashes array is sorted (each hash <= next) |
| `test_binary_search_correctness_large` | 1000 tokens inserted: all 1000 found, 1000 random absent tokens not found |

---

## Done When

- [ ] Builder skips none tokens (`is_none()` → no-op)
- [ ] Builder deduplicates identical tokens
- [ ] `contains()` returns `true` for all inserted tokens via binary search
- [ ] `contains()` returns `false` for absent tokens and none tokens
- [ ] Serialize/deserialize round-trip preserves all tokens and lookup correctness
- [ ] Empty block serializes as 4 bytes and deserializes correctly
- [ ] Hashes are sorted after `build()`
- [ ] Deserialization rejects misaligned data
- [ ] All tests pass
