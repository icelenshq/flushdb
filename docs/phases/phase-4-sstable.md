# Phase 4: SSTable — Persistent Sorted Files

**Complexity: L**
**Crate:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §7

---

## Goal

Build the on-disk (on-S3) persistent sorted file format. SSTables are the unit of durable storage — memtables flush into SSTables, compaction merges SSTables. Every read beyond the memtable involves SSTable lookups.

---

## 1. Binary Format Overview

A custom binary format. Entries are stored by composite key, so all items for a record are contiguous and sorted:

```
┌──────────────────────────────────┐
│ Header                           │
│   magic: 0x464C4442 ("FLDB")   │
│   version: uint16                │
│   compression: NONE|SNAPPY|ZSTD │
│   entry_count: uint64           │
├──────────────────────────────────┤
│ Data Block 0 (4KB default)       │
│   Entry: [record_id_len | record_id | item_key_len | item_key
│           | value_len | value | metadata_len | metadata
│           | entry_type | sequence_number]
│   Entry: ...
├──────────────────────────────────┤
│ Data Block 1                     │
│   ...                            │
├──────────────────────────────────┤
│ ...                              │
├──────────────────────────────────┤
│ Dedup Block                      │
│   compact hash set of            │
│   idempotency token hashes       │
│   (128-bit per token)           │
├──────────────────────────────────┤
│ Bloom Filter Section             │
│   filter over record_id values   │
│   (not item keys)               │
├──────────────────────────────────┤
│ Index Block                      │
│   block_0_first_key → offset, len│
│   block_1_first_key → offset, len│
│   ...                            │
├──────────────────────────────────┤
│ Footer (80 bytes)                │
└──────────────────────────────────┘
```

**Key properties:**
- **Sorted by composite key** — enables binary search via the sparse index and contiguous range scans within a record
- **Bloom filter on record IDs** — eliminates SSTables that don't contain the target record before any I/O
- **Byte-range reads** — individual data blocks can be fetched via `get_range` without downloading the entire file
- **Compression per block** — each data block independently compressed, allowing random access

---

## 2. Data Blocks

### 2.1 BlockBuilder

Data blocks are built incrementally as the frozen memtable is iterated in sorted order:

```
BlockBuilder {
    buffer: Vec<u8>,
    entry_count: u32,
    block_size_target: usize,              // default 4 KB
    first_key: Option<CompositeKey>,
    last_key: Option<CompositeKey>,
    record_ids_seen: HashSet<RecordId>,    // fed to bloom filter
}
```

**Entry encoding within a block:**
```
[record_id_len: varint] [record_id: bytes]
[item_key_len: varint]  [item_key: bytes]
[value_len: varint]     [value: bytes]
[metadata_len: varint]  [metadata: bytes]
[entry_type: u8]
[sequence_number: varint]
```

**Record ID deduplication within blocks:** When consecutive entries share the same `record_id` (very common for wide records), the second and subsequent entries store `record_id_len = 0` and omit the record_id bytes. The reader carries forward the last seen record_id. This can reduce block size by 30-50% for wide records.

**Constraint:** Empty record IDs are forbidden at the API layer. This makes `record_id_len = 0` an unambiguous dedup signal. The first entry in every block MUST include the full record_id.

**Block finalization (when `buffer.len() >= block_size_target`):**
1. Compute CRC32 over the uncompressed buffer and append it (4 bytes)
2. Compress the buffer + CRC (Snappy or ZSTD, configurable; or None)
3. Record `(first_key, offset, compressed_len, uncompressed_len)` in the index builder
4. Collect `record_ids_seen` into the bloom filter builder
5. Reset the block builder

### 2.2 BlockReader

Reads a single data block:
1. Fetch raw bytes (from StorageBackend via `get_range`)
2. Decompress if needed
3. Validate CRC32
4. Iterate entries, carrying forward `record_id` when `record_id_len = 0`

---

## 3. Bloom Filter

Built incrementally as blocks are finalized. Indexes `record_id`s, NOT item keys.

**Properties:**
- **1% FPR** with ~10 bits/key
- A 64 MB memtable with ~100K unique record IDs produces a ~125 KB bloom filter
- Loaded into memory when the SSTable is opened

**Hash function:** Double-hashing with two independent MurmurHash3 seeds. The k-th hash is computed as `h1 + k * h2`.

**Critical detail:** The bloom filter indexes record_ids from ALL entry types — PUTs, DELETEs, and range tombstones alike. This ensures a bloom filter positive also covers any tombstones for that record.

---

## 4. Dedup Block

A compact hash set of idempotency tokens from this SSTable, for cross-SSTable deduplication:

- **Format:** Array of 128-bit token hashes
- **Size:** ~16 bytes per unique token
- **Lookup:** Hash the incoming token, binary search in the sorted hash array
- **Compaction cleanup:** Tokens older than `idempotency_retention_ttl` (default 10 minutes) are dropped during compaction

The dedup block enables checking whether a retry's token was already persisted, without scanning data blocks.

---

## 5. Index Block

Sparse index mapping the first composite key of each data block to its offset:

```
IndexEntry {
    first_key: CompositeKey,
    block_offset: u64,
    block_size: u32,
    uncompressed_size: u32,
}
```

Entries are stored sorted. Binary search locates the block that could contain any target key. For a point lookup, the index narrows the search to at most one data block.

---

## 6. Footer

Fixed-size (80 bytes) trailer at the end of the SSTable:

```
Footer {                                                          // Offset  Size
    bloom_filter_offset: u64,                                     //  0       8
    bloom_filter_size: u32,                                       //  8       4
    index_block_offset: u64,                                      // 12       8
    index_block_size: u32,                                        // 20       4
    entry_count: u64,                                             // 24       8
    min_key: [u8; 16],           // truncated min composite key   // 32      16
    max_key: [u8; 16],           // truncated max composite key   // 48      16
    compression_type: u8,                                         // 64       1
    _padding: [u8; 1],                                            // 65       1
    format_version: u16,                                          // 66       2
    crc32: u32,                                                   // 68       4
    magic: u32,                  // 0x464C4442 ("FLDB")           // 72       4
    _reserved: [u8; 4],                                           // 76       4
}                                                                 // Total:  80
```

**`min_key`/`max_key` truncation:** Keys truncated to 16 bytes. Can produce false positives but never false negatives. The footer key range is a cheap pre-filter to avoid loading bloom filters for completely disjoint SSTables.

**Reading the footer:** `get_range(key, file_size - 80, 80)` — always the last 80 bytes.

---

## 7. SSTableWriter

End-to-end writer that builds a complete SSTable from a sorted entry iterator:

1. Write header
2. Iterate entries in sorted order:
   - Feed to BlockBuilder
   - When block full, finalize and write compressed block
   - Collect record_ids into bloom filter builder
   - Collect idempotency tokens into dedup builder
3. Write dedup block
4. Write bloom filter
5. Write index block
6. Write footer
7. Upload completed SSTable to StorageBackend

The writer accepts an iterator (the frozen memtable) and a StorageBackend destination.

---

## 8. SSTableReader

Reads an SSTable from StorageBackend:

1. **Open:** Read footer (last 80 bytes). Validate magic and CRC.
2. **Load metadata:** Read index block and bloom filter into memory.
3. **Point lookup:**
   - Check bloom filter for record_id → skip if negative
   - Binary search index for target block
   - Fetch and decompress data block
   - Scan block for exact key
4. **Range iteration:**
   - Binary search index for start block
   - Iterate blocks forward, yielding entries in order
   - Stop when past end key or record_id changes

**Entry value support:** Both `EntryValue::Inline` (value stored directly in data block) and `EntryValue::BlobRef` (pointer to separated blob) are parsed. Phase 4 only writes `Inline`; `BlobRef` reading is implemented for forward compatibility.

---

## New Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| snap | 1 | Snappy compression for data blocks |
| zstd | 0.13 | Zstd compression for data blocks |
| ulid | 1 | SSTable file naming (sortable + unique IDs) |

---

## Future Work Considerations

When building Phase 4, keep the following downstream dependencies in mind:

| What You're Building | Who Needs It Later | What To Watch For |
|---------------------|-------------------|-------------------|
| **SSTableWriter** | Flush pipeline (P5b), Compaction (P5e) | Flush and compaction both produce SSTables. The writer should accept any sorted entry iterator, not just memtable iterators — compaction feeds it a merge-sorted stream from multiple input SSTables. Design the input as a generic `Iterator<Item = Entry>`. |
| **SSTableReader** | Read path (P5c), Cache layer (P6) | The read path opens readers for all live SSTables. The reader must support: (1) opening metadata without reading all blocks (footer → index → bloom), (2) block-level random access via `get_range`, (3) range iteration across blocks. Cache integration (P6) will wrap block fetches — design block reads as a separable step that a cache can intercept. |
| **Bloom filter** | Cache pinning (P6), Ribbon filters (future) | Bloom filters are pinned in DRAM by the cache layer. Expose bloom filters as a standalone structure that can be loaded and held independently of the SSTable reader. Future work replaces bloom with ribbon filters for compacted SSTables — use a trait or enum (`FilterBlock`) so the read path doesn't hardcode bloom. |
| **Index block** | Cache pinning (P6), Compaction (P5e) | Like bloom filters, indexes are pinned in DRAM. Compaction uses index blocks to determine key range overlaps between levels. The index must support efficient `key_range() -> (min_key, max_key)` and `overlaps(key_range)` queries. |
| **Data block format** | Coalesced fetches (P6), NVMe cache (future) | The cache layer coalesces adjacent block reads into a single `get_range`. Blocks must be independently decompressible — no cross-block compression dependencies. Block boundaries must be deterministic from the index. |
| **Dedup block** | Server idempotency (P7) | The server checks dedup blocks when memtable dedup misses. L0/L1 dedup blocks will be pinned in DRAM. Design dedup block loading as independent of full SSTable reads — the server needs to check dedup without opening data blocks. |
| **SSTable file naming (ULID)** | Run fragments (P5e), S3 layout (P7) | Compaction produces run fragments: `run-{id}/frag-{index}.sst`. The naming scheme must accommodate both standalone SSTables (L0 flushes) and run fragments (compaction output). Consider this in the ULID/path conventions now. |
| **BlobRef parsing** | Value separation (future) | `EntryValue::BlobRef` is defined but unused. The SSTableReader must still parse it correctly — if a future version writes BlobRefs, old readers shouldn't crash. Include BlobRef in serialization/deserialization tests even if no writer produces them yet. |

---

## Done When

- Flush a memtable (from Phase 3) to an SSTable via StorageBackend — read it back, all entries match exactly
- Data blocks respect 4KB target size, rotate correctly
- Record ID deduplication within blocks: consecutive entries with same record_id stored with `record_id_len = 0`
- Compression works for all three modes (None, Snappy, Zstd) — round-trips correctly
- CRC32 validation catches corrupted data blocks
- Bloom filter correctly eliminates SSTables that don't contain the target record ID
- Bloom filter has no false negatives for record IDs that ARE present
- Index block binary search finds the correct data block for any key
- Footer encodes/decodes correctly, magic and CRC validated
- Dedup block: tokens round-trip, lookup finds present tokens, rejects absent tokens
- SSTableReader opens an SSTable by reading footer first, then metadata on demand
- Point lookup through SSTableReader returns correct result
- Range iteration through SSTableReader yields entries in correct sorted order
