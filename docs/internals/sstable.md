# SSTable Format

SSTables are immutable sorted files on S3. Each one is produced by flushing a frozen memtable (L0) or by compaction (L1+).

## Binary Layout

```
┌──────────────────────────────────────────┐
│ Header                                    │
│   magic: 0x464C4442 ("FLDB")            │
│   version: uint16                         │
│   compression: NONE | SNAPPY | ZSTD       │
│   entry_count: uint64                     │
├──────────────────────────────────────────┤
│ Data Block 0 (4 KB target)                │
│   Entry, Entry, Entry, ...                │
│   CRC32 (uncompressed)                    │
├──────────────────────────────────────────┤
│ Data Block 1...N                          │
├──────────────────────────────────────────┤
│ Dedup Block                               │
│   128-bit hashes of idempotency tokens    │
├──────────────────────────────────────────┤
│ Bloom/Ribbon Filter                       │
│   Over record_id values (NOT item keys)   │
├──────────────────────────────────────────┤
│ Index Block                               │
│   first_key → (block_offset, block_size)  │
├──────────────────────────────────────────┤
│ Footer (80 bytes fixed)                   │
│   Section offsets, entry count,           │
│   min/max key (16B truncated),            │
│   compression, version, CRC, magic        │
└──────────────────────────────────────────┘
```

## Data Blocks

4 KB target, independently compressed (Snappy or ZSTD). Each block contains sorted entries:

```
[record_id_len: varint] [record_id: bytes]
[item_key_len: varint]  [item_key: bytes]
[value_len: varint]     [value: bytes]
[metadata_len: varint]  [metadata: bytes]
[entry_type: u8]
[sequence_number: varint]
```

**Record ID deduplication:** Consecutive entries sharing a record ID store `record_id_len = 0` and omit the bytes. The reader carries forward the last seen value. Saves **30-50%** block space for wide records. Empty record IDs are forbidden at the API layer, making `0` an unambiguous dedup signal. The first entry in every block always includes the full record ID.

Each block is finalized with a CRC32 over uncompressed bytes, then compressed. The index builder records `(first_key, offset, compressed_size)`. Unique record IDs are fed to the bloom filter builder.

## Bloom and Ribbon Filters

Filters index **record IDs**, not full composite keys. One check covers all items in a record.

| SSTable Source | Filter Type | Bits/Key | FPR |
|---------------|------------|----------|-----|
| Memtable flush (L0) | Bloom filter | ~10 | ~1% |
| Compaction (L1+) | Ribbon filter | ~7 | ~1% |

Ribbon filters are 30% smaller at the same FPR. Higher construction cost, but that's a one-time cost at compaction time. Hash function: double-hashing with two MurmurHash3 seeds (`h1 + k * h2`).

Filters cover all entry types (PUT, DELETE, RANGE_DELETE) so a filter positive also covers tombstones.

## Index Block

Sparse index mapping first composite key → block location:

```rust
IndexEntry { first_key, block_offset: u64, block_size: u32, uncompressed_size: u32 }
```

Binary search locates the single block that could contain any target key.

## Footer

Fixed 80 bytes at the end of every SSTable:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 8 | `bloom_filter_offset` |
| 8 | 4 | `bloom_filter_size` |
| 12 | 8 | `index_block_offset` |
| 20 | 4 | `index_block_size` |
| 24 | 8 | `entry_count` |
| 32 | 16 | `min_key` (truncated) |
| 48 | 16 | `max_key` (truncated) |
| 64 | 1 | `compression_type` |
| 66 | 2 | `format_version` |
| 68 | 4 | `crc32` |
| 72 | 4 | `magic` (0x464C4442) |

`min_key`/`max_key` truncation can produce false positives but never false negatives — a cheap pre-filter before loading bloom filters.

## S3 Access Pattern

A cold read (nothing cached) walks the SSTable from the tail:

```
              SSTable on S3
┌──────────────────────────────────────┐
│ Data Block 0    ◄────────────────────┼──── Step 4: GET data block
│ Data Block 1                         │     decompress, scan for key
│ ...                                  │
│ Data Block N                         │
├──────────────────────────────────────┤
│ Dedup Block                          │
├──────────────────────────────────────┤
│ Bloom Filter    ◄────────────────────┼──── Step 2: GET bloom filter
│                                      │     check if record_id present
├──────────────────────────────────────┤     if negative → skip SSTable
│ Index Block     ◄────────────────────┼──── Step 3: GET index block
│                                      │     binary search → block offset
├──────────────────────────────────────┤
│ Footer (80B)    ◄────────────────────┼──── Step 1: GET bytes=-80
│                                      │     find section offsets
└──────────────────────────────────────┘

Small SSTables: footer + bloom + index contiguous → single ~200 KB GET
Hot SSTables: bloom + index pinned in DRAM → skip to step 4
```

## Upload Strategy

| SSTable Size | Method |
|-------------|--------|
| < 16 MB | Single `PutObject` |
| ≥ 16 MB | Streaming multipart upload (16 MB parts, double-buffered) |

Peak memory: O(32 MB) regardless of SSTable size. Incomplete uploads cleaned up by S3 lifecycle rule (24 hours).
