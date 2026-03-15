# Task 8: SSTableWriter — End-to-End Writer

**Crate:** `flushdb-engine`
**File:** `src/sstable/writer.rs`
**Depends on:** Task 1 (types/config), Task 2 (footer/header), Task 3 (BlockBuilder), Task 5 (BloomFilter), Task 6 (DedupBlock), Task 7 (IndexBlock)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §7, Phase 4 §7

---

## Goal

Orchestrate the end-to-end construction of a complete SSTable from a sorted entry iterator. The writer builds data blocks incrementally, collects bloom filter and dedup block data as blocks finalize, computes the index, and uploads the finished SSTable to a `StorageBackend`. Designed to accept any sorted entry iterator — frozen memtable iterators for flush (Phase 5), and merge-sorted streams for compaction (Phase 5e).

---

## What to Build

### 8.1 SstInfo Struct

Metadata about a written SSTable, returned to the caller for manifest updates:

```
SstInfo {
    path: String,                           // StorageBackend key
    entry_count: u64,
    file_size: u64,
    min_key: CompositeKey,
    max_key: CompositeKey,
    bloom_filter_offset: u64,
    bloom_filter_size: u32,
    index_block_offset: u64,
    index_block_size: u32,
    dedup_block_size: u32,
    compression: CompressionType,
}
```

**Derives:** `Debug`, `Clone`

### 8.2 SSTableWriter

```
SSTableWriter {
    config: SstConfig,
}
```

The writer is stateless — it holds only configuration. The `StorageBackend` is passed per-write call to allow the caller to control which backend instance is used.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: SstConfig) -> Self` | Creates writer with given config |
| `write` | `<B: StorageBackend>(&self, backend: &B, path: &str, entries: impl Iterator<Item = MemtableEntry>) -> FlushResult<SstInfo>` | Builds and uploads a complete SSTable from a `MemtableEntry` iterator. Converts each entry's value to `EntryValue::Inline`. Returns `SstInfo`. |
| `write_entries` | `<B: StorageBackend>(&self, backend: &B, path: &str, entries: impl Iterator<Item = BlockEntry>) -> FlushResult<SstInfo>` | Accepts `BlockEntry` iterator (for compaction merge streams where entries already have `EntryValue`). Passes through values unchanged (Inline or BlobRef). |

### 8.3 Write Pipeline

The `write` method follows this sequence:

1. **Initialize:**
   - Output buffer: `Vec<u8>`
   - `BlockBuilder::new(config.block_size_target)`
   - `BloomFilterBuilder::new(config.bloom_bits_per_key)`
   - `DedupBlockBuilder::new()`
   - `IndexBlockBuilder::new()`
   - Entry counter, global min_key / max_key tracking

2. **Reserve header space:** Write 16 zero bytes at start of buffer (placeholder for `SstHeader`).

3. **Iterate entries in sorted order.** For each `MemtableEntry`:
   a. Add to `BlockBuilder` via `add_entry(key, value, metadata, entry_type, seq, token)`
   b. Track global min_key (first entry) and max_key (every entry)
   c. Increment entry counter
   d. If `BlockBuilder.is_full()`:
      - `let finished = block_builder.finish(config.compression)`
      - Record block offset in output buffer
      - Append `finished.data` to output buffer
      - `index_builder.add(finished.first_key, block_offset, finished.data.len() as u32, finished.uncompressed_size)`
      - `bloom_builder.add_all(&finished.record_ids)`
      - `dedup_builder.add_all(&finished.idempotency_tokens)`
      - `block_builder.reset()`

4. **Flush remaining entries:** If `!block_builder.is_empty()`, finalize last block and process as in step 3d.

5. **Write dedup block:**
   - `let dedup = dedup_builder.build()`
   - `let dedup_bytes = dedup.serialize()`
   - Append `dedup_bytes` to output buffer
   - Record `dedup_block_size = dedup_bytes.len() as u32`

6. **Write bloom filter:**
   - `let bloom = bloom_builder.build()`
   - `let filter = FilterBlock::Bloom(bloom)`
   - `let filter_bytes = filter.serialize()`
   - Record `bloom_filter_offset` = current buffer length
   - Append `filter_bytes` to output buffer
   - Record `bloom_filter_size = filter_bytes.len() as u32`

7. **Write index block:**
   - `let index = index_builder.build()`
   - `let index_bytes = index.serialize()`
   - Record `index_block_offset` = current buffer length
   - Append `index_bytes` to output buffer
   - Record `index_block_size = index_bytes.len() as u32`

8. **Write footer:**
   - Construct `SstFooter` with all recorded offsets, sizes, min/max keys (truncated), entry_count, compression, format_version, dedup_block_size
   - `let footer_bytes = footer.encode()`
   - Append `footer_bytes` to output buffer

9. **Backfill header:**
   - Construct `SstHeader { compression: config.compression, entry_count }`
   - `let header_bytes = header.encode()`
   - Copy `header_bytes` into `buffer[0..HEADER_SIZE]`

10. **Upload:**
    - `backend.put(path, Bytes::from(buffer)).await`

11. **Return SstInfo** with all metadata

### 8.4 ULID Path Utilities

The writer accepts a `path` parameter — callers are responsible for path generation. Provide utility functions:

| Function | Signature | Behavior |
|----------|-----------|----------|
| `generate_sst_path` | `(namespace: &str, level: u32) -> String` | Returns `flushdb/{namespace}/sstables/L{level}/{ulid}.sst` using `ulid::Ulid::new()` |
| `generate_run_fragment_path` | `(namespace: &str, level: u32, run_id: &str, fragment_index: u32) -> String` | Returns `flushdb/{namespace}/sstables/L{level}/run-{run_id}/frag-{fragment_index:04}.sst` |

These utilities accommodate both standalone L0 SSTables (memtable flush) and run fragments (compaction output), per the "Future Work Considerations" for SSTable file naming.

### 8.5 Error Handling

| Condition | Error |
|-----------|-------|
| Empty iterator (no entries) | `FlushError::InvalidArgument { message: "cannot write empty SSTable" }` |
| Compression failure | `FlushError::Io` (wrapped from snap/zstd) |
| Backend upload failure | Propagated `FlushError` from `backend.put()` |

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_writer_tests.rs`

All tests use `LocalFsBackend` with `tempdir`.

### Basic Write Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_single_entry` | Single-entry SSTable: write → verify file exists → read footer → validate entry_count=1 |
| `test_write_100_entries` | 100 sorted entries → file exists → footer entry_count=100 |
| `test_write_creates_file_at_path` | File exists at given path on `LocalFsBackend` after write |
| `test_write_returns_correct_sst_info` | `SstInfo` fields: path matches, entry_count matches, min_key/max_key match first/last entry |

### Block Rotation Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_rotates_blocks_at_target` | Entries totaling >4KB produce multiple data blocks — verify via footer index_block_size > 0 and index has multiple entries |
| `test_write_index_entries_match_block_count` | Number of index entries equals number of finalized blocks |

### Compression Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_compression_none` | Uncompressed SSTable: valid footer, footer.compression_type == None |
| `test_write_compression_snappy` | Snappy SSTable: file_size < uncompressed SSTable file_size for same entries |
| `test_write_compression_zstd` | Zstd SSTable: file_size < uncompressed SSTable file_size for same entries |

### Metadata Embedding Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_bloom_filter_readable` | Read bloom filter bytes from file at footer offset → `FilterBlock::deserialize` succeeds |
| `test_write_dedup_block_has_tokens` | Write entries with idempotency tokens → `footer.dedup_block_size > 4` (more than just the count header) |
| `test_write_footer_min_max_keys` | Footer min_key matches `truncate_key(first_entry.key)`, max_key matches `truncate_key(last_entry.key)` |
| `test_write_header_consistent_with_footer` | Read header from file start → header.entry_count == footer.entry_count, header.compression == footer.compression_type |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_write_empty_iterator_errors` | Empty iterator → `InvalidArgument` error |
| `test_write_all_same_record_id` | 50 entries with same record_id → dedup compression reduces block sizes (verify file_size is reasonable) |
| `test_write_entries_with_tombstones` | Mix of Put, Delete, RangeDelete entries → all included in footer entry_count |
| `test_write_entries_with_none_tokens` | All entries have `IdempotencyToken::none()` → dedup_block_size == 4 (empty dedup block: count=0 only) |

### Path Utility Tests
| Test | What It Validates |
|------|-------------------|
| `test_generate_sst_path_format` | Matches `flushdb/{ns}/sstables/L{level}/{ulid}.sst` pattern |
| `test_generate_run_fragment_path_format` | Matches `flushdb/{ns}/sstables/L{level}/run-{id}/frag-{idx:04}.sst` pattern |

---

## Done When

- [ ] Single-entry and multi-entry SSTables write successfully via `StorageBackend`
- [ ] Block rotation occurs at `block_size_target`
- [ ] All three compression modes produce valid SSTables
- [ ] Footer offsets correctly locate bloom filter, index block, and dedup block sections
- [ ] Header and footer `entry_count` are consistent
- [ ] Bloom filter contains all record_ids from all entry types
- [ ] Dedup block contains all non-none idempotency tokens
- [ ] Empty iterator produces clear `InvalidArgument` error
- [ ] `SstInfo` accurately describes the written SSTable
- [ ] Path utilities produce correctly formatted paths
- [ ] All tests pass
