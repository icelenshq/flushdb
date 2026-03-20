# Task 10: Module Structure & Exports

**Crate:** `flushdb-engine`
**Files:** `src/sstable/mod.rs`, `src/lib.rs`, `Cargo.toml`
**Depends on:** All above tasks (1-9)
**Estimated complexity:** S
**Design reference:** CLAUDE.md (crate dependency graph, file organization)

---

## Goal

Wire up the SSTable module structure, re-export public types from `lib.rs`, and add the new crate dependencies. This task ensures the SSTable API is cleanly accessible to downstream code — the flush pipeline, compaction, and read path in Phase 5 all consume these types.

---

## What to Build

### 10.1 Directory Structure

```
crates/flushdb-engine/src/
  sstable/
    mod.rs              // module declarations and re-exports
    types.rs            // CompressionType, SstConfig, constants
    varint.rs           // varint encode/decode
    hash.rs             // MurmurHash3 (private)
    footer.rs           // SstFooter, SstHeader
    block_builder.rs    // BlockBuilder, FinishedBlock
    block_reader.rs     // BlockEntry, BlockEntryIterator, decode_block
    bloom_filter.rs     // BloomFilter, BloomFilterBuilder, FilterBlock
    dedup_block.rs      // DedupBlock, DedupBlockBuilder
    index_block.rs      // IndexBlock, IndexBlockBuilder, IndexEntry
    writer.rs           // SSTableWriter, SstInfo, path utilities
    reader.rs           // SSTableReader, SstableIterator
```

### 10.2 `src/sstable/mod.rs` Re-exports

```rust
pub mod types;
pub mod varint;
pub mod footer;
pub mod block_builder;
pub mod block_reader;
pub mod bloom_filter;
pub mod dedup_block;
pub mod index_block;
pub mod writer;
pub mod reader;

mod hash;  // private — internal utility, not part of public API

// Convenience re-exports for common types
pub use types::{
    CompressionType, SstConfig,
    SSTABLE_MAGIC, FORMAT_VERSION, DEFAULT_BLOCK_SIZE,
    FOOTER_SIZE, HEADER_SIZE, MIN_MAX_KEY_TRUNCATION_LEN,
    DEFAULT_BLOOM_BITS_PER_KEY, DEFAULT_BLOOM_HASH_COUNT, DEDUP_HASH_SIZE,
};
pub use footer::{SstFooter, SstHeader};
pub use block_builder::{BlockBuilder, FinishedBlock};
pub use block_reader::{BlockEntry, BlockEntryIterator, decode_block};
pub use bloom_filter::{BloomFilter, BloomFilterBuilder, FilterBlock};
pub use dedup_block::{DedupBlock, DedupBlockBuilder};
pub use index_block::{IndexBlock, IndexBlockBuilder, IndexEntry};
pub use writer::{SSTableWriter, SstInfo, generate_sst_path, generate_run_fragment_path};
pub use reader::SSTableReader;
```

### 10.3 `src/lib.rs` Update

Add to `flushdb-engine`'s `lib.rs`:

```rust
pub mod sstable;
```

This makes all SSTable types accessible as `flushdb_engine::sstable::*`.

### 10.4 `Cargo.toml` Dependencies

Add to `crates/flushdb-engine/Cargo.toml` under `[dependencies]`:

| Crate | Version | Purpose |
|-------|---------|---------|
| `snap` | `1` | Snappy compression for data blocks |
| `zstd` | `0.13` | Zstd compression for data blocks |
| `ulid` | `1` | SSTable file naming (sortable + unique IDs) |

These are listed in `docs/plan.md` as Phase 4 dependencies. `crc32fast` is already in the workspace from Phase 2.

### 10.5 Test File Organization

All test files live in `crates/flushdb-engine/tests/`:

```
crates/flushdb-engine/tests/
  sstable_types_tests.rs          // Task 1
  sstable_footer_tests.rs         // Task 2
  sstable_block_builder_tests.rs  // Task 3
  sstable_block_reader_tests.rs   // Task 4
  sstable_bloom_filter_tests.rs   // Task 5
  sstable_dedup_block_tests.rs    // Task 6
  sstable_index_block_tests.rs    // Task 7
  sstable_writer_tests.rs         // Task 8
  sstable_reader_tests.rs         // Task 9
  sstable_integration_tests.rs    // Task 10
```

Per CLAUDE.md: tests go in separate `tests/` directory, NOT inline `#[cfg(test)]` blocks.

---

## Tests

**File:** `crates/flushdb-engine/tests/sstable_integration_tests.rs`

### Integration Tests
| Test | What It Validates |
|------|-------------------|
| `test_memtable_to_sstable_round_trip` | Create a `Memtable` (Phase 3), insert 50 entries, freeze, iterate via `into_skiplist().into_iter()`, write SSTable, read back all entries — every field matches |
| `test_sstable_with_range_tombstones` | Memtable with Put + Delete + RangeDelete entries → SSTable write → read back → correct entry_types and values |
| `test_sstable_large_entry_count` | Write 10,000 entries → SSTable with many blocks → random point lookups and full iteration all correct |

---

## Done When

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass (tasks 1-9 plus integration)
- [ ] `cargo clippy --workspace` — no warnings
- [ ] All SSTable types accessible via `flushdb_engine::sstable::*`
- [ ] New dependencies (snap, zstd, ulid) resolve correctly
- [ ] Phase 3 Memtable → SSTable write → read round-trip works end-to-end
- [ ] `hash` module is private (not re-exported)
- [ ] Test files follow project convention (external test files, not inline)
