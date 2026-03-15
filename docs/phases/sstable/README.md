# Phase 4: SSTable — Subtask Breakdown

## Overview

Phase 4 builds the persistent sorted file format that is the backbone of durable storage. SSTables are the unit of durable storage — memtables flush into SSTables, compaction merges SSTables. Every read beyond the memtable involves SSTable lookups. The format supports block-level compression, bloom filters for record-level elimination, a dedup block for idempotency, and a sparse index for binary search.

**Crates:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §7, §7.1-§7.6

---

## Subtask Execution Order

Tasks are ordered by dependency — each task depends only on tasks above it.

| # | Subtask | File | Crate | Dependencies |
|---|---------|------|-------|-------------|
| 1 | [SSTable Types & Constants](./01-sstable-types-constants.md) | `src/sstable/types.rs`, `varint.rs`, `hash.rs` | flushdb-engine | none |
| 2 | [Footer — SSTable Trailer](./02-footer.md) | `src/sstable/footer.rs` | flushdb-engine | task 1 |
| 3 | [BlockBuilder — Data Block Construction](./03-block-builder.md) | `src/sstable/block_builder.rs` | flushdb-engine | task 1 |
| 4 | [BlockReader — Data Block Decoding](./04-block-reader.md) | `src/sstable/block_reader.rs` | flushdb-engine | task 1, 3 |
| 5 | [BloomFilter — Record ID Filter](./05-bloom-filter.md) | `src/sstable/bloom_filter.rs` | flushdb-engine | task 1 |
| 6 | [DedupBlock — Idempotency Token Storage](./06-dedup-block.md) | `src/sstable/dedup_block.rs` | flushdb-engine | task 1 |
| 7 | [IndexBlock — Sparse Block Index](./07-index-block.md) | `src/sstable/index_block.rs` | flushdb-engine | task 1 |
| 8 | [SSTableWriter — End-to-End Writer](./08-sstable-writer.md) | `src/sstable/writer.rs` | flushdb-engine | tasks 1-7 |
| 9 | [SSTableReader — End-to-End Reader](./09-sstable-reader.md) | `src/sstable/reader.rs` | flushdb-engine | tasks 1-7 |
| 10 | [Module Structure & Exports](./10-module-structure-exports.md) | `mod.rs`, `lib.rs`, `Cargo.toml` | flushdb-engine | all above |

---

## Acceptance Criteria (Phase-Level)

All of these must pass before Phase 4 is considered complete:

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] Memtable (Phase 3) → SSTable write → read round-trip: all entries match exactly
- [ ] Data blocks respect 4KB target size and rotate correctly
- [ ] Record ID deduplication: consecutive same-record entries stored with `record_id_len = 0`
- [ ] All three compression modes (None, Snappy, Zstd) round-trip correctly
- [ ] CRC32 validation catches corrupted data blocks
- [ ] Bloom filter has zero false negatives for present record_ids
- [ ] Bloom filter FPR < 2% with 10 bits/key
- [ ] Index block binary search finds correct data block for any key
- [ ] Footer encodes/decodes correctly, magic and CRC validated
- [ ] Dedup block: tokens round-trip, binary search finds present tokens, rejects absent
- [ ] SSTableReader opens by reading footer first, loads metadata on demand
- [ ] Point lookup through SSTableReader returns correct result
- [ ] Range iteration through SSTableReader yields entries in correct sorted order
- [ ] EntryValue::BlobRef parsing works for forward compatibility

---

## New Dependencies (Phase 4)

| Crate | Version | Purpose |
|-------|---------|---------|
| snap | 1 | Snappy compression for data blocks |
| zstd | 0.13 | Zstd compression for data blocks |
| ulid | 1 | SSTable file naming (sortable + unique IDs) |

---

## Design Decisions

1. **Footer `_reserved` → `dedup_block_size`:** The 4-byte `_reserved` field in the footer is repurposed as `dedup_block_size: u32`. This enables 2-read dedup lookup (footer → dedup block) instead of requiring the index block to compute the dedup offset. The dedup block offset is derived as `bloom_filter_offset - dedup_block_size`.

2. **EntryValue encoding in blocks:** Block value fields include the EntryValue discriminant tag (0x00 Inline, 0x01 BlobRef). Phase 4 always writes Inline; the reader handles both for forward compatibility with value separation.

3. **FilterBlock enum:** BloomFilter is wrapped in a `FilterBlock` enum (single `Bloom` variant) so Ribbon filters can be added as a variant in Phase 5e without changing the reader interface.

4. **MurmurHash3 in-crate:** MurmurHash3 is implemented in safe Rust as a private utility rather than adding an external dependency, keeping the dependency footprint minimal.
