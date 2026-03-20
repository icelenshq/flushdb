# Phase 6: Caching + Read Optimizations — Subtask Breakdown

## Overview

Add a DRAM cache layer between the read path and StorageBackend so repeated reads complete in <1ms, scan-resistant admission prevents full-record scans from evicting point-read hotspots, and compaction-aware eviction ensures no stale data is served. After this phase the embedded engine is read-performance-competitive with production KV stores.

**Crates:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §12, §13

---

## Subtask Execution Order

Tasks are ordered by dependency — each task depends only on tasks above it.

| # | Subtask | File(s) | Crate | Dependencies |
|---|---------|---------|-------|-------------|
| 1 | [BlockCache + CacheConfig](./01-block-cache.md) | `src/cache/block_cache.rs`, `src/cache/mod.rs` | flushdb-engine | none |
| 2 | [CachingBlockFetcher](./02-caching-block-fetcher.md) | `src/cache/caching_fetcher.rs` | flushdb-engine | task 1 |
| 3 | [PinnedMetadataCache](./03-pinned-metadata-cache.md) | `src/cache/pinned_metadata.rs` | flushdb-engine | task 1 |
| 4 | [Compaction-Aware Cache Eviction](./04-compaction-eviction.md) | `src/cache/eviction.rs` | flushdb-engine | task 1, 2, 3 |
| 5 | [ContinuityTracker — Negative Lookup Cache](./05-continuity-tracker.md) | `src/cache/continuity.rs` | flushdb-engine | task 1, 4 |
| 6 | [Coalesced Block Fetches](./06-coalesced-fetches.md) | `src/cache/coalescing.rs` | flushdb-engine | task 2 |
| 7 | [GET Budget](./07-get-budget.md) | `src/cache/budget.rs` | flushdb-engine | task 2 |
| 8 | [Adaptive Pagination](./08-adaptive-pagination.md) | `src/cache/pagination.rs`, `src/read_path.rs` | flushdb-engine | task 2 |
| 9 | [Engine Integration + Cache Lifecycle](./09-engine-integration.md) | `src/engine.rs`, `src/sstable_handle.rs`, `src/read_path.rs` | flushdb-engine | all above |

---

## Acceptance Criteria (Phase-Level)

All of these must pass before Phase 6 is considered complete:

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] Repeated point reads for the same key hit cache (<1ms latency after first read)
- [ ] Full-record scan does NOT evict hot point-read data — point reads remain fast after scan
- [ ] Bloom filters and index blocks are pinned in memory, not evicted by data cache
- [ ] Compaction invalidates stale cache entries — no stale reads after compaction
- [ ] Continuity tracking: negative lookup within fully-cached key range skips StorageBackend
- [ ] Coalesced fetches: range scan over N adjacent blocks results in fewer than N StorageBackend calls
- [ ] GET budget: a read exceeding 8 StorageBackend GETs is capped and returns partial results
- [ ] Adaptive pagination produces more accurate page sizes on second and subsequent pages

---

## New Dependencies (Phase 6)

| Crate | Version | Purpose |
|-------|---------|---------|
| moka | 0.12 | W-TinyLFU cache implementation (DRAM tier) |
