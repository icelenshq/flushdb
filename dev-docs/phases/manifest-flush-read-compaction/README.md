# Phase 5: Manifest + Flush + Read Path + Compaction — Subtask Breakdown

## Overview

Phase 5 wires together the manifest, flush pipeline, read path, recovery, and compaction into a **working embedded KV store**. After this phase, you can write millions of keys, kill the process, recover from manifest + WAL, and read back correct data. This is the critical gate — everything before is infrastructure, everything after is optimization and networking.

**Crates:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §8, §9, §10, §11, §13, §22

---

## Subtask Execution Order

Tasks are ordered by dependency — each task depends only on tasks above it.

| # | Subtask | File | Crate | Dependencies |
|---|---------|------|-------|-------------|
| 1 | [Manifest Types & Serialization](./01-manifest-types.md) | `src/manifest/types.rs` | flushdb-engine | none |
| 2 | [Manifest Manager](./02-manifest-manager.md) | `src/manifest/manager.rs` | flushdb-engine | task 1 |
| 3 | [Manifest Snapshots & Pruning](./03-manifest-snapshots.md) | `src/manifest/snapshots.rs` | flushdb-engine | tasks 1, 2 |
| 4 | [BlockFetcher Trait & SSTable Metadata Cache](./04-block-fetcher.md) | `src/block_fetcher.rs` | flushdb-engine | none |
| 5 | [Merge Iterator](./05-merge-iterator.md) | `src/merge_iterator.rs` | flushdb-engine | none |
| 6 | [Flush Pipeline](./06-flush-pipeline.md) | `src/flush.rs` | flushdb-engine | tasks 1, 2 |
| 7 | [Point Read Path](./07-point-read.md) | `src/read_path.rs` | flushdb-engine | tasks 4, 5 |
| 8 | [Range Read Path & Pagination](./08-range-read.md) | `src/read_path.rs` | flushdb-engine | tasks 4, 5 |
| 9 | [Recovery](./09-recovery.md) | `src/recovery.rs` | flushdb-engine | tasks 1, 2, 4 |
| 10 | [Compaction Triggers & Write Stalling](./10-compaction-triggers.md) | `src/compaction/scheduler.rs` | flushdb-engine | task 1 |
| 11 | [Compaction Executor](./11-compaction-executor.md) | `src/compaction/executor.rs` | flushdb-engine | tasks 1, 2, 4, 5, 10 |
| 12 | [Engine Orchestrator](./12-engine.md) | `src/engine.rs` | flushdb-engine | all above |

---

## Acceptance Criteria (Phase-Level)

All of these must pass before Phase 5 is considered complete:

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] Write 10M keys, kill process, recover from manifest + WAL — reads return correct data
- [ ] Manifest CAS correctly rejects conflicting concurrent updates
- [ ] Manifest snapshots work — recovery loads snapshot + applies deltas
- [ ] Flush pipeline: frozen memtable → SSTable → manifest update → WAL cleanup — full cycle
- [ ] Crash at any point in flush pipeline → recovery produces correct state
- [ ] Point reads return correct value across all layers (memtable, frozen, L0-L3)
- [ ] Point reads correctly return not-found for tombstoned keys
- [ ] Range reads merge-sort correctly across all layers
- [ ] Range tombstones shadow covered keys during reads
- [ ] Byte-based pagination with page tokens — resuming produces no gaps or duplicates
- [ ] Compaction keeps L0 bounded (never exceeds 12 files under sustained load)
- [ ] Trivial move optimization works for non-overlapping compaction inputs
- [ ] L0 write stalling kicks in progressively at 4/8/12 files
- [ ] Tombstones preserved through compaction until bottom level + TTL expired
- [ ] This is a **working embedded KV store**

---

## New Dependencies (Phase 5)

| Crate | Version | Purpose |
|-------|---------|---------|
| tokio-util | 0.7 | Codec framing, async stream helpers |
