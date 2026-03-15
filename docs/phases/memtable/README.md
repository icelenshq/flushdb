# Phase 3: Memtable — Subtask Breakdown

## Overview

Phase 3 builds the in-memory sorted data structure that sits between the WAL and SSTables. The memtable accepts writes, serves point and range reads with correct merge semantics, supports range tombstones, enforces idempotency dedup, and implements freeze/swap for background flush. This is the first phase in `flushdb-engine`.

**Crates:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §6, §4 (shard-per-core)

---

## Subtask Execution Order

Tasks are ordered by dependency — each task depends only on tasks above it.

| # | Subtask | File | Crate | Dependencies |
|---|---------|------|-------|-------------|
| 1 | [Arena Allocator](./01-arena-allocator.md) | `src/arena.rs` | flushdb-engine | none |
| 2 | [Skip List Core](./02-skip-list-core.md) | `src/skiplist.rs` | flushdb-engine | task 1 |
| 3 | [Skip List Iteration & Range Scans](./03-skip-list-iteration.md) | `src/skiplist.rs` | flushdb-engine | task 2 |
| 4 | [Range Tombstone Index](./04-range-tombstone-index.md) | `src/range_tombstone.rs` | flushdb-engine | none |
| 5 | [Memtable — Core Insert & Read](./05-memtable-core.md) | `src/memtable.rs` | flushdb-engine | task 2, 3, 4 |
| 6 | [Idempotency Deduplication](./06-idempotency-dedup.md) | `src/memtable.rs` | flushdb-engine | task 5 |
| 7 | [Freeze, Swap & Frozen Memtable List](./07-freeze-swap.md) | `src/memtable.rs`, `src/memtable_list.rs` | flushdb-engine | task 5, 6 |
| 8 | [Public API & Lib Exports](./08-public-api-exports.md) | `src/lib.rs` | flushdb-engine | all above |

---

## Acceptance Criteria (Phase-Level)

All of these must pass before Phase 3 is considered complete:

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] 100K random entries inserted, then iterated — output matches BTreeMap oracle (same keys, same order)
- [ ] Point lookup finds exact keys, returns most recent value when key has multiple versions
- [ ] Range scan with start/end boundaries returns correct subset
- [ ] Full record scan returns all items for a given record_id, stops at record boundary
- [ ] Range tombstones correctly shadow covered keys with lower sequence numbers
- [ ] Range tombstones do NOT shadow keys with higher sequence numbers
- [ ] Freeze prevents further inserts to the frozen memtable
- [ ] Freeze triggers on both size threshold (64 MB) and time threshold (5 min)
- [ ] Frozen memtable remains readable after freeze
- [ ] Dedup rejects duplicate tokens across active and frozen memtables
- [ ] All-zero tokens bypass dedup and are always applied
- [ ] Backpressure stalls writes when frozen memtable count exceeds limit
- [ ] Frozen memtable is `Send` (can be moved to flush task)
- [ ] Sorted iterator yields entries in `CompositeKey` order for flush pipeline

---

## New Dependencies (Phase 3)

None — uses `rand` (for skip list height) already in workspace.
