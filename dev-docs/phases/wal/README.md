# Phase 2: WAL — Durable Local Writes — Subtask Breakdown

## Overview

Build a crash-safe, segment-based write-ahead log that durably records every write before it's applied. The WAL provides the durability guarantee between writes arriving and being flushed to S3 — if the process crashes, replay from the WAL reconstructs the correct state.

**Crates:** `flushdb-wal`
**Design references:** STORAGE_DESIGN.md §5

---

## Subtask Execution Order

Tasks are ordered by dependency — each task depends only on tasks above it.

| # | Subtask | File | Crate | Dependencies |
|---|---------|------|-------|-------------|
| 1 | [WAL Configuration & Constants](./01-wal-configuration.md) | `src/config.rs` | flushdb-wal | none |
| 2 | [WAL Entry Wire Format](./02-wal-entry-wire-format.md) | `src/entry.rs` | flushdb-wal | task 1 |
| 3 | [Segment Header](./03-segment-header.md) | `src/segment_header.rs` | flushdb-wal | task 1 |
| 4 | [Segment Writer](./04-segment-writer.md) | `src/segment_writer.rs` | flushdb-wal | task 1, 2, 3 |
| 5 | [Segment Reader](./05-segment-reader.md) | `src/segment_reader.rs` | flushdb-wal | task 2, 3 |
| 6 | [WAL Writer](./06-wal-writer.md) | `src/wal_writer.rs` | flushdb-wal | task 4, 5 |
| 7 | [WAL Reader & Recovery](./07-wal-reader-recovery.md) | `src/wal_reader.rs` | flushdb-wal | task 5 |
| 8 | [Group Commit](./08-group-commit.md) | `src/group_commit.rs` | flushdb-wal | task 6 |
| 9 | [Dirty Segment Tracker](./09-dirty-segment-tracker.md) | `src/dirty_tracker.rs` | flushdb-wal | task 1 |
| 10 | [WAL Manager & Backpressure](./10-wal-manager.md) | `src/wal_manager.rs` | flushdb-wal | task 6, 7, 8, 9 |

---

## Acceptance Criteria (Phase-Level)

All of these must pass before Phase 2 is considered complete:

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] Write 10K entries across 3+ segments — all entries readable with correct data
- [ ] Kill (simulate crash) mid-write — replay recovers all valid entries, discards partial tail entry
- [ ] CRC validation detects corrupted entries mid-segment
- [ ] Segment rotation triggers at 32MB boundary
- [ ] Group commit measurably batches fsyncs (N writes → fewer than N fsync calls)
- [ ] All writes in a batch are notified only after fsync completes
- [ ] Dirty segment tracking correctly prevents premature segment deletion
- [ ] Segments are only deleted when their dirty map is empty
- [ ] WAL backpressure returns `ResourceExhausted` when size exceeds threshold

---

## New Dependencies (Phase 2)

| Crate | Version | Purpose |
|-------|---------|---------|
| bytes | workspace | Direct dependency for WAL entry buffers |
| tokio | workspace | Async runtime, timers for group commit, spawn_blocking for sync I/O |

All other dependencies (crc32fast, tracing, flushdb-types) are already declared in `flushdb-wal/Cargo.toml`.
