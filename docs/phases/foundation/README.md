# Phase 1: Foundation — Subtask Breakdown

## Overview

Phase 1 establishes every core type, trait, and protobuf definition the rest of the system builds on. After this phase, `cargo build --workspace` succeeds, all interfaces are locked, and every subsequent phase implements against these contracts.

**Crates:** `flushdb-types`, `flushdb-proto`
**Design references:** STORAGE_DESIGN.md §2, §3, §4, §19

---

## Subtask Execution Order

Tasks are ordered by dependency — each task depends only on tasks above it.

| # | Subtask | File | Crate | Dependencies |
|---|---------|------|-------|-------------|
| 1 | [Workspace Scaffolding](./01-workspace-scaffolding.md) | — | all | none |
| 2 | [FlushError — Unified Error Types](./02-flush-error.md) | `error.rs` | flushdb-types | task 1 |
| 3 | [CompositeKey — Key Encoding](./03-composite-key.md) | `composite_key.rs` | flushdb-types | task 1, 2 |
| 4 | [Item & EntryType — Data Model](./04-item-entry-type.md) | `item.rs`, `entry_type.rs` | flushdb-types | task 1 |
| 5 | [EntryValue — SSTable Entry Format](./05-entry-value.md) | `entry_value.rs` | flushdb-types | task 1 |
| 6 | [IdempotencyToken — Dedup Token](./06-idempotency-token.md) | `idempotency_token.rs` | flushdb-types | task 1, 2 |
| 7 | [OrderedKey — Version Key](./07-ordered-key.md) | `ordered_key.rs` | flushdb-types | task 1 |
| 8 | [MemtableEntry — Internal Entry](./08-memtable-entry.md) | `memtable_entry.rs` | flushdb-types | task 1, 3, 4, 5, 6 |
| 9 | [StorageBackend Trait](./09-storage-backend-trait.md) | `storage_backend.rs` | flushdb-types | task 1, 2 |
| 10 | [LocalFsBackend — Filesystem Implementation](./10-local-fs-backend.md) | `local_fs_backend.rs` | flushdb-types | task 1, 2, 9 |
| 11 | [Protobuf API Types](./11-protobuf-api-types.md) | `flushdb.proto` | flushdb-proto | task 1 |
| 12 | [Lib.rs Exports & Final Integration](./12-lib-exports-integration.md) | `lib.rs` | flushdb-types | all above |

---

## Acceptance Criteria (Phase-Level)

All of these must pass before Phase 1 is considered complete:

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] CompositeKey round-trip (encode/decode) correct for all cases
- [ ] CompositeKey sort order matches expected `memcmp` ordering
- [ ] CompositeKey rejects invalid inputs (oversized, null bytes in record_id)
- [ ] IdempotencyToken 24-byte encode/decode, all-zero detection
- [ ] OrderedKey 12-byte big-endian encoding, correct sort by time
- [ ] EntryValue::Inline and EntryValue::BlobRef both serialize/deserialize
- [ ] FlushError covers all listed variants with context
- [ ] LocalFsBackend passes all StorageBackend trait methods including conditional_put conflict
- [ ] Proto compiles and generates Rust types for all four operations

---

## New Dependencies (Phase 1)

| Crate | Version | Purpose |
|-------|---------|---------|
| byteorder | 1 | Big-endian encoding for OrderedKey, CompositeKey |

All other dependencies (tokio, bytes, thiserror, serde, tonic/prost, uuid, etc.) are already in workspace from initial setup.
