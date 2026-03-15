# Task 12: Lib.rs Exports & Final Integration

**Crate:** `flushdb-types`
**File:** `src/lib.rs`
**Depends on:** All previous tasks (2-10)
**Estimated complexity:** S

---

## Goal

Wire up all modules in `flushdb-types/src/lib.rs`, ensure clean public API exports, and verify the entire workspace builds and passes all tests with zero warnings.

---

## What to Build

### 12.1 Module Declarations

`crates/flushdb-types/src/lib.rs` should declare all modules and re-export key types:

```
mod composite_key;
mod entry_type;
mod entry_value;
mod error;
mod idempotency_token;
mod item;
mod local_fs_backend;
mod memtable_entry;
mod ordered_key;
mod storage_backend;

// Re-export public API
pub use composite_key::CompositeKey;
pub use entry_type::EntryType;
pub use entry_value::EntryValue;
pub use error::{FlushError, FlushResult};
pub use idempotency_token::IdempotencyToken;
pub use item::Item;
pub use local_fs_backend::LocalFsBackend;
pub use memtable_entry::MemtableEntry;
pub use ordered_key::OrderedKey;
pub use storage_backend::StorageBackend;
```

### 12.2 Public API Surface

The following types should be importable as `flushdb_types::TypeName`:

| Type | Category |
|------|----------|
| `CompositeKey` | Data model |
| `Item` | Data model |
| `EntryType` | Data model |
| `EntryValue` | Data model |
| `MemtableEntry` | Data model |
| `IdempotencyToken` | Data model |
| `OrderedKey` | Data model |
| `FlushError` | Error handling |
| `FlushResult` | Error handling |
| `StorageBackend` | Traits |
| `LocalFsBackend` | Implementations |

### 12.3 Constants

Re-export key constants at crate level:

| Constant | Value | Source |
|----------|-------|--------|
| `MAX_RECORD_ID_LEN` | 256 | `composite_key` |
| `MAX_ITEM_KEY_LEN` | 4096 | `composite_key` |
| `MAX_COMPOSITE_KEY_LEN` | 4353 | `composite_key` |
| `SEPARATOR` | 0x00 | `composite_key` |
| `RANGE_TOMBSTONE_PREFIX` | 0xFF | `composite_key` |

### 12.4 Final Integration Checks

Run the full pre-commit checklist:

```bash
cargo build --workspace          # zero warnings
cargo test --workspace           # all tests pass
cargo clippy --workspace         # no warnings
```

### 12.5 Cross-Crate Verification

Verify that shell crates can import from `flushdb-types`:
- `flushdb-wal` can `use flushdb_types::{CompositeKey, MemtableEntry, FlushError};`
- `flushdb-engine` can `use flushdb_types::{StorageBackend, EntryValue};`
- `flushdb-server` can reference `flushdb-proto` generated types

### 12.6 What NOT to Export

- Internal helper functions used only within a module
- Test-only utilities
- Implementation details of `LocalFsBackend` (only the struct and its constructor)

---

## Acceptance Criteria (Phase 1 Complete)

This is the final task — when it passes, Phase 1 is done.

- [ ] `cargo build --workspace` — zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] All types importable via `flushdb_types::*`
- [ ] Proto types importable via `flushdb_proto::flushdb::v1::*`
- [ ] Shell crates compile with their dependency on `flushdb-types`
- [ ] No `todo!()`, `unimplemented!()`, or `unsafe` anywhere in workspace
- [ ] No `unwrap()` in production code paths
- [ ] Every public function has tests in `tests/` directory
