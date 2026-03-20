# Task 8: Public API & Lib Exports

**Crate:** `flushdb-engine`
**File:** `src/lib.rs`
**Depends on:** All previous tasks (1–7)
**Estimated complexity:** S
**Design reference:** CLAUDE.md (crate dependency graph), Phase 3 Future Work Considerations

---

## Goal

Wire up `flushdb-engine`'s `lib.rs` to export the public API surface for the memtable subsystem. Verify that all public types are correctly exported, all `Send` bounds hold, the crate compiles cleanly, and the sorted iterator interface is ready for the flush pipeline (Phase 5b).

---

## What to Build

### 8.1 Module Declarations in lib.rs

```rust
mod arena;
mod skiplist;
mod range_tombstone;
mod memtable;
mod memtable_list;
```

### 8.2 Public Re-exports

Export only the types that downstream crates need. Internal implementation details (Arena, SkipNode) stay private.

```rust
// Core memtable types
pub use memtable::{Memtable, MemtableConfig};
pub use memtable_list::MemtableList;

// Range tombstone (needed by read path in Phase 5c)
pub use range_tombstone::{RangeTombstone, RangeTombstoneIndex};

// Skip list iterator (needed by flush pipeline in Phase 5b)
pub use skiplist::SkipListIterator;
pub use skiplist::SkipListIntoIterator;
pub use skiplist::SkipNode;

// Dedup set (needed by server for dedup chain composition in Phase 7)
pub use memtable::DedupSet;
```

**What stays private:**
- `Arena` — internal allocation detail, not needed outside the crate
- `SkipList` — accessed only through `Memtable`
- Internal constants (`MAX_HEIGHT`, `BRANCHING_FACTOR`, `DEFAULT_BLOCK_SIZE`)

### 8.3 Crate Dependencies in Cargo.toml

Verify `flushdb-engine/Cargo.toml` has:

```toml
[dependencies]
flushdb-types = { path = "../flushdb-types" }
bytes = "1"
rand = "0.10"
tracing = "0.1"

[dev-dependencies]
tempfile = "3"
```

- `flushdb-types` — for `CompositeKey`, `MemtableEntry`, `EntryType`, `IdempotencyToken`, `FlushError`, `FlushResult`
- `bytes` — for `Bytes`
- `rand` — for skip list random height generation
- `tracing` — for structured logging (warnings on edge cases)

**No `flushdb-wal` dependency** — the memtable operates on `MemtableEntry` which is defined in `flushdb-types`. WAL → memtable conversion happens at the engine orchestration level (Phase 5+), not in the memtable itself.

### 8.4 Flush Pipeline Iterator Contract

Verify that the flush pipeline (Phase 5b) can use the memtable like this:

```rust
// Phase 5b will do:
let frozen: Memtable = memtable_list.pop_oldest_frozen().unwrap();
for node in frozen.iter() {
    // node is &SkipNode with key, value, metadata, entry_type, sequence_number
    // write to SSTable
}
```

The `into_iter()` variant consumes the memtable:
```rust
let frozen: Memtable = memtable_list.pop_oldest_frozen().unwrap();
let skiplist = frozen.into_skiplist();  // transfer ownership
for node in skiplist.into_iter() {
    // owned SkipNode
}
```

Add this method to `Memtable`:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `into_skiplist` | `(self) -> SkipList` | Consumes memtable, returns the inner skip list. Used by flush pipeline. Only valid on frozen memtables — panics if `!self.frozen` (debug assert). |
| `range_tombstones` | `(&self) -> &RangeTombstoneIndex` | Borrow the range tombstone index. Needed by flush to write tombstones to SSTable. |

### 8.5 Compile-Time Assertions

Add to `lib.rs`:

```rust
#[cfg(test)]
mod send_assertions {
    use super::*;

    fn _assert_send<T: Send>() {}

    fn _assert_types_are_send() {
        _assert_send::<Memtable>();
        _assert_send::<MemtableList>();
        _assert_send::<SkipNode>();
        _assert_send::<RangeTombstone>();
        _assert_send::<RangeTombstoneIndex>();
        _assert_send::<DedupSet>();
    }
}
```

### 8.6 Documentation Verification

Verify that public API items have clear, concise doc comments where behavior is non-obvious. Per CLAUDE.md rules, do NOT add doc comments that restate what the signature already says. Only document:
- Non-obvious behavior (e.g., "returns None if covered by range tombstone")
- Invariants callers must uphold (e.g., "only call on frozen memtables")
- Error conditions

---

## Tests

**File:** `crates/flushdb-engine/tests/lib_export_tests.rs`

### Export Verification Tests
| Test | What It Validates |
|------|-------------------|
| `test_public_types_importable` | All public types can be imported from `flushdb_engine::*` |
| `test_memtable_config_default` | `MemtableConfig::default()` returns correct defaults (64 MB size, 3 max frozen) |
| `test_into_skiplist_on_frozen` | Frozen memtable's `into_skiplist()` returns usable skip list |
| `test_into_skiplist_panics_on_active` | Active (unfrozen) memtable's `into_skiplist()` panics in debug |
| `test_range_tombstones_accessor` | `range_tombstones()` returns reference to the index |

### End-to-End Integration Tests
| Test | What It Validates |
|------|-------------------|
| `test_full_lifecycle` | Create list → insert entries → freeze → insert more → read from both → pop frozen → iterate for flush |
| `test_flush_pipeline_simulation` | Create list → insert 1K entries → freeze → pop_oldest_frozen → into_skiplist → into_iter → collect all entries → verify sorted order and completeness |

### Build Verification
| Test | What It Validates |
|------|-------------------|
| `test_workspace_builds` | `cargo build --workspace` succeeds (verified manually, not a Rust test) |
| `test_clippy_clean` | `cargo clippy --workspace` passes (verified manually) |

---

## Done When

- [ ] `flushdb-engine` lib.rs exports all necessary public types
- [ ] Internal types (Arena, SkipList) are NOT public
- [ ] All public types satisfy `Send` bound (compile-time verified)
- [ ] `into_skiplist()` and `range_tombstones()` enable flush pipeline contract
- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] End-to-end lifecycle test passes (insert → freeze → read → pop → flush iterate)
