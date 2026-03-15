# Task 6: Idempotency Deduplication

**Crate:** `flushdb-engine`
**File:** `src/memtable.rs`
**Depends on:** Task 5 (Memtable Core)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §6, Phase 3 §6

---

## Goal

Add write deduplication to the memtable using idempotency tokens. The memtable maintains a set of tokens for all entries in the current (and frozen) memtables. Before applying a write, the token is checked — if found, the write is rejected as a duplicate. All-zero tokens bypass dedup entirely. This is the hot-path first check in the dedup chain (memtable → SSTable dedup blocks in Phase 4+).

---

## What to Build

### 6.1 DedupSet

A dedicated type wrapping `HashSet<IdempotencyToken>` for the memtable's token set:

```
DedupSet {
    tokens: HashSet<IdempotencyToken>
}
```

**Design decisions:**
- **Separate struct** rather than bare `HashSet` — provides a clean interface for the dedup chain and allows future optimization (e.g., bloom filter pre-check) without changing callers.
- **`HashSet<IdempotencyToken>`** — `IdempotencyToken` already implements `Hash` and `Eq`. The full 24-byte token is the key.
- **No capacity hints** — the set grows organically. A typical memtable at 64 MB holds ~100K–500K entries, so the hash set is ~10–50 MB in the worst case (24 bytes × entry count × load factor). This is acceptable.

### 6.2 DedupSet Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty dedup set |
| `contains` | `(&self, token: &IdempotencyToken) -> bool` | Returns true if token is in the set. Always returns false for all-zero tokens (`token.is_none()`). |
| `insert` | `(&mut self, token: IdempotencyToken)` | Adds token to the set. No-op for all-zero tokens. |
| `len` | `(&self) -> usize` | Number of tokens in the set |

### 6.3 Memtable Integration

Add a `dedup_set: DedupSet` field to the `Memtable` struct.

Modify `Memtable::insert()` to check for duplicates before inserting:

```
insert(entry):
  if self.frozen:
    return Err(ResourceExhausted)

  // Dedup check (skip for all-zero tokens)
  if dedup_set.contains(&entry.idempotency_key):
    return Err(FlushError::DuplicateToken {
      token: format!("{:?}", entry.idempotency_key)
    })

  // Assign sequence number and insert (existing logic from Task 5)
  seq = assign_sequence(entry)
  skiplist.insert(entry)
  if entry is RangeDelete: update range_tombstones

  // Track token for future dedup
  dedup_set.insert(entry.idempotency_key)

  return Ok(seq)
```

### 6.4 Cross-Memtable Dedup Check

The dedup check must span active AND frozen memtables. The `MemtableList` (Task 7) will compose dedup checks across all memtables. For this task, expose the dedup set for external checking:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `check_dedup` | `(&self, token: &IdempotencyToken) -> bool` | Returns true if the token exists in this memtable's dedup set. Returns false for all-zero tokens. |

The `MemtableList` (Task 7) will call `check_dedup` on each memtable in the chain.

### 6.5 Dedup Chain Design (Interface Seam for Future Phases)

The full dedup chain is: active memtable → frozen memtables → SSTable dedup blocks.

For now, expose a trait-like pattern that future phases can extend:

```
// On Memtable:
check_dedup(&self, token: &IdempotencyToken) -> bool

// MemtableList (Task 7) will implement:
check_dedup_all(&self, token: &IdempotencyToken) -> bool
  // checks active + all frozen memtables
```

Phase 4+ will add SSTable dedup block checking after the memtable chain returns `false`.

### 6.6 All-Zero Token Semantics

- `IdempotencyToken::is_none()` returns true for all-zero tokens
- All-zero tokens ALWAYS bypass dedup — they are never inserted into the dedup set and never match during contains checks
- This allows fire-and-forget writes that are always applied

---

## Tests

**File:** `crates/flushdb-engine/tests/memtable_dedup_tests.rs`

### DedupSet Unit Tests
| Test | What It Validates |
|------|-------------------|
| `test_dedup_set_insert_and_contains` | Insert token, contains returns true |
| `test_dedup_set_missing_token` | Contains returns false for absent token |
| `test_dedup_set_all_zero_token_bypasses` | All-zero token: contains returns false, insert is no-op |
| `test_dedup_set_len` | len reflects actual distinct tokens inserted |
| `test_dedup_set_duplicate_insert_idempotent` | Inserting same token twice doesn't change len |

### Memtable Dedup Integration Tests
| Test | What It Validates |
|------|-------------------|
| `test_insert_rejects_duplicate_token` | Insert entry with token T, insert another entry with same token T — second returns `DuplicateToken` error |
| `test_insert_different_tokens_both_succeed` | Two entries with different tokens — both succeed |
| `test_insert_all_zero_token_never_rejected` | Multiple entries with all-zero token — all succeed |
| `test_insert_all_zero_then_real_token` | All-zero entry succeeds, then entry with real token succeeds |
| `test_dedup_across_different_keys` | Same token used for different CompositeKeys — second is still rejected (token-based, not key-based) |
| `test_check_dedup_returns_true_for_inserted` | check_dedup returns true after entry with that token was inserted |
| `test_check_dedup_returns_false_for_missing` | check_dedup returns false for token not in this memtable |
| `test_check_dedup_all_zero_always_false` | check_dedup returns false for all-zero token even after all-zero inserts |

### Error Variant Tests
| Test | What It Validates |
|------|-------------------|
| `test_duplicate_token_error_contains_token_info` | DuplicateToken error includes token representation for debugging |
| `test_duplicate_token_does_not_assign_sequence` | Rejected duplicate doesn't consume a sequence number |

### Scale Test
| Test | What It Validates |
|------|-------------------|
| `test_10k_unique_tokens_all_accepted` | 10K entries with unique tokens — all succeed, dedup set has 10K entries |
| `test_replay_same_10k_tokens_all_rejected` | Replay the same 10K tokens — all rejected as duplicates |

---

## Done When

- [ ] Duplicate tokens are rejected with `FlushError::DuplicateToken`
- [ ] All-zero tokens bypass dedup entirely (always accepted, never stored)
- [ ] Rejected duplicates do not consume sequence numbers
- [ ] `check_dedup()` exposes dedup state for cross-memtable checking (Task 7)
- [ ] DedupSet is a clean abstraction (not bare HashSet on Memtable)
- [ ] All tests pass
