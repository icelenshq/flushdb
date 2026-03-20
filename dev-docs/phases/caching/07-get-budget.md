# Task 7: GET Budget

**Crate:** `flushdb-engine`
**File:** `src/cache/budget.rs`
**Depends on:** Task 2 (CachingBlockFetcher — budget wraps the fetcher)
**Estimated complexity:** S
**Design reference:** Phase 6 §8 (SSTable GET Budget)

---

## Goal

Cap the number of StorageBackend GETs per read operation to prevent pathological cases — such as a record spread across hundreds of L0 SSTables — from causing unbounded latency. When the budget is exceeded, the read returns partial results with a flag indicating incompleteness.

---

## What to Build

### 7.1 ReadBudget

A per-read-operation counter tracking remaining StorageBackend GETs:

```
ReadBudget {
    remaining: u32,
    initial: u32,
    exhausted: bool,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(budget: u32) -> Self` | Creates budget with `remaining = budget`, `initial = budget`, `exhausted = false` |
| `try_spend` | `(&mut self) -> bool` | If `remaining > 0`, decrements and returns true. If `remaining == 0`, sets `exhausted = true` and returns false. |
| `spend` | `(&mut self, count: u32)` | Decrements remaining by count (saturating). Sets exhausted if remaining reaches 0. |
| `remaining` | `(&self) -> u32` | Current remaining budget |
| `is_exhausted` | `(&self) -> bool` | Returns true if budget was exceeded at any point |
| `used` | `(&self) -> u32` | Returns `initial - remaining` |

### 7.2 Budget Integration in CachingBlockFetcher

The budget is tracked inside `CachingBlockFetcher` rather than in a separate wrapper. This ensures only actual StorageBackend GETs (cache misses) consume budget — cache hits are free. Add a budgeted fetch method to `CachingBlockFetcher`:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `fetch_block_budgeted` | `(&self, sst_path: &str, offset: u64, size: u32, compression: CompressionType, budget: &mut ReadBudget) -> FlushResult<Vec<BlockEntry>>` | Same as `fetch_block`, but on cache miss, calls `budget.try_spend()` before calling inner fetcher. Cache hits do not consume budget. Returns `FlushError::ResourceExhausted` if budget exhausted on a miss. |

This ensures only actual StorageBackend GETs count against the budget.

### 7.4 Read Path Integration

`ReadPath` methods accept an optional `ReadBudget`:

- `point_read`: Each `SSTableHandle::get()` call uses `fetch_block_budgeted`. If budget exhausted, the read returns the best result found so far (may be None even if the key exists in a not-yet-checked SSTable).
- `range_read`: Each block fetch uses `fetch_block_budgeted`. If budget exhausted mid-scan, the scan stops and returns entries collected so far with `is_partial: true`.

### 7.5 PartialReadIndicator

Extend `RangeReadResult` to indicate partial results:

```
RangeReadResult {
    entries: Vec<MergeEntry>,
    next_page_token: Option<PageToken>,
    total_bytes: usize,
    is_partial: bool,          // NEW: true if GET budget was exhausted
}
```

For `GetResult`, partiality is implicit — a `None` result when the budget was exhausted means "key might exist but we ran out of budget."

### 7.5 Breaking Change: RangeReadResult

Adding `is_partial: bool` to `RangeReadResult` is a breaking change to an existing public type. All existing construction sites (currently just `ReadPath::range_read()`) must be updated to include `is_partial: false`. Existing tests that construct `RangeReadResult` will also need updating.

### 7.6 Design Decisions

- **Budget counts StorageBackend GETs, not cache lookups:** A read that hits cache for all blocks should not be penalized. Only actual I/O matters for latency control.
- **Budget is per read operation, not global:** Each call to `engine.get()` or `engine.scan()` creates a fresh `ReadBudget`. No cross-request tracking.
- **ResourceExhausted is caught, not propagated:** The read path catches `ResourceExhausted` from the budgeted fetcher and converts it to a partial result, not an error returned to the caller. The caller sees `is_partial: true`, not an error.
- **Default budget of 8:** Empirically, a point read touches at most `L0_count + 3` SSTables (one per non-overlapping level), and bloom filters eliminate most. A budget of 8 allows a healthy read to complete while capping pathological cases.

### 7.7 Future-Proofing

- **SLO-aware budget (Phase 7):** The server extends GET budgets with deadline awareness. Design `ReadBudget` so it can later accept a `deadline: Option<Instant>` field that provides time-based cutoff in addition to count-based. Do NOT implement deadline logic yet — just ensure the struct can be extended.

---

## Tests

**File:** `crates/flushdb-engine/tests/get_budget_tests.rs`

### ReadBudget Unit Tests
| Test | What It Validates |
|------|-------------------|
| `test_budget_spend` | Budget of 3: try_spend 3 times succeeds, 4th returns false |
| `test_budget_exhausted_flag` | After budget runs out, is_exhausted returns true |
| `test_budget_used_tracking` | Budget of 8, spend 3, used() returns 3, remaining() returns 5 |
| `test_budget_spend_multiple` | spend(3) on budget of 5 → remaining is 2 |
| `test_budget_spend_saturating` | spend(10) on budget of 3 → remaining is 0, exhausted is true |

### Budgeted Fetch Tests
| Test | What It Validates |
|------|-------------------|
| `test_cache_hit_does_not_consume_budget` | Populate cache, fetch_block_budgeted returns data, budget.used() is 0 |
| `test_cache_miss_consumes_budget` | Fetch uncached block with budget 3, budget.used() is 1 after fetch |
| `test_budget_exhausted_returns_error` | Budget of 1, fetch 2 uncached blocks — second returns ResourceExhausted |

### Read Path Integration
| Test | What It Validates |
|------|-------------------|
| `test_point_read_within_budget` | Read key present in 1 SSTable with budget 8 — succeeds normally |
| `test_point_read_exhausts_budget` | Create 10 L0 SSTables with different keys, read with budget 2 — returns best result found within budget |
| `test_range_read_partial_on_budget` | Scan with budget 2, SSTable has 5 blocks — returns partial results with is_partial=true |
| `test_range_read_is_partial_flag` | Verify RangeReadResult.is_partial is false when budget not exhausted, true when it is |

---

## Done When

- [ ] `ReadBudget` correctly tracks and enforces GET count limits
- [ ] Cache hits do NOT consume budget — only StorageBackend calls count
- [ ] Budget exhaustion returns `ResourceExhausted` error from fetcher
- [ ] Read path catches budget exhaustion and converts to partial results
- [ ] `RangeReadResult.is_partial` flag correctly indicates incomplete reads
- [ ] Default budget of 8 is configured via `CacheConfig`
- [ ] All tests pass
