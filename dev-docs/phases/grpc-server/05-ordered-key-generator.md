# Task 5: OrderedKey Generator

**Crate:** `flushdb-server`
**File:** `src/version_generator.rs`
**Depends on:** Nothing (uses `OrderedKey` type from flushdb-types)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §17 (Ordered Key Generation), Phase 7 §6

---

## Goal

Implement server-side version key generation that produces monotonically increasing 12-byte `OrderedKey` values. Each write operation returns a version to the client. The generator is designed for single-node use now, with a pluggable `node_id` assignment strategy for future cluster mode.

---

## What to Build

### 5.1 VersionGenerator Struct

```
VersionGenerator {
    node_id:        u16,
    sequence:       AtomicU16,
    last_timestamp:  AtomicU64,
}
```

**Thread-safety:** Uses atomics for sequence and timestamp. The generator is `Send + Sync` and can be shared across async tasks via `Arc`.

### 5.2 Construction

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(node_id: u16) -> Self` | Creates generator with given node_id, sequence starts at 0 |
| `with_node_id_from_env` | `() -> FlushResult<Self>` | Reads `FLUSHDB_NODE_ID` env var, parses as u16 |

### 5.3 Version Generation

| Method | Signature | Behavior |
|--------|-----------|----------|
| `next_version` | `(&self) -> OrderedKey` | Generates next monotonically increasing OrderedKey |

**Algorithm:**
1. Get current time in milliseconds (`SystemTime::now()`)
2. Load `last_timestamp` atomically
3. If `current_ms > last_timestamp`: store `current_ms` as new timestamp, reset sequence to 0
4. If `current_ms == last_timestamp`: increment sequence atomically
5. If `current_ms < last_timestamp` (clock skew): use `last_timestamp`, increment sequence
6. If sequence overflows (wraps past `u16::MAX`): spin-wait until next millisecond, then reset sequence to 0

**Monotonicity guarantee:** The resulting `OrderedKey` is always >= the previous one when comparing by the natural 12-byte big-endian order. This holds even under clock skew because the generator never decreases the timestamp.

### 5.4 Node ID Assignment

For single-node mode, `node_id` is configured directly. Future cluster mode will use S3 CAS for a monotonic node ID counter at `cluster/node-id-counter`.

The `node_id` field in `OrderedKey` is already defined (2 bytes, u16). This task just provides a way to set it.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `node_id` | `(&self) -> u16` | Returns the configured node_id |

### 5.5 Design Decisions

- **Atomics, not mutex** — This is a hot-path component. `AtomicU16` and `AtomicU64` with `Ordering::Relaxed` for sequence and `Ordering::AcqRel` for timestamp avoid any locking.
- **~65K keys/ms/node** — u16 sequence allows 65,536 versions per millisecond per node. If exceeded, the generator spin-waits for the next millisecond rather than failing.
- **Clock skew tolerance** — If the system clock moves backward, the generator continues using the last-seen timestamp. Monotonicity is never violated.

---

## Tests

**File:** `crates/flushdb-server/tests/version_generator_tests.rs`

### Monotonicity Tests
| Test | What It Validates |
|------|-------------------|
| `test_sequential_versions_monotonic` | 1000 sequential calls produce strictly increasing OrderedKeys |
| `test_versions_sorted_by_bytes` | Generated OrderedKeys sort correctly by their 12-byte big-endian encoding |
| `test_same_millisecond_different_sequence` | Multiple calls within 1ms produce same timestamp but incrementing sequence |

### Node ID Tests
| Test | What It Validates |
|------|-------------------|
| `test_node_id_preserved` | Generated OrderedKey contains the configured node_id |
| `test_different_node_ids` | Two generators with different node_ids produce keys with different node_id fields |

### Concurrency Tests
| Test | What It Validates |
|------|-------------------|
| `test_concurrent_generation_unique` | 10 tasks generating 1000 keys each — all 10,000 keys are unique |
| `test_concurrent_generation_monotonic` | Per-task key sequences are monotonically increasing |

### Overflow Tests
| Test | What It Validates |
|------|-------------------|
| `test_sequence_overflow_advances_timestamp` | After u16::MAX calls in same millisecond, timestamp advances |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_zero_node_id` | node_id=0 works correctly |
| `test_max_node_id` | node_id=65535 works correctly |

---

## Done When

- [ ] `VersionGenerator` produces monotonically increasing `OrderedKey` values
- [ ] Sequence resets correctly on timestamp advancement
- [ ] Clock skew never violates monotonicity
- [ ] Concurrent access produces unique keys (no duplicates across tasks)
- [ ] Sequence overflow causes timestamp advancement (spin-wait), not failure
- [ ] Generator is `Send + Sync` (usable with `Arc`)
- [ ] All tests pass
