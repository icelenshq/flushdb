# Task 7: OrderedKey — Version Key

**Crate:** `flushdb-types`
**File:** `src/ordered_key.rs`
**Depends on:** Task 1 (workspace)
**Estimated complexity:** S
**Design reference:** Phase 1 §1.7

---

## Goal

Implement the 12-byte system-generated version key returned to clients in write responses. OrderedKey is monotonically increasing, naturally sorted by time, and serves as the write version identifier.

---

## What to Build

### 7.1 OrderedKey Struct

Fixed-size 12-byte version key:

```
OrderedKey {
  timestamp_ms: u64   // 8 bytes — big-endian millisecond timestamp
  node_id:      u16   // 2 bytes — big-endian node identifier
  sequence:     u16   // 2 bytes — big-endian per-node sequence counter
}
```

**Total size:** 12 bytes, always.

**Derives:** `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Hash`, `Ord`, `PartialOrd`

### 7.2 Wire Format

```
Offset  Size   Field
0       8      timestamp_ms (big-endian u64)
8       2      node_id (big-endian u16)
10      2      sequence (big-endian u16)
```

All fields are big-endian so that raw byte comparison produces correct chronological ordering.

### 7.3 Ordering Guarantee

The `Ord` implementation MUST be equivalent to comparing the 12-byte big-endian encoding as raw bytes. This means:
1. Keys sort by `timestamp_ms` first
2. Then by `node_id` (tiebreaker for concurrent writes across nodes)
3. Then by `sequence` (tiebreaker for concurrent writes on same node within same millisecond)

This supports ~65K keys per millisecond per node with up to 65,536 nodes.

### 7.4 Construction Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(timestamp_ms: u64, node_id: u16, sequence: u16) -> Self` | Direct construction |
| `from_bytes` | `(bytes: &[u8]) -> FlushResult<Self>` | Parse from exactly 12 bytes. Returns `InvalidArgument` if length != 12. |

### 7.5 Accessor Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `timestamp_ms` | `(&self) -> u64` | Returns the timestamp component |
| `node_id` | `(&self) -> u16` | Returns the node identifier |
| `sequence` | `(&self) -> u16` | Returns the sequence counter |
| `to_bytes` | `(&self) -> [u8; 12]` | Serialize to fixed 12-byte big-endian array |

### 7.6 Design Notes

- **No generator in Phase 1** — the `OrderedKeyGenerator` (which manages the sequence counter and ensures monotonicity) is built in later phases when the write path exists. Phase 1 only defines the data type.
- **Big-endian everywhere** — use `byteorder` crate for encoding/decoding. This is critical for sort correctness.
- **`Copy` semantics** — 12 bytes is small enough to live on the stack, no heap allocation needed.

---

## Tests

**File:** `crates/flushdb-types/tests/ordered_key_tests.rs`

| Test | What It Validates |
|------|-------------------|
| `test_new_construction` | All fields stored correctly |
| `test_round_trip_bytes` | `from_bytes(key.to_bytes())` equals original |
| `test_from_bytes_wrong_length` | 11 and 13 bytes return `InvalidArgument` |
| `test_sort_by_timestamp` | `(t=100, n=0, s=0) < (t=200, n=0, s=0)` |
| `test_sort_by_node_id` | `(t=100, n=1, s=0) < (t=100, n=2, s=0)` |
| `test_sort_by_sequence` | `(t=100, n=1, s=1) < (t=100, n=1, s=2)` |
| `test_sort_timestamp_dominates` | `(t=100, n=999, s=999) < (t=101, n=0, s=0)` |
| `test_sort_matches_byte_comparison` | Sort a vec of OrderedKeys, compare against sorting their byte representations |
| `test_big_endian_encoding` | Manually check byte values: `timestamp_ms=0x0102030405060708` → bytes `[01, 02, 03, 04, 05, 06, 07, 08, ...]` |
| `test_max_values` | `(u64::MAX, u16::MAX, u16::MAX)` encodes and decodes correctly |
| `test_zero_values` | `(0, 0, 0)` encodes and decodes correctly |
| `test_copy_semantics` | OrderedKey is `Copy` — can be used after assignment |
| `test_accessors` | `timestamp_ms()`, `node_id()`, `sequence()` return correct values |

---

## Done When

- [ ] 12-byte big-endian encoding
- [ ] Sort order matches raw byte comparison
- [ ] Round-trip encode/decode lossless
- [ ] Timestamp dominates sort order
- [ ] `Copy` semantics (no heap allocation)
- [ ] All tests pass
