# Phase 3: Memtable — In-Memory Sorted Store

**Complexity: L**
**Crate:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §6

---

## Goal

Build a fast, single-owner, in-memory sorted data structure that accepts writes from the WAL and serves reads with correct merge semantics. The memtable is the first layer reads check and the source of data for SSTable flushes.

---

## 1. Skip List

The memtable is a skip list owned by a single CPU core — no concurrent access from other cores (shard-per-core model).

**Properties:**
- **Max height:** 12 levels (supports ~4 billion entries efficiently)
- **Promotion probability:** 1/4 (each level has 1/4 the entries of the level below)
- **Node allocation:** Arena-based — all nodes allocated from contiguous memory, improving cache locality

**Concurrency model:**
- **Single-writer:** The owning core is the sole writer. No CAS loops, no contention. Inserts are plain pointer writes.
- **Reads from owning core:** Direct traversal, zero synchronization.
- No `Arc<Mutex<>>` anywhere — this is a hard rule from the project's shard-per-core architecture.

**Operations:**
- **Insert:** Standard skip list insert — find position at each level, splice in new node. O(log n) expected.
- **Point lookup:** Traverse from top level down. O(log n) expected.
- **Range scan:** Find start position, iterate forward at bottom level. O(log n + k) where k = result count.
- **Full record scan:** Seek to `(record_id, MIN_KEY)`, scan until `record_id` changes.

**Key ordering:** Entries sorted by `CompositeKey` (memcmp). Within a record, items are sorted by item key. Across records, sorted by record ID.

---

## 2. Arena Allocator

Each memtable owns a bump allocator with pre-allocated blocks (default 1 MB each):

```
Arena {
  blocks: Vec<Box<[u8; 1_048_576]>>   // 1 MB blocks
  current_offset: usize               // offset within current block (plain integer — single-writer)
  total_allocated: usize              // running total for threshold check
}
```

**Key properties:**
- **Allocation:** Bump the offset, return slice. If current block full, allocate a new one.
- **Deallocation:** O(1) — drop all blocks when the memtable is released after SSTable flush. No per-entry freeing.
- **Size metric:** `total_allocated` is the memtable size used for freeze threshold checks.
- **No atomics needed:** Single-writer model means offset and total are plain integers.

---

## 3. Range Tombstone Index

The memtable maintains a secondary index for range tombstones, separate from the main skip list:

```
RangeTombstoneIndex {
  tombstones: Vec<RangeTombstone>   // sorted by (record_id, start_key)
}

RangeTombstone {
  record_id:       Bytes
  start_key:       Bytes     // inclusive
  end_key:         Bytes     // exclusive
  sequence_number: u64
}
```

**Usage during reads:**
- For every point lookup result, check whether the target key falls within a range tombstone that has a **higher** sequence number than the found entry
- The `covers(record_id, item_key, found_sequence) -> bool` method determines if a range tombstone shadows a given entry
- Tombstones are kept sorted for efficient binary search by `(record_id, start_key)`

**Insertion:** When a `RANGE_DELETE` entry arrives, add it to both the skip list (for SSTable persistence) and the range tombstone index (for read-time filtering).

---

## 4. Freeze and Swap

When the memtable reaches capacity, it is frozen and replaced:

**Triggers (whichever comes first):**
- **Size threshold:** `total_allocated >= memtable_size_threshold` (default 64 MB)
- **Time threshold:** 5 minutes since last freeze (bounds the unflushed window for low-throughput partitions — without it, a partition writing 1KB/s would take ~18 hours to reach 64 MB)

**Freeze protocol:**
1. **Swap:** Replace the active memtable pointer with a new empty memtable. Plain pointer swap on the owning core. The old memtable becomes "frozen."
2. **Frozen memtable list:** The frozen memtable is pushed onto a read-only list. Reads still check frozen memtables (newest first).
3. **Flush trigger:** A background flush task is notified.
4. **Immutability:** No further inserts to the frozen memtable. Insert attempts must go to the new active memtable.

**Critical invariant:** Between freeze and flush completion, the frozen memtable MUST remain accessible for reads. It is only released after its SSTable is confirmed on S3 and the manifest is updated.

**Backpressure:** If N frozen memtables accumulate (default N=3, meaning 192 MB pinned), writes are rejected with backpressure error.

---

## 5. Sequence Number Assignment

Each write gets a monotonically increasing 64-bit sequence number from a per-partition counter:

- **Initialization:** From the WAL's last sequence number on startup (or manifest's `last_flushed_sequence` during recovery)
- **Incremented:** For each write
- **Written to:** Both WAL entries and memtable entries

**Purposes:**
1. **Ordering:** When the same composite key appears multiple times, the highest sequence number wins
2. **WAL recovery:** Replay entries with sequence numbers > manifest's `last_flushed_sequence`

---

## 6. Idempotency Deduplication

The memtable participates in write deduplication:

- **Active memtable:** Maintains a `HashSet<IdempotencyToken>` of all tokens in the current memtable
- **Frozen memtables:** Their token sets remain accessible for dedup checks
- **Dedup check on write:** Before applying a write, check if its token exists in the active memtable's set OR any frozen memtable's set. If found, reject as duplicate.
- **All-zero token bypass:** Tokens that are all zeros skip dedup entirely — they are always applied.

The unflushed window (active + frozen memtables) covers the most common retry scenario: retries within seconds. Longer-term dedup is handled by the SSTable dedup block (Phase 4+).

---

## 7. WAL Backpressure Integration

The memtable layer enforces WAL size limits:

- Track total WAL size on disk
- If WAL exceeds `max_wal_size` (default 256 MB), stall writes to that partition with `ResourceExhausted`
- Resume writes once a flush completes and WAL segments are cleaned up

This prevents local disk exhaustion during prolonged S3 unavailability.

---

## New Dependencies

None — uses `rand` (for skip list height) already in workspace.

---

## Future Work Considerations

When building Phase 3, keep the following downstream dependencies in mind:

| What You're Building | Who Needs It Later | What To Watch For |
|---------------------|-------------------|-------------------|
| **Skip list iteration** | Flush pipeline (P5b), SSTable writer (P4) | The flush pipeline iterates the frozen memtable in sorted order to build SSTables. Expose a sorted iterator that yields entries in `CompositeKey` order — this is the SSTableWriter's input interface. |
| **Freeze/swap mechanism** | Flush pipeline (P5b) | The flush pipeline is triggered by freeze. Design freeze to produce an immutable snapshot that can be handed to a background flush task. The frozen memtable must be `Send` so it can move to a flush thread/task. |
| **Range tombstone index** | Merge-read path (P5c) | The read path checks range tombstones across active memtable, frozen memtables, AND SSTables. The `covers()` API should be reusable — consider extracting the range tombstone check into a shared interface that SSTable range tombstones can also implement. |
| **Idempotency dedup** | SSTable dedup blocks (P4), Server dedup (P7) | Memtable dedup is the hot-path first check. The server will also check SSTable dedup blocks for tokens not found in memtables. Design the dedup check as a chain: memtable → SSTable, so the server can compose them without knowing internals. |
| **Frozen memtable list** | Read path merge order (P5c) | Reads check active → frozen-1 → frozen-2 → SSTables. The frozen list must support concurrent reads (from the read path) while the flush pipeline removes flushed entries. Since this is single-owner per core, a simple `Vec` with index management works — but the read path must iterate newest-first. |
| **Backpressure signals** | Server error responses (P7) | The server translates `ResourceExhausted` into gRPC `RESOURCE_EXHAUSTED` with a `retry-after` hint. Include enough context in the error (e.g., current frozen count, threshold) for the server to compute a meaningful retry delay. |
| **Arena allocator** | Shard-per-core scheduler (future) | The arena's memory is pinned to the owning core. Future shard-per-core scheduling assumes no cross-core memory sharing. Keep allocations core-local — no shared arenas across memtables. |

---

## Done When

- 100K random entries inserted, then iterated — output matches BTreeMap oracle (same keys, same order)
- Point lookup finds exact keys, returns most recent value when key has multiple versions
- Range scan with start/end boundaries returns correct subset
- Full record scan returns all items for a given record_id, stops at record boundary
- Range tombstones correctly shadow covered keys with lower sequence numbers
- Range tombstones do NOT shadow keys with higher sequence numbers
- Freeze prevents further inserts to the frozen memtable
- Freeze triggers on both size threshold (64 MB) and time threshold (5 min)
- Frozen memtable remains readable after freeze
- Dedup rejects duplicate tokens across active and frozen memtables
- All-zero tokens bypass dedup and are always applied
- Backpressure stalls writes when WAL exceeds size threshold
