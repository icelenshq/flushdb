# Phase 2: WAL — Durable Local Writes

**Complexity: L**
**Crate:** `flushdb-wal`
**Design references:** STORAGE_DESIGN.md §5

---

## Goal

Build a crash-safe, segment-based write-ahead log that durably records every write before it's applied. The WAL is the safety net — if the process crashes at any point, replay from the WAL reconstructs the correct state.

---

## 1. Segment-Based Architecture

The WAL is not a single file — it is a sequence of **segments**, each a fixed-size file (default 32 MB) on local disk.

```
wal/
  partition-{id}/
    segment-000000000001.wal    # oldest active segment
    segment-000000000002.wal
    segment-000000000003.wal    # current segment (append target)
```

**Why segments instead of one file:**
- **Truncation is file deletion, not file rewriting.** After a flush confirms on S3, entire segments are deleted rather than truncating the middle of a file.
- Each segment can be independently fsynced and rotated.
- Segment numbers are monotonically increasing 12-digit zero-padded integers. Gaps in numbering are fine (deleted segments leave gaps).

**Segment rotation:** When the current segment reaches `segment_size_target` (32 MB), the writer opens a new segment with the next number and switches to it. The old segment remains open for reads until all its generations are flushed.

---

## 2. Entry Wire Format

Each WAL entry is length-prefixed and CRC32-protected:

```
┌─────────────────────────────────────────────────────┐
│ entry_length: uint32 (little-endian)                │  4 bytes
├─────────────────────────────────────────────────────┤
│ sequence_number: uint64 (little-endian)             │  8 bytes
│ entry_type: uint8 (0=PUT, 1=DELETE, 2=RANGE_DELETE) │  1 byte
│ namespace_len: uint16                               │  2 bytes
│ namespace: bytes                                    │  variable
│ record_id_len: uint16                               │  2 bytes
│ record_id: bytes                                    │  variable
│ item_key_len: uint16                                │  2 bytes
│ item_key: bytes                                     │  variable
│ item_value_len: uint32                              │  4 bytes
│ item_value: bytes                                   │  variable
│ item_metadata_len: uint16                           │  2 bytes
│ item_metadata: bytes                                │  variable
│ idempotency_token: bytes                            │  24 bytes (fixed)
├─────────────────────────────────────────────────────┤
│ crc32: uint32 (over all preceding bytes in entry)   │  4 bytes
└─────────────────────────────────────────────────────┘
```

**Entry type semantics:**
- `PUT (0)`: `item_key` is the key, `item_value` is the value
- `DELETE (1)`: `item_key` is the key to delete, `item_value` is empty (length 0)
- `RANGE_DELETE (2)`: `item_key` is range start (inclusive), `item_value` encodes range end (exclusive), both scoped to `record_id`

**CRC scope:** The CRC32 covers all bytes from `sequence_number` through `idempotency_token` (everything between `entry_length` and `crc32`).

---

## 3. WAL Writer

The writer is the append-only interface to the current segment:

**Responsibilities:**
- Accept a `MemtableEntry` + namespace and serialize it to the wire format
- Append to the current segment's file buffer
- Track the current segment's size for rotation decisions
- Assign monotonically increasing sequence numbers (or accept pre-assigned ones)

**Segment rotation logic:**
1. After each append, check if `current_segment_size >= segment_size_target`
2. If so, finalize current segment (flush buffer) and open new segment with next number
3. New segment number = previous + 1

---

## 4. WAL Reader

The reader iterates entries from one or more segments for recovery:

**Recovery parsing algorithm:**
1. Read `entry_length` (4 bytes)
2. Read that many bytes + 4 (the CRC)
3. Validate CRC over the entry body
4. If CRC passes: yield the parsed entry
5. If CRC fails **at the tail of the segment**: partial write from a crash during append — safe to discard (this is the expected truncation boundary)
6. If CRC fails **mid-segment**: the segment is corrupted — recovery halts and alerts

**Iteration:** The reader yields entries in sequence number order across segments. For multi-segment replay, segments are read in order by segment number.

---

## 5. Group Commit

Individual fsync per write is prohibitively expensive. The write path uses group commit to batch fsync calls:

**Three phases:**

1. **Buffering phase:** Incoming writes are appended to an in-memory WAL buffer and inserted into the memtable immediately. The write is **not yet ACK'd** to the caller.

2. **Commit phase:** Every 200μs (configurable) **or** when the buffer reaches 256KB (whichever comes first), the buffer is fsynced as a single batch.

3. **ACK phase:** All writes in the committed batch are ACK'd to their respective callers simultaneously.

**Performance impact:** Reduces fsync calls from N/sec (one per write) to ~5,000/sec (one per batch). Under sustained load, hundreds of writes share a single fsync, yielding 5-10x throughput improvement.

**Latency trade-off:** Adds up to 200μs of buffering latency. For latency-sensitive use, the commit interval is configurable down to 50μs.

**Commit ordering invariant:** Fsync must complete before any write in the batch is ACK'd. A write is only considered durable when the caller receives the ACK.

**Notification mechanism:** Each write submitted to the group commit buffer receives a notification handle (e.g., a oneshot channel). When the batch fsync completes, all handles in the batch are notified.

---

## 6. Fsync Strategy

Two modes, configurable per namespace:

| Mode | Behavior | Durability | Latency |
|------|----------|------------|---------|
| `SYNC` (default) | `fsync()` after every write batch | Survives power loss | ~0.5-2ms per write |
| `BATCH_SYNC` | `fsync()` on timer (every 10ms) | May lose last 10ms on power loss | ~50-100μs per write |

In `BATCH_SYNC` mode, the write path appends to an OS buffer and returns immediately. A background task calls `fsync()` every 10ms.

---

## 7. Dirty Segment Tracking

A segment cannot be deleted until **all** memtable generations that wrote to it have been flushed to S3.

Each segment maintains a dirty map:
```
dirty_map: HashMap<memtable_generation_id, highest_sequence_in_this_segment>
```

**Cleanup protocol:**
1. When a memtable (generation G) is flushed and its SSTable confirmed on S3: mark generation G as flushed in the segment manager
2. For each segment, remove G from its dirty map
3. If a segment's dirty map is empty, the segment is safe to delete

**Forced flush triggers for low-write partitions:**
1. **Segment age trigger:** If any WAL segment has been pinned for longer than `wal_segment_max_age` (default 5 minutes), force-flush all memtables that reference it
2. **WAL size pressure trigger:** If total WAL size on disk exceeds `wal_max_total_bytes` (default 512 MB), force-flush the memtable referencing the oldest pinned segment

Both triggers produce undersized L0 SSTables, but L0→L1 compaction will merge them.

**WAL size backpressure:** If the WAL exceeds `max_wal_size` (default 256MB — 4x the memtable threshold), writes to that partition are stalled with `ResourceExhausted` until a flush completes. This prevents local disk exhaustion during prolonged S3 unavailability.

---

## New Dependencies

None — uses `crc32fast` already in workspace.

---

## Future Work Considerations

When building Phase 2, keep the following downstream dependencies in mind:

| What You're Building | Who Needs It Later | What To Watch For |
|---------------------|-------------------|-------------------|
| **Segment dirty tracking** | Flush pipeline (P5b) | Flush completion marks a generation as flushed and triggers segment cleanup. The `dirty_map` API must support external callers (the flush pipeline) marking generations as flushed — don't make this internal-only. |
| **WAL Reader** | Recovery (P5d) | Recovery replays entries with `sequence_number > manifest.last_flushed_sequence`. The reader must support filtering by sequence number range efficiently, not just full-segment iteration. |
| **Group commit notification** | Server write ACK (P7) | The oneshot channel (or equivalent) returned from `append()` is how the gRPC server knows when a write is durable. Design the API so callers can `await` durability — the server will hold the gRPC response until the notification fires. |
| **Namespace field in entries** | Multi-tenant routing (P7) | WAL entries include `namespace` — this is needed so recovery can route replayed entries to the correct partition engine. Don't strip it during serialization even though single-partition WALs seem redundant. |
| **Sequence number assignment** | Memtable ordering (P3), Manifest tracking (P5a) | The WAL owns the monotonic sequence counter. The memtable and manifest both reference these numbers. Ensure the counter survives segment rotation and the last-assigned number is recoverable from the WAL on startup. |
| **Segment format** | WAL replication (future cluster) | Future cluster mode replicates WAL segments to followers. Design segments as self-contained units — each segment should be independently parseable without state from prior segments. Include a segment header with the starting sequence number. |

---

## Done When

- Write 10K entries across 3+ segments — all entries readable with correct data
- Kill (simulate crash) mid-write — replay recovers all valid entries, discards partial tail entry
- CRC validation detects corrupted entries mid-segment
- Segment rotation triggers at 32MB boundary
- Group commit measurably batches fsyncs (measure: N writes → fewer than N fsync calls)
- All writes in a batch are notified only after fsync completes
- Dirty segment tracking correctly prevents premature segment deletion
- Segments are only deleted when their dirty map is empty
- WAL backpressure returns `ResourceExhausted` when size exceeds threshold
