# Write Path

This page describes how data moves from a client write request through the
local storage engine and eventually to S3. Every stage is designed so that
a crash at any point is recoverable without data loss for acknowledged writes.

---

## Write-Ahead Log (WAL)

### Segment-Based Architecture

The WAL is not a single append-only file. It is a sequence of fixed-size
segment files, each defaulting to 32 MB.

```
wal/
  partition-{id}/
    segment-000000000001.wal
    segment-000000000002.wal
    segment-000000000003.wal   <-- current (append target)
```

Segments use monotonically increasing 12-digit zero-padded numbers. Gaps in
numbering are expected because segment deletion leaves holes.

Why segments instead of one file:

- **Truncation is file deletion.** When a flush confirms that all data
  in a segment has reached S3, the segment file is removed. There is no
  rewriting or hole-punching.
- **Independent fsync and rotation.** Each segment can be fsynced and
  sealed independently. When the current segment reaches its size limit,
  a new segment opens and the old one becomes immutable.
- **Simple crash reasoning.** Only the tail of the last segment can
  contain a partial write. All prior segments are sealed and CRC-valid.

### Entry Format

Each entry is length-prefixed, self-describing, and CRC32-protected:

```
+---------------------------------------------------+
| entry_length       uint32 (little-endian)         |  4 bytes
+---------------------------------------------------+
| sequence_number    uint64 (little-endian)         |  8 bytes
| entry_type         uint8                          |  1 byte
|                    0 = PUT                         |
|                    1 = DELETE                      |
|                    2 = RANGE_DELETE                |
| namespace_len      uint16                         |  2 bytes
| namespace          bytes                          |  variable
| record_id_len      uint16                         |  2 bytes
| record_id          bytes                          |  variable
| item_key_len       uint16                         |  2 bytes
| item_key           bytes                          |  variable
| item_value_len     uint32                         |  4 bytes
| item_value         bytes                          |  variable
| item_metadata_len  uint16                         |  2 bytes
| item_metadata      bytes                          |  variable
| idempotency_token  bytes (fixed)                  | 24 bytes
+---------------------------------------------------+
| crc32              uint32                         |  4 bytes
|                    (covers all preceding bytes)   |
+---------------------------------------------------+
```

The sequence number is a monotonically increasing 64-bit counter per
partition. It orders writes globally within the partition and is the
tiebreaker when the same key appears at multiple layers during reads.

Entry type semantics:

| Type | item_key | item_value |
|------|----------|------------|
| PUT | Target key | Payload |
| DELETE | Target key | Empty (length 0) |
| RANGE_DELETE | Start key (inclusive) | End key (exclusive) |

The 24-byte idempotency token consists of an 8-byte client request ID and
a 16-byte UUID v4. An all-zero token means "no idempotency" and the entry
is always applied.

### Recovery

Recovery reads each segment front-to-back:

1. Read `entry_length` (4 bytes).
2. Read that many bytes plus the trailing 4-byte CRC.
3. Compute CRC over the entry body and compare.
4. If CRC matches, the entry is valid. Apply it.
5. If CRC fails at the **tail** of the last segment, the entry was a
   partial write interrupted by a crash. Discard it safely.
6. If CRC fails in the **middle** of a sealed segment, the segment is
   corrupted. Recovery halts and raises an alert.

Only entries with sequence numbers beyond the manifest's
`last_flushed_sequence` need to be replayed, because everything at or
below that watermark is already durable in S3.

---

## Group Commit

Writing one fsync and one replication round-trip per individual write is
prohibitively expensive. Group commit batches many writes into one
durable operation.

```
              Incoming writes
              w1  w2  w3  w4  w5
               |   |   |   |   |
               v   v   v   v   v
          +---------------------------+
          |  In-memory WAL buffer     |  <-- writes inserted into
          |  + memtable insert        |      memtable immediately
          +---------------------------+
                      |
        Trigger: 200us elapsed OR 256KB buffered
                      |
                      v
          +---------------------------+
          |  fsync entire batch       |
          |  replicate to followers   |
          +---------------------------+
                      |
                      v
          +---------------------------+
          |  ACK w1, w2, w3, w4, w5   |
          +---------------------------+
```

### How it works

1. **Buffer phase.** Incoming writes append to an in-memory WAL buffer
   and insert into the active memtable immediately. No acknowledgment
   yet.
2. **Commit phase.** Every 200 microseconds or when the buffer reaches
   256 KB (whichever comes first), the buffer is fsynced as a single
   batch and replicated to followers as one batch message.
3. **ACK phase.** All writes in the committed batch are acknowledged to
   their respective clients simultaneously.

### Why it matters

- Reduces fsync calls from N per second (one per write) to roughly
  5,000 per second (one per batch).
- Reduces follower replication from N round-trips to one per batch.
- Under sustained load, hundreds of writes share a single fsync and
  replication round-trip, yielding 5-10x throughput improvement.

### Durability guarantee

A write is considered durable **only** after:

1. The local WAL batch is fsynced to disk, AND
2. A quorum of replicas has acknowledged the batch.

No client acknowledgment is sent until both conditions are met.

### Latency trade-off

Group commit adds up to 200 microseconds of buffering latency. This is
configurable:

- The commit interval can be reduced to as low as 50 microseconds for
  latency-sensitive workloads.
- A per-request `flush_immediate` flag bypasses batching entirely,
  triggering an immediate fsync and replication round-trip for that
  single write.

---

## Fsync Strategies

Two modes, configurable per namespace:

| Mode | Behavior | Durability | Typical latency |
|------|----------|------------|-----------------|
| SYNC (default) | fsync after every write batch | Survives power loss | ~0.5-2 ms per write |
| BATCH_SYNC | fsync on a 10 ms timer | May lose last 10 ms on power loss | ~50-100 us per write |

In SYNC mode, each group commit batch triggers an fsync before the
acknowledgment phase. This is the safe default.

In BATCH_SYNC mode, the write path appends to an OS buffer and returns
immediately. A background thread calls fsync every 10 milliseconds.
This is acceptable when followers use SYNC mode, because the quorum
replication provides a secondary durability layer.

---

## Memtable

The memtable is the in-memory buffer that holds recently written data
before it is flushed to an SSTable on S3.

### Data structure: skip list

The memtable uses a skip list with the following properties:

- **12 levels**, with a promotion probability of 1/4 (each level
  contains roughly one quarter of the entries in the level below).
- Supports roughly 4 billion entries efficiently at 12 levels.

### Single-writer model

The memtable is owned by a single CPU core. There is no concurrent
access from other cores, which means:

- **No CAS loops.** Inserts are plain pointer writes.
- **No contention.** No locks, no atomics on the write path.
- Reads from the owning core traverse the skip list directly, with
  zero synchronization.
- Cross-core reads are posted as tasks via lock-free queues to the
  owning core, which executes the read and returns the result.

### Arena-based allocation

Each memtable owns a bump allocator that pre-allocates memory in 1 MB
blocks:

- Allocation is a pointer bump. No per-object `malloc`/`free`.
- Deallocation is O(1): when the memtable is released after flush, all
  blocks are dropped at once.
- `total_allocated` tracks cumulative bytes and serves as the threshold
  metric.

### Freeze and swap

When the memtable reaches its threshold (default 64 MB) or a time limit
fires (default 5 minutes), the active memtable is frozen:

```
Before freeze:               After freeze:

  active memtable (64 MB)      frozen memtable (read-only)
                                active memtable (0 bytes, new)
```

1. **Pointer swap.** The active memtable pointer is replaced with a new
   empty memtable. The old memtable becomes frozen. This is a single
   pointer write on the owning core.
2. **Frozen list.** The frozen memtable is pushed onto a read-only list.
   Reads still check all frozen memtables until their data is flushed.
3. **Flush trigger.** A background flush task is notified.

The time-based trigger bounds the unflushed window for low-write
partitions. Without it, a partition writing 1 KB/s would take about
18 hours to reach 64 MB.

### Sequence numbers

Each write receives a monotonically increasing 64-bit sequence number
from a per-partition counter. Sequence numbers appear in both the WAL
entry and the memtable entry. They serve two purposes:

1. **Ordering.** When the same key appears multiple times, the highest
   sequence number wins.
2. **Recovery.** Only WAL entries with sequence numbers above the
   manifest's `last_flushed_sequence` are replayed on startup.

### Range tombstone index

The memtable maintains a secondary index of range tombstones, sorted by
(record_id, start_key). During point reads, this index is checked to
determine whether the target key falls within a range tombstone that has
a higher sequence number than the found entry. If it does, the key is
treated as deleted.

---

## Flush Pipeline

The flush pipeline moves data from a frozen memtable to a durable SSTable
on S3. Here is the step-by-step sequence:

```
1. TRIGGER
   Memtable reaches 64 MB, or 5 minutes elapse.
        |
2. FREEZE
   Pointer swap: active --> frozen, new empty --> active.
        |
3. BUILD SSTABLE
   Iterate frozen memtable in sorted order.
   Build data blocks (4 KB target, compressed).
   Build bloom filter over record IDs.
   Build sparse index and footer.
        |
4. UPLOAD TO S3
   If SSTable < 16 MB:  single PutObject.
   If SSTable >= 16 MB: streaming multipart upload.
        |
5. UPDATE MANIFEST (CAS)
   Add new SSTable to L0.
   Set last_flushed_sequence to the max sequence in the flushed memtable.
   Validate writer epoch (zombie fencing).
        |
6. TRUNCATE WAL
   Delete segments whose referenced memtable generations have all been flushed.
        |
7. RELEASE FROZEN MEMTABLE
   Drop the arena allocator. Remove from the frozen list.
        |
8. CHECK COMPACTION TRIGGER
   If L0 now has more than 4 SSTables, schedule L0 --> L1 compaction.
```

### Manifest update details

The manifest is updated using an optimistic compare-and-swap (CAS)
protocol backed by S3 conditional writes (`If-None-Match: *`). The flush
process reads the current manifest, computes a new version, and writes it
to S3. If another operation updated the manifest concurrently, the CAS
fails with HTTP 412 and the flush retries after re-reading.

### S3 upload strategy

For SSTables under 16 MB, a single `PutObject` call is sufficient.

For larger SSTables, streaming multipart upload overlaps building and
uploading:

```
Build Part 1 ----> Upload Part 1
Build Part 2 ----> Upload Part 2   (overlaps with Upload Part 1)
Build Part 3 ----> Upload Part 3
...
Complete multipart upload (SSTable becomes atomically visible)
```

Each part is 16 MB. Double buffering means peak memory is roughly 32 MB
regardless of total SSTable size.

---

## Dirty Segment Tracking

WAL segments cannot be deleted until all memtable generations that wrote
to them have been flushed to S3.

Each segment maintains a dirty map that tracks which memtable generations
have entries in it:

```
Segment 001:  { generation 12: seq 50000, generation 13: seq 52000 }
Segment 002:  { generation 13: seq 55000 }
Segment 003:  { generation 14: seq 58000 }
```

When a memtable generation is flushed and its SSTable is confirmed on S3:

1. Mark that generation as flushed in the segment manager.
2. Remove the generation from every segment's dirty map.
3. If a segment's dirty map becomes empty, delete the segment file.

### Forced flush triggers

A low-write partition may never reach the memtable size threshold, which
would pin WAL segments indefinitely. Two additional triggers prevent this:

| Trigger | Condition | Action |
|---------|-----------|--------|
| Segment age | Any WAL segment pinned longer than 5 minutes | Force-flush all memtables referencing it |
| WAL size pressure | Total WAL exceeds 512 MB | Force-flush the memtable referencing the oldest pinned segment |

Both triggers may produce undersized L0 SSTables, but L0-to-L1
compaction will merge them.

### Write backpressure

If the WAL exceeds 256 MB (4x the default memtable threshold), writes to
that partition are stalled with a resource-exhausted error until a flush
completes. This prevents local disk exhaustion during prolonged S3
unavailability.

---

## Flush Failure Modes

Every failure during the flush pipeline is recoverable via WAL replay. The
WAL is the safety net; the manifest update is the commit point.

| Failure point | What happened on S3 | Recovery |
|---------------|---------------------|----------|
| Crash during SSTable build | Nothing changed on S3 | WAL replay recreates the memtable and retries the flush |
| Crash during S3 upload | Incomplete multipart upload exists | S3 lifecycle rule aborts the upload after 24 hours; WAL replay retries |
| Crash during manifest CAS | SSTable uploaded but not referenced | Orphaned SSTable; garbage collection cleans it up; WAL replay retries |
| CAS conflict (another writer) | SSTable already on S3 | Re-read the manifest, verify inputs are still valid, retry the CAS |
| Crash after manifest CAS, before WAL truncation | Manifest updated, WAL still has entries | WAL replay replays already-flushed entries; this is idempotent |
