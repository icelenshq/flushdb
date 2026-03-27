# Write-Ahead Log (WAL)

The WAL ensures durability for writes that haven't yet been flushed to S3.

## Lifecycle

```
  Client Write
       │
       ▼
┌──────────────┐    200μs / 256KB    ┌─────────┐    confirmed    ┌─────────────┐
│  WAL Buffer  │ ──────────────────► │ fsync() │ ─────────────► │ ACK Client  │
│  (in-memory) │     group commit    └─────────┘                └─────────────┘
└──────┬───────┘
       │ appended to
       ▼
┌──────────────────────────────────────────────────────────────┐
│                    WAL Segments (local disk)                   │
│                                                                │
│  segment-000000000001.wal   ← all generations flushed → DELETE │
│  segment-000000000002.wal   ← memtable gen 4 still active     │
│  segment-000000000003.wal   ← current append target            │
└──────────────────────────────────────────────────────────────┘
       │ on memtable flush
       ▼
┌──────────────┐    CAS manifest    ┌───────────────────┐
│  SSTable on  │ ─────────────────► │ Segment safe to   │
│  S3 (L0)     │    confirmed       │ delete (dirty map │
└──────────────┘                    │ empty)            │
                                    └───────────────────┘
```

## Segment Architecture

The WAL is a sequence of fixed-size **segments** (default 32 MB) on local disk.

```
wal/partition-{id}/
    segment-000000000001.wal    ← oldest active
    segment-000000000002.wal
    segment-000000000003.wal    ← current append target
```

Segments are deleted whole after a flush confirms data on S3 — no mid-file truncation. Segment numbers are monotonically increasing 12-digit zero-padded integers. Gaps from deleted segments are normal.

## Entry Wire Format

Each entry is length-prefixed and CRC-protected:

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
│ idempotency_token: bytes                            │  24 bytes
├─────────────────────────────────────────────────────┤
│ crc32: uint32 (over all preceding bytes)            │  4 bytes
└─────────────────────────────────────────────────────┘
```

**Recovery:** Read `entry_length`, read payload + CRC, validate. CRC failure at segment tail = partial write from crash (safe to discard). CRC failure mid-segment = corruption (halt and alert).

## Group Commit

Individual fsync per write is expensive. The WAL batches writes:

```
  Buffering (in-memory)          Commit              ACK
┌───────────────────────┐   ┌──────────────┐   ┌────────────┐
│ Write 1               │   │              │   │            │
│ Write 2    → buffer   │──►│   fsync()    │──►│  ACK all   │
│ Batch [3,4,5]         │   │              │   │  clients   │
│ Write 6               │   │              │   │            │
└───────────────────────┘   └──────────────┘   └────────────┘
 Trigger: 200μs or 256KB
```

The group commit loop accepts both single writes and **batch submissions**. A `PutItems` with multiple items submits all its WAL entries as a single batch unit, sharing one durability notification. Single writes and batch writes are combined together in the same commit window.

Writes are appended to a buffer but **not ACK'd** until the buffer is fsynced. The engine awaits a `DurabilityNotification` before incrementing sequence numbers or inserting into the memtable — this prevents sequence gaps on failure. Every 200μs or 256KB (whichever comes first), the batch is fsynced and all writes in it are ACK'd simultaneously. Under sustained load, hundreds of writes share a single fsync — **5-10x throughput improvement**.

The commit interval is configurable down to 50μs for latency-sensitive namespaces.

Two fsync modes per namespace:

| Mode | Behavior | Durability | Latency |
|------|----------|------------|---------|
| `SYNC` (default) | fsync after every batch | Survives power loss | ~0.5-2ms |
| `BATCH_SYNC` | fsync on 10ms timer | May lose last 10ms | ~50-100μs |

The `batch_sync_interval` / `wal_batch_sync_interval_ms` timer must be configured with a positive value. `0` is rejected during config validation.

## Dirty Segment Tracking

A segment cannot be deleted until **all** memtable generations that wrote to it have been flushed to S3. Each segment maintains:

```
dirty_map: HashMap<memtable_generation_id, highest_sequence>
```

When generation G is flushed: remove G from all segments' dirty maps. If a segment's map becomes empty, it's safe to delete.

**Low-write partitions** may never fill a memtable, pinning segments indefinitely. Two pressure valves:
- **Age trigger:** Segments pinned > 5 minutes → force-flush referencing memtables.
- **Size trigger:** Total WAL > 512 MB → force-flush the oldest pinned memtable.

**Backpressure:** WAL > 256 MB (4x memtable threshold) → writes stalled with `RESOURCE_EXHAUSTED` until a flush completes.
