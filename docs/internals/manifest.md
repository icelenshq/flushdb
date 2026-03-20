# Manifest

The manifest defines which SSTables are live at each level. It is the **commit point** for all state changes.

## Role in the System

```
                  ┌──────────────────────────────────────────┐
                  │              S3 Bucket                     │
                  │                                            │
                  │   manifests/                                │
                  │     v41.json  ←── previous                 │
                  │     v42.json  ←── CURRENT (highest ID)     │
                  │       │                                    │
                  │       │  defines what's live:              │
                  │       │                                    │
                  │       ├─► L0: [sst-E]                      │
                  │       ├─► L1: [run-FG/frag-0, frag-1]     │
                  │       ├─► L2: []                           │
                  │       ├─► L3: []                           │
                  │       │                                    │
                  │       ├─► writer_epoch: 7                  │
                  │       ├─► compactor_epoch: 3               │
                  │       └─► last_flushed_sequence: 458923    │
                  │                                            │
                  │   sstables/                                 │
                  │     L0/sst-E.sst        ← referenced       │
                  │     L1/run-FG/frag-*.sst ← referenced      │
                  │     L0/sst-OLD.sst       ← NOT referenced  │
                  │                            (GC candidate)   │
                  └──────────────────────────────────────────┘

  On startup:  read manifest → know exactly which SSTables to use
  On flush:    CAS manifest → add new SSTable to L0
  On compact:  CAS manifest → remove inputs, add outputs
  On recovery: replay WAL entries > last_flushed_sequence
```

## Structure

```json
{
  "format_version": 1,
  "manifest_id": "00000000000000000042",
  "writer_epoch": 7,
  "compactor_epoch": 3,
  "namespace": "my-namespace",
  "last_flushed_sequence": 458923,
  "levels": {
    "L0": [{ "id", "size_bytes", "entry_count", "min_key", "max_key",
             "bloom_filter_offset", "bloom_filter_size",
             "sequence_range", "record_id_count", ... }],
    "L1": [...], "L2": [], "L3": []
  },
  "tombstone_compaction_watermarks": { "L1": ..., "L2": ... },
  "previous_manifest_id": "00000000000000000041"
}
```

**Manifest IDs** are 20-digit zero-padded integers. The highest ID is always the current manifest — S3 `ListObjectsV2` gives you the latest. Two writers computing the same next ID race: one wins, the other retries.

**S3 path:** `s3://{bucket}/{hash % 128}/flushdb/{namespace}/manifests/{id}`

## CAS Update Protocol

Every flush, compaction, and GC operation updates the manifest atomically:

```
1. Read current manifest (ID = N)
2. Validate epoch (zombie check)
3. Compute new manifest (N+1)
4. PUT to S3 with If-None-Match: *
5. SUCCESS → done
   412 PreconditionFailed → re-read, recompute, retry
```

S3 conditional writes reject a PUT if the key already exists. No external coordination needed.

## Concurrent Flush + Compaction

```
Time ─────────────────────────────────────────────────►

Flusher                          Compactor
   │                                │
   │  Read manifest v5              │  Read manifest v5
   │  L0: [A,B,C,D]                │  L0: [A,B,C,D]
   │                                │
   │  Build & upload SSTable E      │  Merge [A,B,C,D] → L1 [F,G]
   │                                │
   │  CAS: v6 (add E to L0)        │
   │  → SUCCESS                     │
   │                                │
   │                                │  CAS: v7 (expected prev=v5)
   │                                │  → FAIL (412)
   │                                │
   │                                │  Re-read v6. Inputs [A,B,C,D] still valid.
   │                                │  Recompute, CAS: v7 → SUCCESS
   │                                │  L0=[E], L1=[F,G]
```

## Epoch-Based Fencing

**Problem:** Node A begins flushing, its lease expires, Node B takes over, A's flush completes and tries to update the manifest.

**Solution:** Each manifest carries `writer_epoch` and `compactor_epoch`. On lease acquisition, the new owner bumps the epoch. The old node's writes are rejected:

```
Node A (epoch=5)                  Node B
   │                                │
   │  Begins flush...               │
   │  ── Lease expires ──           │
   │                                │  Takes lease, CAS manifest: epoch=6
   │                                │  Begins serving.
   │  Flush done.                   │
   │  Read manifest... epoch=6      │
   │  6 > 5 → ZOMBIE. HALT.        │
```

## Snapshots and Pruning

Every 100 versions, a self-contained **snapshot manifest** is written. On startup, read the latest snapshot and apply subsequent deltas. Versions older than the second-most-recent snapshot are eligible for deletion.

A **pointer file** (`manifest.json`) provides fast lookup of the current manifest ID. Falls back to `ListObjectsV2` if stale.
