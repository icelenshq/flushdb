# Phase 5: Manifest + Flush + Read Path + Compaction

**Complexity: XL** _(this is the critical phase — working embedded KV store)_
**Crate:** `flushdb-engine`
**Design references:** STORAGE_DESIGN.md §8, §9, §10, §11, §22

---

## Goal

Wire together the manifest, flush pipeline, read path, recovery, and compaction into a **working embedded KV store**. After this phase, you can write millions of keys, kill the process, recover, and read back correct data.

Build in this order: **5a → 5b → 5c → 5d → 5e**

---

## 5a. Manifest

The manifest is the most critical data structure in flushdb. It defines which SSTables are live at each level, and its update protocol determines the correctness of the entire system.

### Contents

```json
{
  "format_version": 1,
  "manifest_id": "00000000000000000042",
  "writer_epoch": 7,
  "compactor_epoch": 3,
  "namespace": "my-namespace",
  "created_at_ms": 1709251200000,
  "last_flushed_sequence": 458923,
  "levels": {
    "L0": [
      {
        "id": "01JKQW3XYZ-L0-0001",
        "size_bytes": 67108864,
        "entry_count": 52341,
        "min_key": "...",
        "max_key": "...",
        "bloom_filter_offset": 66846720,
        "bloom_filter_size": 131072,
        "index_offset": 66977792,
        "index_size": 65536,
        "created_at_ms": 1709251195000,
        "sequence_range": [450000, 458923],
        "record_id_count": 12000
      }
    ],
    "L1": [
      {
        "id": "01JKPV2ABC-L1-0001",
        "run_id": "run-01JKPV2ABC",
        "fragment_index": 0,
        "size_bytes": 67108864,
        "entry_count": 80000,
        "min_key": "...",
        "max_key": "...",
        "bloom_filter_offset": 66846720,
        "bloom_filter_size": 131072,
        "index_offset": 66977792,
        "index_size": 65536,
        "created_at_ms": 1709250000000,
        "sequence_range": [400000, 449999],
        "record_id_count": 25000
      }
    ],
    "L2": [],
    "L3": []
  },
  "blob_files": [],
  "tombstone_compaction_watermarks": {
    "L1": 1709200000000,
    "L2": 1709100000000
  },
  "previous_manifest_id": "00000000000000000041"
}
```

### Manifest ID Scheme

Zero-padded 20-digit integers: `00000000000000000001`, `00000000000000000002`, etc.

**Why:**
1. **Lexicographic ordering = version ordering.** The "current" manifest is the one with the highest ID when listed.
2. **Eliminates two-step update problem.** Writing the new manifest IS the update.
3. **Natural CAS.** Two writers racing: exactly one succeeds via `conditional_put`, the other gets `PreconditionFailed` and retries.

### Update Protocol (CAS)

Every operation that changes the set of live SSTables must update the manifest atomically:

```
Step 1: Read current manifest (ID = N)

Step 2: Validate epoch
        if trigger == FLUSH && current.writer_epoch > my_writer_epoch: HALT (zombie writer)
        if trigger == COMPACTION && current.compactor_epoch > my_compactor_epoch: HALT (zombie compactor)

Step 3: Compute new manifest
        new_manifest = apply(current, update)
        new_manifest.manifest_id = current.manifest_id + 1
        new_manifest.previous_manifest_id = current.manifest_id

Step 4: Write to StorageBackend with conditional_put (If-None-Match: * semantics)

Step 5: Handle result
        SUCCESS → done
        PreconditionFailed → re-read, re-validate, retry
```

### Epoch-Based Fencing

Prevents zombie writers from corrupting manifests after ownership changes:

- **Writer epoch:** Incremented on every ownership change. Checked before every flush manifest update.
- **Compactor epoch:** Same mechanism for compaction.
- If the manifest's epoch is higher than mine → I'm a zombie → HALT immediately.

### Manifest Snapshots and Pruning

- **Snapshots:** Every 100 manifest versions, write a self-contained snapshot with full materialized state. On startup, read only the latest snapshot and apply subsequent versions on top.
- **Pruning:** Versions older than the second-most-recent snapshot are eligible for deletion. Two snapshots retained for rollback. Background task deletes in batches of 50.

### Concurrent Operations

| Operation | Frequency | Conflict Probability |
|-----------|-----------|---------------------|
| Memtable flush (L0 add) | Every ~60s per partition | Low |
| L0→L1 compaction | When L0 reaches 4 files | Medium (can overlap with flush) |
| L1→L2 compaction | When L1 exceeds 256 MB | Low |
| Tombstone GC | Background timer | Very low |

**Conflict resolution:** Pure retry. Re-read current manifest, check if update is still valid (inputs not already consumed), re-apply, retry CAS.

---

## 5b. Flush Pipeline

### End-to-End Sequence

```
1. TRIGGER: memtable.total_allocated >= threshold OR time >= 5 minutes
2. FREEZE: Pointer swap (active → frozen, new empty → active)
3. BUILD SSTable:
     Iterate frozen memtable in sorted order
     Build data blocks (4 KB target, compressed)
     Build bloom filter over record IDs
     Build dedup block
     Build sparse index + footer
4. UPLOAD to StorageBackend
5. UPDATE MANIFEST (CAS):
     Add new SSTable to L0
     Set last_flushed_sequence = max sequence in flushed memtable
     Validate writer epoch
6. TRUNCATE WAL:
     Delete segments where all referenced generations are flushed
7. RELEASE frozen memtable:
     Drop arena allocator, remove from frozen list
8. CHECK compaction trigger:
     If L0 now has > 4 SSTables, schedule L0→L1 compaction
```

### Failure Modes

| Failure Point | Impact | Recovery |
|---------------|--------|----------|
| Crash during build (step 3) | No StorageBackend state changed | WAL replay recreates memtable |
| Crash during upload (step 4) | Orphaned partial file | Orphan GC cleans it |
| Crash during manifest CAS (step 5) | SSTable uploaded but not referenced | Orphan GC. WAL replay. |
| Crash after CAS, before WAL truncation (step 6) | Manifest updated, WAL not truncated | WAL replay replays already-flushed entries — idempotent |
| CAS failure in step 5 | Another writer updated manifest | Retry CAS. SSTable already uploaded. |

**Key insight:** The WAL is the safety net. The manifest update is the commit point.

### Flush Backpressure

- **Second frozen memtable:** Active fills again while first frozen is still flushing. Reads check: active → frozen-1 → frozen-2 → SSTables.
- **Write stall threshold:** If N frozen memtables accumulate (default N=3), writes rejected with backpressure error.

---

## 5c. Read Path

### Merge-Read Pattern

Reads merge across layers, from newest to oldest:

```
1. Active memtable (newest writes)
2. Frozen memtable(s) (pending flush)
3. L0 SSTables (most recent flushes — may overlap)
4. L1 SSTables (non-overlapping)
5. L2 SSTables (non-overlapping)
6. L3 SSTables (non-overlapping)
```

### Point Read

1. Search active memtable, then each frozen memtable — return immediately if found (checking tombstone status)
2. Check bloom filter for all candidate SSTables (filters are in memory)
3. For L0 (overlapping): check ALL L0 SSTables that pass bloom filter, in parallel
4. For L1-L3 (non-overlapping): binary search to find the one SSTable whose key range covers the target
5. Fetch data blocks from matching SSTables
6. Merge results by sequence number — highest wins. If it's a tombstone, return not-found.

### Range Read

1. Open iterators on all layers with matching record ID (bloom filter eliminates non-matching SSTables)
2. Merge-sort iterators by composite key
3. Apply tombstone filtering: skip entries covered by tombstones with higher sequence numbers
4. Accumulate results until byte-based page budget exhausted or range end reached
5. Return results plus a page token encoding the last key emitted

**Full record read** (`match_all`): Same as range read with start = MIN_KEY, end = MAX_KEY.

### Byte-Based Pagination

Pagination uses byte budgets rather than row counts:
- Client specifies `page_size_bytes` (default 2MB) and optional `item_limit`
- Read until byte budget met
- Page token = last composite key emitted — resume by seeking past it

### Range Tombstone Filtering

During merge-read, for each candidate entry:
1. Check active memtable's range tombstone index
2. Check each frozen memtable's range tombstone index
3. Check SSTable-level range tombstones
4. If any covering tombstone has a higher sequence number → skip the entry

---

## 5d. Recovery

On startup or after a crash:

```
1. Fetch the current manifest from StorageBackend
   (list manifests, pick highest ID)
2. Rebuild in-memory state from manifest:
   - SSTable metadata for all levels
   - Load bloom filters and index blocks into memory
3. Replay local WAL entries with sequence numbers > manifest's last_flushed_sequence:
   - Parse each WAL segment in order
   - Insert replayed entries into a fresh memtable
4. Resume normal operation
   - The rebuilt memtable serves reads alongside SSTables from the manifest
```

**Correctness guarantee:** Zero data loss for any write that was ACK'd, assuming the WAL survived the crash. The manifest's `last_flushed_sequence` is the boundary — everything at or below it is in SSTables, everything above must come from WAL replay.

---

## 5e. Compaction

### Leveled Architecture

```
L0: up to 4 overlapping SSTables (flush output, ~64 MB each)
L1: non-overlapping, max 256 MB total
L2: non-overlapping, max 2.56 GB total
L3: non-overlapping, max 25.6 GB total
```

Size ratio: 10x between levels (L0 trigger ~256 MB → L1 256 MB → L2 2.56 GB → L3 25.6 GB). Worst-case write amplification: ~10 per level.

### Compaction Triggers

| Trigger | Condition | Action |
|---------|-----------|--------|
| L0 overflow | `len(L0) > 4` | Compact all L0 → overlapping range in L1 |
| L1 overflow | `total_size(L1) > 256 MB` | Pick one L1 SSTable, compact with overlapping L2 range |
| L2 overflow | `total_size(L2) > 2.56 GB` | Pick one L2 SSTable, compact with overlapping L3 range |
| Tombstone TTL | Periodic timer (1 hour) | Compact bottom-level SSTables with expired tombstones |
| Space amplification | Second-largest tier reaches half of largest tier | Cross-tier compaction (SAG target: 1.75, configurable 1.0–2.0) |

### Write Stalling

Progressive backpressure when L0 fills up:

| L0 File Count | Action |
|---------------|--------|
| 4 (soft limit) | Compaction triggered. Writes proceed at full speed. |
| 8 (slow limit) | Write throughput throttled. Each write sleeps `(l0_count - 4) * 1ms`. |
| 12 (hard limit) | Writes fully stalled with `ResourceExhausted` and `retry-after` hint. |

### SSTable Run Fragments

Compaction produces **runs** — sequences of smaller non-overlapping fragments (~1 GB each for L1+):

```
sstables/L1/run-{id}/frag-0000.sst
sstables/L1/run-{id}/frag-0001.sst
sstables/L1/run-{id}/frag-0002.sst
```

**Benefits:**
- Delete each input fragment as its key range is fully written. Max temporary space = `2 × fragment_size`.
- A run is logically one SSTable for read purposes.

**Trivial move optimization:** When an L(N) fragment's key range does NOT overlap with any fragment in L(N+1), move it by manifest-only update — no I/O. For time-ordered keys, the majority of compactions are trivial moves.

**Partial range compaction:** Only overlapping fragments participate in the merge. Non-overlapping L(N+1) fragments are left in place.

### Merge Process

```
1. Open iterators on all input SSTables
2. Merge-sort by composite key:
   - Duplicate keys: keep highest sequence_number
   - Point tombstones: if TTL expired AND bottom level, drop both
   - Range tombstones: propagate to output if they still cover keys in lower levels
3. Build output SSTable fragments:
   - Split into new fragment at size boundaries
   - Each fragment gets its own bloom filter, index, footer
   - Upload each fragment as completed
4. Update manifest (CAS):
   - Add new run to target level
   - Remove input SSTables from source levels
   - Validate compactor_epoch
5. Mark old SSTables for deferred deletion
```

### Tombstone Lifecycle

```
Write: Client DeleteItems → WAL → Memtable → L0 SSTable
  │
  ▼
L1 (compaction): Tombstone merged. Covered PUTs dropped.
                 Tombstone MUST survive — older PUTs may exist in L2/L3.
  │
  ▼
L2 (compaction): Same — propagated, covered entries dropped.
  │
  ▼
L3 (bottom level): Tombstone's TTL checked.
                   If expired (default 7 days): tombstone dropped.
```

**Range tombstone watermark protocol:** Tombstones at level Li eligible for deletion only when all deeper levels have been fully compacted past the tombstone's creation time.

---

## New Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| tokio-util | 0.7 | Codec framing, async stream helpers |

---

## Future Work Considerations

When building Phase 5, keep the following downstream dependencies in mind:

| What You're Building | Who Needs It Later | What To Watch For |
|---------------------|-------------------|-------------------|
| **Manifest** | Cache invalidation (P6), Server partitions (P7), Cluster fencing (future) | The manifest version drives cache continuity tracking invalidation (P6) — expose a way to subscribe to manifest version changes. The server creates per-partition manifests (P7). Epoch fields (`writer_epoch`, `compactor_epoch`) are used for cluster fencing — implement the epoch check logic now even though single-node always passes. |
| **Manifest CAS protocol** | Cluster coordination (future) | Future cluster mode has multiple nodes racing on manifests. The retry-on-conflict logic must be robust: re-read, re-validate inputs (check if input SSTables were already consumed by another compaction), re-apply. Don't assume single-writer — the protocol should be correct under concurrent access from day one. |
| **Flush pipeline** | Server write path (P7), WAL integration (P2) | The server's write path triggers flushes when memtables freeze. Expose flush as an async operation the server can monitor (e.g., for backpressure status). Flush completion must notify WAL dirty tracking (P2) to enable segment cleanup. |
| **Read path merge** | Cache layer (P6) | P6 inserts a cache between the read path and StorageBackend. Design block reads as a pluggable step — the read path should call through an interface that the cache can wrap, not directly call `StorageBackend::get_range`. Consider a `BlockFetcher` trait or similar seam. |
| **Compaction** | Cache eviction (P6), Metrics (future) | Compaction invalidates cached blocks from consumed SSTables. After compaction, emit a list of consumed SSTable IDs so the cache can batch-evict them. Future metrics need compaction throughput, duration, and write amplification — instrument the compaction loop with `tracing` spans now. |
| **Recovery** | Server startup (P7) | The server recovers all partitions on startup. Recovery must be per-partition and parallelizable — don't use global state. Each partition's recovery is independent: load its manifest, replay its WAL. The server orchestrates N parallel recoveries. |
| **L0 write stalling** | Server error responses (P7) | Progressive backpressure (4/8/12 L0 files) maps to gRPC error responses. The stall signal should include the current L0 count and delay hint so the server can construct meaningful `retry-after` headers. |
| **Tombstone lifecycle** | CDC (future) | Future CDC emits delete events. Tombstone creation should include enough metadata (timestamp, record_id scope) for CDC to reconstruct the delete event. Don't strip this context during compaction propagation. |

---

## Done When

- Write 10M keys, kill process, recover from manifest + WAL — reads return correct data for every key
- Manifest CAS correctly rejects conflicting concurrent updates
- Manifest snapshots work — recovery loads snapshot + applies deltas
- Flush pipeline: frozen memtable → SSTable → manifest update → WAL cleanup — full cycle works
- Crash at any point in the flush pipeline → recovery produces correct state
- Point reads return the correct value across all layers (memtable, frozen, L0-L3)
- Point reads correctly return not-found for tombstoned keys
- Range reads merge-sort correctly across all layers
- Range tombstones shadow covered keys during reads
- Byte-based pagination with page tokens works — resuming produces no gaps or duplicates
- Compaction keeps L0 bounded (never exceeds 12 files under sustained load)
- Trivial move optimization works for non-overlapping compaction inputs
- L0 write stalling kicks in progressively at 4/8/12 files
- Tombstones are preserved through compaction until they reach the bottom level and TTL expires
- This is a **working embedded KV store**
