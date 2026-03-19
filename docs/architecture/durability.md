# Durability & Recovery

This document describes flushdb's durability guarantees, the mechanisms that protect data between write acknowledgment and permanent storage, and the recovery protocols that restore consistent state after failures.

---

## Durability Model

flushdb delegates long-term durability to S3 and uses local write-ahead logs to protect data that has not yet reached S3.

**S3 as the durability foundation.** All flushed data -- SSTables, manifests, blob files -- resides in S3, which provides 99.999999999% (eleven nines) durability. Once data reaches S3 and is recorded in the manifest, it is permanent. No amount of node failure, disk loss, or cluster disruption can affect it.

**WAL as the local durability layer.** Between the moment a write is accepted and the moment it is flushed to S3, the write-ahead log on the partition owner's local disk is the primary durability mechanism. The WAL is segment-based, CRC-protected, and fsynced before any write is acknowledged.

**WAL replication as the secondary durability layer.** Before acknowledging a write, the partition owner replicates the WAL batch to follower nodes. This means the unflushed data exists on multiple independent disks, not just the owner's. The replication quorum required before acknowledgment is configurable per namespace (ONE, QUORUM, or ALL).

**The acknowledgment contract.** A write is considered durable only after the client receives an ACK. An ACK is only sent after the following sequence completes without error:

1. WAL entry appended to the owner's local segment
2. Segment fsynced to disk
3. WAL batch replicated to enough followers to meet the configured consistency level
4. ACK returned to the client

If any step fails, no ACK is sent. The client retries, and idempotency tokens prevent duplicate application.

---

## The Unflushed Window

The unflushed window is the time between when a write is acknowledged to the client and when that write reaches S3 as part of a flushed SSTable. This is the only interval during which data loss is theoretically possible.

**Why it exists.** flushdb batches writes in an in-memory structure (the memtable) for performance. Flushing every write individually to S3 would impose 50-200ms of latency per operation, making the system impractical for write-heavy workloads. Instead, writes accumulate in the memtable and are flushed to S3 as a batch when a size or time threshold is reached.

**How long it lasts.** The unflushed window typically spans seconds to minutes, depending on two thresholds:

- **Size threshold** (default 64 MB): When the memtable reaches this size, it is frozen and flushed.
- **Time threshold** (default 5 minutes): Even if the size threshold has not been reached, the memtable is flushed after this interval. This bounds the window for low-throughput partitions.

Under sustained write load, flushes happen frequently and the window is short. For a partition receiving 1 KB/s, the time threshold ensures the window never exceeds 5 minutes.

**What it takes to lose data.** Data in the unflushed window exists on the owner's local WAL and on follower WALs. Losing this data requires simultaneous, unrecoverable failure of the owner node and enough follower nodes to prevent quorum recovery. For a replication factor of 3 with QUORUM consistency, this means two of three nodes must suffer unrecoverable disk failure before the next flush completes.

**The coordination layer exists to protect this window.** Lease management, WAL replication, epoch-based fencing, and failover protocols all serve a single purpose: ensuring that the unflushed window is covered by functioning, consistent replicas at all times. Once data reaches S3, the coordination layer is irrelevant to its durability.

---

## Epoch-Based Fencing

Ownership transfers create a dangerous edge case: a node that has lost ownership may not know it yet and may attempt to commit state changes that conflict with the new owner. This is the zombie writer problem.

**The scenario.** Node A owns partition P and begins flushing its memtable to S3. While the flush is in progress, Node A's lease expires (due to a network partition, GC pause, or other delay). Node B detects the expired lease, acquires ownership, increments the writer epoch, replays the follower WAL, and begins accepting writes. Node A's flush completes. If Node A could update the manifest with its new SSTable, the manifest would reflect Node A's state but not Node B's recent writes. Node B's unflushed data would be invisible to future recovery, because the manifest's `last_flushed_sequence` would advance past entries that only exist in Node B's WAL.

**The solution: writer epochs.** Every manifest carries a `writer_epoch` -- a monotonically increasing integer that identifies the current authorized writer.

The protocol on lease acquisition:

1. Acquire the partition lease via conditional write to S3.
2. Read the current manifest.
3. Increment `writer_epoch` by one.
4. Write a new manifest with the incremented epoch via compare-and-swap (CAS).
5. Only after step 4 succeeds, begin accepting writes.

Between steps 1 and 5, the node is in **fencing state**: it serves reads from the existing flushed state and any recovered WAL entries, but rejects all writes. This prevents a window where two nodes both believe they can write.

The protocol on manifest update (flush or other state change):

1. Read the current manifest.
2. If `manifest.writer_epoch > my_epoch`, the node is a zombie. Halt immediately. Abandon the in-progress operation.
3. Otherwise, proceed with the CAS protocol.

Because the new owner commits a higher epoch to the manifest before accepting any writes, the zombie's subsequent CAS attempt will always discover the higher epoch and stop.

**Compactor epochs** work identically using a separate `compactor_epoch` field, preventing zombie compaction workers from corrupting the SSTable set after an ownership change.

**Write barrier after epoch acquisition.** The new owner does not serve writes immediately after acquiring the epoch. A write barrier ensures that any in-flight operations from the previous owner have time to complete or be detected:

- **Lease expired (old owner presumed dead):** The barrier lasts approximately 5 seconds, covering the maximum expected in-flight operation time of the old owner.
- **Lease preempted (forced takeover during rebalancing):** The barrier lasts approximately 15 seconds, providing additional margin because the old owner may still be active and processing requests.

During the barrier, reads are served from the flushed manifest state combined with entries recovered from the follower WAL. The system is available for reads throughout the transition.

---

## Recovery Protocol

When a node starts up or takes ownership of a partition after failover, it reconstructs the partition's full state from two sources: the manifest in S3 (authoritative for all flushed data) and the local WAL (authoritative for unflushed data that survived the restart).

**Step 1: Fetch the manifest from S3.** The manifest is the single source of truth for which SSTables are live, at which levels, and what the last flushed sequence number is. The node reads the current manifest (identified by the highest manifest ID) and uses it as the foundation for all subsequent state.

**Step 2: Rebuild in-memory indexes.** The manifest contains metadata for every live SSTable -- key ranges, bloom filter offsets, index offsets, sizes. The node uses this metadata to reconstruct its in-memory index structures, SSTable registry, and level assignments without reading any SSTable data from S3.

**Step 3: Replay the WAL.** The node scans its local WAL segments (or follower WAL segments, in the case of failover) for entries with sequence numbers greater than the manifest's `last_flushed_sequence`. These entries represent writes that were acknowledged but not yet flushed. They are replayed into a fresh memtable, restoring the pre-crash in-memory state.

**Step 4: Resume normal operation.** The node begins serving reads (merging across the recovered memtable and the SSTable set from the manifest) and accepting writes (appending to the WAL and memtable as usual).

**The durability guarantee.** Zero data loss for any acknowledged write, provided the WAL on the owner or on at least one follower survived the failure. WAL replay is the bridge between the last flush and the crash point.

---

## Manifest as Commit Point

The manifest update is the atomic commit point for every state change in flushdb. Understanding this is key to understanding the system's correctness properties.

**Atomicity via CAS.** Manifest updates use S3 conditional writes (`If-None-Match: *`). A new manifest is written as a new S3 object with a sequentially higher ID. If two writers race to write the same next ID, exactly one succeeds and the other receives a precondition failure. This provides optimistic concurrency control without any external coordination service.

**WAL replay is idempotent.** If a crash occurs after a flush commits to the manifest but before the WAL is truncated, recovery will replay WAL entries that were already flushed. This is safe because replaying an already-flushed entry into the memtable simply produces a duplicate that will be resolved by sequence number during reads (the flushed copy in the SSTable and the replayed copy in the memtable have the same sequence number and content).

**Orphaned SSTables are harmless.** If a crash occurs after an SSTable is uploaded to S3 but before the manifest is updated to reference it, the SSTable exists on S3 but is invisible to readers. It is an orphan. A periodic garbage collection process lists all SSTable objects in S3, compares them against the manifest, and deletes any unreferenced objects older than a grace period. No data corruption results from orphaned SSTables; they are simply wasted storage until cleanup.

**Manifest rollback.** If a manifest update produces incorrect state, rolling back is straightforward: write a new manifest (with a higher ID, so it becomes the current one) containing the contents of a known-good previous manifest. The "rolled back" manifest supersedes the bad one by virtue of having a higher ID. Any SSTables referenced only by the bad manifest become orphans and are cleaned up by garbage collection.

---

## Group Commit Failure Scenarios

Group commit batches multiple writes into a single fsync and replication round-trip. The strict ordering of phases -- fsync, then replication, then ACK -- determines the outcome at each failure point.

| Failure Point | ACK Sent? | Data State | Outcome |
|---|---|---|---|
| Crash after fsync, before replication begins | No | Batch exists only on owner's local WAL | No data loss from the client's perspective. No ACK was sent, so clients will retry. If the owner's disk is recoverable, idempotency tokens deduplicate the replayed entries against retries accepted by any new owner. |
| Crash after partial replication (quorum not met) | No | Batch on owner's WAL and some followers, but not enough for quorum | No data loss from the client's perspective. No ACK was sent. The WAL reconciliation protocol collects partial entries from surviving followers and merges them, recovering what is available. |
| Crash after quorum replication, before ACK sent | No (but batch is durable) | Batch exists on owner and quorum of followers | The batch is fully durable. Clients did not receive an ACK, so they will retry. Retries are deduplicated via idempotency tokens. No data loss and no duplicates in the final state. |

The key invariant across all scenarios: a client only observes data loss if it received an ACK for a write that was subsequently lost. Since ACKs are only sent after fsync and quorum replication, this requires simultaneous unrecoverable failure of the owner and enough followers to break quorum -- before the next flush to S3.

---

## Consistency Guarantees

**Writes to the same partition are sequential.** Each partition has exactly one owner at any given time. All writes to a partition flow through that single owner, which assigns monotonically increasing sequence numbers. There is no concurrent write conflict within a partition.

**Writes to different partitions are independent.** flushdb provides no cross-partition transactions, no cross-partition ordering guarantees, and no mechanism for atomic multi-partition updates. Each partition is an independent unit of consistency.

**Read-after-write on the same partition is guaranteed.** Because both reads and writes for a partition are served by the same owner node, a read issued after a write ACK will always observe that write. The write is in the active memtable (or a frozen memtable, if a freeze occurred between the write and the read), which is checked before any SSTable.

**Follower reads are consistent for flushed data.** Followers can serve reads for data that has been flushed to S3. This data is immutable -- once an SSTable is written and referenced by the manifest, its contents never change. Follower reads against S3-backed data are consistent by definition, as they read from the same immutable objects the owner would read from. Followers do not serve reads for unflushed data, which avoids the consistency complications of reading from a replication stream that may be behind the owner's current state.
