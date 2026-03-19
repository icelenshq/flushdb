# Architecture Overview

flushdb is a distributed key-value database that uses Amazon S3 as its durable source of truth. It combines an LSM-tree storage engine with S3-backed persistence to deliver fast local writes, strong durability guarantees, and operationally simple scaling.

This page provides a high-level tour of the system. Each section links to a deeper dive for readers who want implementation-level detail.

---

## High-Level Architecture

flushdb separates fast writes from durable storage. The core insight: S3 already provides 11 nines of durability, strong read-after-write consistency, and unlimited capacity. There is no reason to replicate that work inside the database cluster.

```
                            +-------------------+
                            |     S3 Bucket     |
                            | (source of truth) |
                            |                   |
                            |  Manifests        |
                            |  SSTables (L0-L3) |
                            |  Blob files       |
                            |  Leases           |
                            +--------+----------+
                                     ^
                                     | flush / compaction / reads
                                     |
        +----------------------------+----------------------------+
        |                            |                            |
   +----+----+                 +-----+-----+                +----+----+
   | Node A  |  <-- gossip --> |  Node B   |  <-- gossip --> | Node C  |
   |         |                 |           |                 |         |
   | WAL     |                 | WAL       |                 | WAL     |
   | Memtable|                 | Memtable  |                 | Memtable|
   | Cache   |                 | Cache     |                 | Cache   |
   +---------+                 +-----------+                 +---------+
```

**Nodes are stateless from a durability perspective.** The only state that matters long-term lives in S3. A node's local disk holds the WAL (write-ahead log) and memtable, which together buffer writes that have not yet been flushed. Once a flush completes and the resulting SSTable lands in S3, the local data is redundant. The node can be terminated and replaced without data loss.

**The coordination layer exists solely to protect the unflushed window** -- the gap between when a write is acknowledged to the client (after WAL fsync + quorum replication) and when it reaches S3 (after memtable flush). WAL replication to follower nodes ensures that even if the owner crashes before flushing, the data survives on followers who can replay it. Once the flush commits, coordination has nothing left to protect.

This design means:

- No disk provisioning or replica repair. S3 handles storage.
- No anti-entropy protocols. S3 is the single source of truth.
- Cluster operations (scaling, replacement, upgrades) only need to drain the unflushed window, not migrate terabytes of data.

---

## Component Stack

The system is organized into five major layers, from client-facing down to durable storage.

```
+--------------------------------------------------------------+
|                     gRPC Server Layer                         |
|  Request routing, namespace management, cross-partition       |
|  fan-out, pagination, idempotency dedup, client signaling     |
+--------------------------------------------------------------+
|                     Coordination Layer                        |
|  SWIM gossip, S3-based leases, partition ownership,           |
|  WAL replication, epoch fencing, failover                     |
+--------------------------------------------------------------+
|                      Storage Engine                           |
|  WAL (segment-based, group commit)                            |
|  Memtable (skip list, arena allocator)                        |
|  SSTable (custom binary format, bloom/ribbon filters)         |
|  Compaction (leveled, fragment-based, preemptible)            |
|  Manifest (versioned, CAS via S3 conditional writes)          |
+--------------------------------------------------------------+
|                       Cache Layer                             |
|  DRAM  (<1ms)  -- hot blocks, filters, indexes                |
|  NVMe  (<5ms)  -- warm blocks evicted from DRAM              |
|  S3    (50-200ms) -- cold / authoritative data                |
+--------------------------------------------------------------+
|                        S3 Backend                             |
|  SSTables, manifests, blob files, chunk objects, leases       |
|  Hash-prefixed layout (128 shards) for throughput             |
+--------------------------------------------------------------+
```

### gRPC Server Layer

Exposes four operations -- PutItems, GetItems, DeleteItems, ScanItems -- all scoped to a namespace and record ID. The server layer handles request routing to the correct partition owner, cross-partition fan-out for queries that span multiple partitions, byte-based pagination, and idempotency token validation. Namespace configuration (partition strategy, replication factor, consistency level, SLOs) is managed here.

### Storage Engine

The LSM-tree engine that handles all reads and writes within a single partition. Writes go through WAL append and memtable insert. When the memtable reaches its size threshold (default 64 MB) or age limit (default 5 minutes), it is frozen and flushed to S3 as an L0 SSTable. Background compaction merges SSTables across four levels (L0 through L3) with a 10x size ratio between levels. The manifest -- a versioned JSON document in S3 -- tracks which SSTables are live at each level.

### Cache Layer

A three-tier cache sits between the storage engine and S3. DRAM holds hot data blocks, bloom/ribbon filters, and sparse indexes. NVMe stores warm blocks evicted from DRAM. S3 is the cold tier and the authoritative data source. The cache uses a W-TinyLFU admission policy (window LRU + frequency sketch + segmented main LRU) to resist scan pollution. Each CPU core owns its own cache partition, consistent with the shard-per-core model.

### Coordination Layer

SWIM-based gossip for membership and failure detection. S3-based distributed leases for partition ownership. WAL replication from partition owner to followers for protecting the unflushed window. Epoch-based fencing to prevent zombie writers after ownership transitions. The entire layer is designed to be as thin as possible -- it protects in-flight data and routes requests, nothing more.

---

## Shard-Per-Core Architecture

flushdb eliminates lock contention on the hot path by pinning each partition's data structures to a specific CPU core.

```
+-------------------------------------------------------------------+
|                          Node                                      |
|                                                                    |
|  Core 0                Core 1                Core N                |
|  +----------------+    +----------------+    +----------------+    |
|  | Partitions:    |    | Partitions:    |    | Partitions:    |    |
|  |   P0, P4, P8   |    |   P1, P5, P9   |    |   P3, P7, P11  |    |
|  |                |    |                |    |                |    |
|  | Memtable       |    | Memtable       |    | Memtable       |    |
|  | Frozen list    |    | Frozen list    |    | Frozen list    |    |
|  | WAL buffer     |    | WAL buffer     |    | WAL buffer     |    |
|  | Compaction st. |    | Compaction st. |    | Compaction st. |    |
|  | LRU cache      |    | LRU cache      |    | LRU cache      |    |
|  +-------+--------+    +-------+--------+    +-------+--------+    |
|          |                      |                      |           |
|          +----------+-----------+----------+-----------+           |
|                     |    SPSC queues       |                       |
|                     +----------------------+                       |
+-------------------------------------------------------------------+
```

### Data Is Partitioned, Not Shared

Each core owns its own active memtable, frozen memtable list, WAL buffer, compaction state, and LRU cache region. Virtual partitions (vnodes) are mapped to cores via `vnode_id % num_cores`. No data structure is shared between cores.

### Zero Locks on the Hot Path

Because each core is the sole writer and reader for its partitions, there are no lock acquisitions, no CAS loops, no contention on the write or read path. Memtable inserts are plain pointer writes. Sequence counters are plain integers. Arena offsets are plain integers.

### Cross-Core Communication

When a request arrives at a core that does not own the target partition, it posts a task to the owning core via a lock-free SPSC (single-producer, single-consumer) queue. The owning core executes the operation and returns the result through the same channel.

### Priority-Based Cooperative Scheduling

Each core runs a priority scheduler with three tiers:

```
+----------+---------------------------------------------+------------------+
| Priority | Tasks                                       | Preemption       |
+----------+---------------------------------------------+------------------+
| P0       | Reads, writes (memtable insert, WAL append) | Immediate        |
| P1       | Memtable flush, WAL group commit             | Yields within    |
|          |                                             | 10 microseconds  |
| P2       | Compaction, bloom/ribbon filter construction  | Yields within    |
|          |                                             | 50 microseconds  |
+----------+---------------------------------------------+------------------+
```

Compaction tasks (P2) check a per-core preemption flag every 50 microseconds. When a read or write arrives, the scheduler sets the flag, and the compaction task yields at its next checkpoint. This bounds worst-case compaction-induced read latency to 50 microseconds. Compaction throughput is not significantly impacted because it is I/O-bound (S3 reads and writes), not CPU-bound.

The result: client-facing operations are never blocked by background work. Reads and writes always run at P0 priority with immediate scheduling.

---

## Data Flow Summary

### Write Flow

```
Client
  |
  v
Coordinator (routes by partition key via consistent hash ring + lease)
  |
  v
Partition Owner (single core)
  |
  +---> WAL append (local disk, fsync via group commit)
  |
  +---> Replicate WAL batch to W-1 followers
  |
  +---> Insert into memtable (sorted by composite key)
  |
  +---> ACK to client (write is now durable)
  |
  +---> [Background, async] Push CDC event to ring buffer
  |
  +---> [Background, when memtable full or aged]
           |
           +---> Freeze memtable, swap in empty one
           +---> Build SSTable from frozen memtable
           +---> Upload SSTable to S3 (L0)
           +---> Update manifest via CAS (commit point)
           +---> Truncate covered WAL segments
           +---> Release frozen memtable memory
           +---> Trigger compaction if L0 count > 4
```

Key invariant: a write is only ACK'd after the WAL is fsynced locally and replicated to a quorum of followers. The manifest update is the commit point that makes data permanent in S3.

### Read Flow

```
Client
  |
  v
Coordinator (routes to partition owner)
  |
  v
Partition Owner
  |
  +---> Check active memtable
  +---> Check frozen memtable(s)
  +---> Check L0 SSTables (overlapping -- all checked in parallel)
  |       |
  |       +---> Bloom filter on record_id (in DRAM, microseconds)
  |       +---> Positive? Fetch data block via cache hierarchy
  |
  +---> Check L1 SSTables (non-overlapping -- at most one match)
  +---> Check L2 SSTables (non-overlapping)
  +---> Check L3 SSTables (non-overlapping)
  |
  +---> Merge results by sequence number (highest wins)
  +---> Apply tombstone filtering
  +---> Accumulate until byte budget exhausted
  |
  v
Return items + page token (if more data remains)
```

At each SSTable level, bloom filters (DRAM-resident) eliminate SSTables that do not contain the target record before any storage I/O occurs. For SSTables that pass the filter, the cache hierarchy is consulted: DRAM first, then NVMe, then S3. L0 data block fetches are issued concurrently since L0 SSTables can overlap, so effective latency for the L0 tier is a single round-trip rather than sequential.

---

## S3 as Source of Truth

### Why S3?

S3 provides three properties that are extremely expensive to build inside a database cluster:

- **11 nines of durability** (99.999999999%). Replicating this within a cluster requires complex quorum protocols, anti-entropy repair, and careful disk management.
- **Strong read-after-write consistency.** A successful PUT is immediately visible to all subsequent GETs. No eventual consistency surprises.
- **Unlimited capacity.** No disk provisioning, no shard splitting for storage reasons, no capacity planning.

### The Trade-Off

S3 has high per-request latency (~50ms) and does not support point writes (every write is a full object PUT). It is unsuitable as a direct backing store for a low-latency database.

### How flushdb Bridges the Gap

flushdb absorbs writes locally -- into a WAL for durability, into a memtable for fast reads -- and flushes them to S3 in bulk as immutable SSTables. This converts many small writes into few large sequential writes, which is exactly what S3 is optimized for.

```
  Many small writes                       Few large writes
  (client ops)                            (SSTable PUTs)
       |                                       |
       v                                       v
  +----------+     freeze      +---------+    upload    +-----+
  | Memtable | -------------> | SSTable | -----------> | S3  |
  +----------+     + build     +---------+              +-----+
       ^
       |
  +----------+
  |   WAL    |  (local disk, durability until flush)
  +----------+
```

After a flush completes and the manifest is updated, the data is permanently in S3. The local WAL segments are deleted. The node can be replaced, and the new node recovers by reading the manifest and replaying any unflushed WAL entries from followers.

### The Manifest

The manifest is a versioned JSON document stored in S3. It is the single source of truth for what SSTables are live, at which levels, and what the last flushed sequence number is. All manifest updates go through a compare-and-swap (CAS) protocol using S3 conditional writes (`If-None-Match: *`), providing optimistic concurrency control without any external coordination service.

Manifest IDs are zero-padded 20-digit integers. The current manifest is always the highest ID. Writing a new manifest IS the atomic update -- no two-phase commit, no pointer swap, no external lock.

---

## Links to Deeper Dives

- [Write Path](write-path.md) -- WAL segment architecture, group commit batching, fsync strategies, memtable freeze-and-flush pipeline, replication protocol
- [Read Path](read-path.md) -- merge-read across layers, bloom/ribbon filter elimination, byte-range S3 GETs, parallel L0 reads, pagination
- [Storage Engine](storage-engine.md) -- SSTable binary format, block building, compaction levels and triggers, fragment-based runs, tombstone lifecycle
- [Caching](caching.md) -- three-tier cache design, W-TinyLFU admission, per-shard LRU, continuity tracking, compaction-aware eviction, NVMe prefetch
- [Durability & Recovery](durability.md) -- epoch-based fencing, zombie writer detection, WAL reconciliation, failover timeline, manifest rollback
