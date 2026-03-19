# Cluster Coordination

## Design Philosophy

The coordination layer exists for one reason: protecting the unflushed window -- the gap between when a write is ACK'd and when it reaches S3. S3 handles everything else.

Once a write is flushed to S3, it inherits 11 nines of durability and strong read-after-write consistency. At that point, the cluster adds nothing. There is no consensus protocol, no distributed transaction manager, no replica repair system. The coordination layer is deliberately thin -- it protects only the data that has not yet reached object storage.

```
  Client write
      |
      v
  +-------------------+
  | WAL fsync         |  <-- Write is on local disk
  | + quorum          |  <-- Write is replicated
  | replication       |
  +-------------------+
      |
      | ACK to client              <--- UNFLUSHED WINDOW starts here
      |
      v
  +-------------------+
  | Memtable          |  <-- Data in memory, protected by WAL + replicas
  +-------------------+
      |
      | Background flush
      v
  +-------------------+
  | S3 (SSTable)      |  <-- Durable. Coordination no longer needed.
  +-------------------+                <--- UNFLUSHED WINDOW ends here
```

The coordination layer exists to keep this window safe: partition ownership prevents write conflicts, WAL replication survives node loss, and lease-based fencing prevents zombie writers from corrupting flushed state.

---

## Partition Ownership

Each partition has exactly one owner node responsible for all reads and writes. There are no shared-ownership or multi-master partitions. This single-owner model eliminates write conflicts entirely at the partition level.

### Consistent Hashing Ring

Ownership is determined by a consistent hashing ring with virtual nodes. Each physical node occupies multiple positions on the ring, distributing partitions more evenly than a single-position-per-node scheme.

```
                    Consistent Hashing Ring

              Node A         Node B         Node C
              (v0,v3)        (v1,v4)        (v2,v5)
                |              |              |
    +-----------+--------------+--------------+-----------+
    |   +---v0--+  +---v1--+  |  +---v2--+   |           |
    |   |  P0   |  |  P1   |  |  |  P2   |   |           |
    |   |  P3   |  |  P4   |  |  |  P5   |   |           |
    |   +-------+  +---v3--+  |  +---v4--+   |--v5--+    |
    |              |  P6   |  |  |  P7   |   |  P8   |    |
    |              +-------+  |  +-------+   +-------+    |
    +--------------------------+--------------------------+
```

### Virtual Partitions

Virtual partitions are configured per namespace as a power-of-2 count. A record's partition is determined by `hash(record_id) % partition_count`, and the ring determines which node owns that partition.

| Concept | Description |
|---------|-------------|
| Virtual partition | A logical subdivision of the keyspace, always a power-of-2 count |
| Physical node | A running flushdb process that owns one or more virtual partitions |
| Virtual node | A position on the consistent hashing ring; each physical node holds multiple |
| Partition key | The input used to determine which virtual partition a record maps to |

### Partition Key Strategies

Four strategies are available, configured per namespace at creation time:

| Strategy | Behavior | Example |
|----------|----------|---------|
| Simple | Record ID is the partition key directly | `user:123` hashes as-is |
| Composite | Multiple fields joined via delimiter | `tenant-A:us-east:user:123` uses tenant + region |
| Prefix | First N characters of the record ID | First 8 bytes of `abcdefghij...` |
| Custom hash | Named hash function applied before assignment | Application-defined distribution |

Partition key strategy and partition count are immutable after namespace creation.

---

## S3-Based Distributed Leases

Partition ownership is tracked via versioned S3 lease keys. There is no external coordination service -- no ZooKeeper, no etcd, no Raft. S3 conditional writes (`If-None-Match: *`) provide compare-and-swap semantics natively.

### How It Works

Each partition has a sequence of lease keys stored in S3. The highest lexicographic version is the current lease. Each lease key contains the owner node, the writer epoch, an expiration timestamp, and a pointer to the previous version.

```
leases/partition-007/
    lease-00000000000000000001.json     <-- expired
    lease-00000000000000000002.json     <-- expired
    lease-00000000000000000003.json     <-- CURRENT (highest version)
```

### Lease Timing

| Parameter | Value |
|-----------|-------|
| TTL | 30 seconds |
| Renewal interval | 10 seconds |
| Renewal attempts before expiry | 3 |
| Version gap cap | 100 between current and oldest surviving key |

### Acquisition Protocol

```
Step 1.  List all lease keys for the partition
         Identify current lease (highest version)

Step 2.  If no lease exists or the current lease is expired:
           Compute next_version = current_version + 1

Step 3.  PUT new lease key with If-None-Match: *
           +-- Success --> record as held_lease_version,
           |               proceed to fencing and write barrier
           +-- HTTP 412 (conflict) --> re-list and retry from Step 1
```

On a 412 response, another node won the race. The losing node re-reads the lease state and retries if the winner's lease subsequently expires.

### Renewal Protocol

```
Step 1.  Compute next_version = held_lease_version + 1

Step 2.  PUT new lease key with If-None-Match: *
           +-- Success --> update held_lease_version,
           |               delete lease keys older than current - 5
           +-- Failure --> enter LEASE_ENDANGERED state
```

Renewal creates a new versioned key rather than updating an existing one. This preserves an auditable history and avoids read-modify-write races.

### Resilience Mechanisms

| Mechanism | Trigger | Behavior |
|-----------|---------|----------|
| Adaptive interval | S3 PUT p99 > 500ms | Shorten renewal interval from 10s to 5s |
| Immediate re-renewal | Single PUT latency > 5s | Fire follow-up renewal immediately |
| LEASE_ENDANGERED | Renewal failure | Continue serving reads, pause write ACKs, retry aggressively |
| Yield | 3 consecutive renewal failures | Relinquish ownership |
| Jittered timing | Always active | +/- 2 seconds across partitions to avoid thundering herd |

The LEASE_ENDANGERED state is the key resilience feature. When a renewal fails, the node does not immediately give up. It continues serving reads (which are safe because the node still has the most recent data), pauses write acknowledgments (so clients know to retry), and retries renewals aggressively. Only after three consecutive failures does the node yield ownership.

### Lease Discovery

Non-owner nodes learn about partition ownership through gossip, not by polling S3. During the convergence window after a failover, a stale route returns `NOT_PARTITION_OWNER`. The coordinator then falls back to reading the lease directly from S3 as a correction mechanism.

---

## SWIM Gossip Protocol

Node discovery and failure detection use the SWIM (Scalable Weakly-consistent Infection-style Membership) protocol. Nodes find each other via seed nodes -- either static configuration or DNS -- and exchange health and metadata over UDP.

### Gossip Payload

Each node's gossip payload is approximately 200 bytes and contains:

| Field | Purpose |
|-------|---------|
| Node ID | Unique identifier for the node |
| gRPC address | Endpoint for inter-node RPCs |
| Ring version | Detects ring topology changes |
| Owned partitions | Current partition assignments for this node |
| Manifest versions | Per-partition manifest version for cache invalidation |
| Lifecycle status | JOINING, ACTIVE, LEAVING, or DEAD |

### Failure Detection

SWIM detects node failures in approximately 2-3 seconds through its cycle of direct pings, indirect pings via other members, and suspect declarations.

```
Node A                    Node B                    Node C
  |                         |                         |
  |--- direct ping -------->|                         |
  |                         |  (no response)          |
  |                         |                         |
  |--- indirect ping req ---|------------------------>|
  |                         |                    ping |---> Node B
  |                         |                         |     (no response)
  |                         |                         |
  |<-- "B unreachable" -----|-------------------------|
  |                         |                         |
  | Mark B as SUSPECT       |                         |
  | (after timeout: DEAD)   |                         |
```

### Lifecycle States

```
    +----------+     Ring        +----------+
    | JOINING  | -------------> |  ACTIVE  |
    +----------+   assigned     +----------+
                                  |      |
                         Graceful |      | Failure
                         shutdown |      | detected
                                  v      v
                             +--------+ +------+
                             |LEAVING | | DEAD |
                             +--------+ +------+
                                  |
                                  v
                             +------+
                             | DEAD |
                             +------+
```

| State | Meaning |
|-------|---------|
| JOINING | Connected to cluster, not yet assigned partitions |
| ACTIVE | Participating in the ring, owning and serving partitions |
| LEAVING | Graceful shutdown in progress, draining partitions to other nodes |
| DEAD | Removed from ring, partitions available for re-acquisition |

---

## Metadata Caching

All S3 metadata -- partition schemas, ring state, manifests, lease information -- is cached in memory on every node.

### Gossip-Based Invalidation

Rather than polling S3 on a timer, nodes rely on gossip to learn when metadata has changed. When a node updates a manifest or acquires a lease, it bumps the relevant version in its gossip payload. Other nodes detect the version change and fetch the updated object from S3 on demand.

```
                          Steady State

  Node A (owner)                    Node B                    Node C
       |                               |                         |
       | Flush completes,              |                         |
       | manifest v5 -> v6            |                         |
       |                               |                         |
       |-- gossip: manifest=v6 ------>|                         |
       |-- gossip: manifest=v6 ------------------------------>  |
       |                               |                         |
       |                  (B sees v6,  |                         |
       |                   fetches     |                         |
       |                   manifest    |                         |
       |                   from S3)    |                         |
```

### Recurring S3 Writes

During normal operation, the only recurring S3 writes are lease renewals: one PUT per owned partition every 10 seconds. All other S3 interactions (manifest reads, SSTable fetches) are triggered by events, not timers.

| Operation | Frequency | Direction |
|-----------|-----------|-----------|
| Lease renewal | 1 PUT per partition per 10 seconds | Write |
| Manifest fetch | On gossip-detected version change | Read |
| SSTable fetch | On cache miss during read path | Read |
| Lease discovery | On `NOT_PARTITION_OWNER` fallback | Read |

---

## WAL Replication

Before a write is acknowledged to the client, the partition owner replicates the WAL batch to W-1 follower nodes. Followers store replicated entries in a dedicated follower WAL on local disk. They do not build a memtable for replicated data -- the follower WAL exists solely as a recovery resource.

### Replication Flow

```
 Client                Owner                 Follower A          Follower B
   |                     |                       |                   |
   |-- PutItems -------->|                       |                   |
   |                     |-- WAL append + fsync  |                   |
   |                     |                       |                   |
   |                     |-- Replicate batch --->|                   |
   |                     |-- Replicate batch --------------------------->|
   |                     |                       |                   |
   |                     |<-- ACK ---------------|                   |
   |                     |          (quorum met) |                   |
   |<-- OK + version ----|                       |                   |
```

The owner batches writes (group commit) and replicates the batch as a single message, reducing network round trips. A write is only ACK'd after both local fsync and quorum replication succeed.

### Consistency Levels

Configured per namespace. Controls how many replicas must acknowledge a write before the client receives a response.

| Level | Write Acknowledged After | Survives | Latency |
|-------|--------------------------|----------|---------|
| ONE | Owner WAL fsync only | Nothing (data lost if owner dies before flush) | Lowest |
| QUORUM | Majority of replicas ACK | Minority of replicas failing | Moderate (default) |
| ALL | All replicas ACK | Any single replica surviving | Highest |

Reads always go to the partition owner (R=1). Follower reads are available for flushed data only, enabled by a per-request flag. This is safe because flushed data is immutable in S3 -- all replicas see the same SSTable contents.

### Follower Catchup Protocol

When a follower detects a gap in its received sequence numbers, it initiates catchup:

```
Step 1.  Follower sends CatchupRequest(partition_id, last_sequence)
         to the partition owner

Step 2.  Owner streams WAL entries from the requested sequence forward
           +-- If entries already truncated (flushed to S3):
           |     respond with CatchupFromManifest(manifest_version)
           +-- Otherwise: stream entries normally

Step 3.  If follower falls >1 full flush behind:
           Marked CATCHING_UP, excluded from write quorum
```

A follower in CATCHING_UP state does not count toward quorum. It must close the gap before it participates in write acknowledgments again.

---

## Failover

When a node dies, gossip detects the failure and surviving followers race to acquire the orphaned partition's lease from S3.

### Timeline

```
T+0s          Node failure occurs
              |
T+2-3s        Gossip failure detection completes
              |
T+3-4s        Lease acquisition via S3 CAS + manifest fetch
              |
              +-- Begin serving reads (~3-4 seconds total)
              |
T+3-7s        WAL reconciliation (0-3 seconds, configurable)
              |
T+7-12s       Write barrier expires
              |
              +-- Begin serving writes (~8-12 seconds total)
```

| Phase | Duration | Activity |
|-------|----------|----------|
| Failure detection | 2-3 seconds | SWIM direct ping, indirect ping, suspect declaration |
| Lease + manifest | ~1 second | S3 conditional write for lease + manifest GET |
| WAL reconciliation | 0-3 seconds | Merge follower WAL entries beyond last flush point |
| Write barrier | ~5 seconds | Fence zombie writers; serve reads only during this period |

### WAL Reconciliation

The new owner must recover unflushed writes that the dead node had replicated to followers but not yet flushed to S3. This is the critical recovery operation.

```
Step 1.  New owner reads its local follower WAL
         Identifies entries with sequence numbers beyond the last
         flushed sequence recorded in the manifest

Step 2.  Broadcasts reconciliation request to all surviving followers
         with the partition ID and the sequence range it needs

Step 3.  Each follower responds with WAL entries the new owner
         is missing from that range

Step 4.  New owner merges all received entries
         Deduplication by sequence number (identical sequences = same write)
         Replays merged entries into its memtable

Step 5.  If gaps remain after timeout and no other followers are reachable:
         Log as data loss event with specific missing sequence ranges
         Emit metric for alerting
         Serve with available entries
```

The reconciliation timeout defaults to 3 seconds (configurable up to 10 seconds). The new owner completes early if all followers respond before the timeout.

### Epoch-Based Fencing

A zombie writer is a node that lost its lease but still attempts to update the manifest -- for example, completing a flush that started before the lease expired. Without fencing, this could overwrite the new owner's state.

```
 Node A (zombie)                              Node B (new owner)
   |                                              |
   | Owns partition, epoch=5                      |
   | Begins flush...                              |
   |                                              |
   | --- network partition / lease expires ---     |
   |                                              | Detects A is dead
   |                                              | Acquires lease
   |                                              | Writes manifest with epoch=6
   |                                              | Begins serving
   |                                              |
   | Flush completes                              |
   | Reads manifest...                            |
   | Sees epoch=6, mine is 5                      |
   | ZOMBIE DETECTED. HALT.                       |
   | Abandoned SSTable cleaned up by GC           |
```

Each lease acquisition increments the `writer_epoch` in the manifest via CAS. Any subsequent manifest update checks: if the manifest's writer epoch exceeds the node's own epoch, the node is a zombie and must halt immediately. The orphaned SSTable on S3 is cleaned up by garbage collection.

The write barrier duration after acquiring a lease depends on how the previous owner was displaced:

| Takeover Reason | Write Barrier Duration | Rationale |
|-----------------|------------------------|-----------|
| Lease expired (owner presumed dead) | 5 seconds | Allow in-flight flushes to complete or fail |
| Lease preempted (rebalance) | 15 seconds (half of lease TTL) | Old owner may still be actively writing |

During the write barrier, the new owner serves reads from the flushed manifest state plus any recovered WAL entries, but rejects all writes.

---

## Rebalancing

When a node joins or leaves the cluster, only partitions on the affected ring segment move. The majority of partitions are unaffected.

```
Before (3 nodes):             After (4 nodes, Node D joins):

  A: [P0, P3, P6]               A: [P0, P3]          <-- lost P6
  B: [P1, P4, P7]               B: [P1, P4, P7]      <-- unchanged
  C: [P2, P5, P8]               C: [P2, P5, P8]      <-- unchanged
                                 D: [P6]              <-- acquired P6
```

S3 stores the authoritative ring state. Gossip propagates ring version changes to all nodes.

### Transfer Sequence

Because all flushed data lives in S3, partition transfer does not require copying data between nodes. Only the unflushed WAL window requires reconciliation.

```
Step 1.  Ring state updated (new node added or existing node removed)

Step 2.  Affected partition's old owner:
           Flushes any unflushed memtable data to S3
           Relinquishes lease

Step 3.  New owner:
           Acquires lease via S3 CAS
           Reads manifest from S3
           Performs WAL reconciliation with followers
           Begins serving after write barrier
```

The total transfer time per partition follows the same timeline as failover (8-12 seconds to full read-write availability), with the addition of the forced flush by the old owner. Graceful transfers (LEAVING state) allow the old owner to complete in-flight flushes before relinquishing, reducing reconciliation work for the new owner.
