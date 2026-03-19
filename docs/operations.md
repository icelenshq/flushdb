# Operations

This page covers the operational aspects of flushdb: how data is laid out in S3, how keys are generated and distributed, tunable configuration parameters, and the metrics you should monitor in production.

---

## S3 Object Layout

Every namespace in flushdb maps to a well-defined prefix hierarchy within the S3 bucket:

```
s3://{bucket}/{hash-prefix}/flushdb/{namespace}/
  ├── manifests/     (versioned manifest JSON files)
  ├── sstables/
  │   ├── L0/        (flush output, individual SSTable files)
  │   ├── L1/        (compaction output, run fragments)
  │   ├── L2/        (run fragments)
  │   └── L3/        (run fragments)
  ├── blobs/         (separated large values, 32KB-4MB)
  ├── chunks/        (chunked very large values, >=4MB)
  ├── blob-refs/     (reference counting snapshots)
  └── leases/        (partition ownership lease keys)
```

**manifests/** -- Holds versioned snapshots of the storage engine state. Each manifest records which SSTables exist, their level assignments, key ranges, and sequence numbers. Older manifests are retained for crash recovery and rollback.

**sstables/** -- Organized by compaction level. L0 contains unsorted flush output (one file per memtable flush). L1 through L3 hold progressively larger, sorted run fragments produced by compaction. Higher levels contain older, less frequently accessed data.

**blobs/** -- Stores values between 32 KB and 4 MB that have been separated from their SSTable entries to keep block sizes efficient. Referenced by inline blob pointers in the SSTable.

**chunks/** -- Values 4 MB and larger are split into fixed-size chunks stored here. A chunk manifest in the SSTable entry describes reassembly order.

**blob-refs/** -- Periodic reference-counting snapshots that track which blobs and chunks are still live. Used during garbage collection to safely reclaim storage.

**leases/** -- S3-based lease keys that establish partition ownership. Each node must hold a valid lease to serve reads and writes for a partition.

---

## S3 Prefix Sharding

S3 partitions request throughput by key prefix. flushdb exploits this by distributing objects across 128 hash-based prefixes (the `{hash-prefix}` component in the path above). This sharding strategy provides:

| Property | Value |
|----------|-------|
| Number of hash prefixes | 128 |
| Aggregate PUT throughput (before throttling) | ~448,000 ops/sec |
| Aggregate GET throughput (before throttling) | ~704,000 ops/sec |

SSTable file IDs use ULIDs (Universally Unique Lexicographically Sortable Identifiers). ULIDs have two properties that make them ideal for this layout:

- **Time-sortable** -- The leading 48-bit timestamp means lexicographic sort order matches creation order, which simplifies manifest management and debugging.
- **Uniformly distributed** -- The trailing 80 random bits ensure that the hash of a ULID spreads evenly across all 128 prefixes, preventing hot-prefix bottlenecks.

---

## Ordered Key Generation

Every write version in flushdb receives a system-generated 12-byte ordered key. These keys serve as the internal sort order for the LSM tree and are distinct from user-facing record IDs.

**Format (12 bytes total):**

| Bytes | Length | Content |
|-------|--------|---------|
| 0-7 | 8 bytes | Timestamp in milliseconds since epoch |
| 8-9 | 2 bytes | Node ID |
| 10-11 | 2 bytes | Per-node sequence counter |

**Properties:**

| Property | Detail |
|----------|--------|
| Sort order | Monotonically increasing; naturally sorted by time |
| Keys per millisecond per node | ~65,000 |
| Maximum nodes | 65,536 |
| Node ID assignment | Drawn from an S3-based monotonic counter |

The timestamp prefix means that range scans over recent data are efficient (keys are clustered). The node ID prevents collisions across concurrent writers. The per-node sequence counter handles bursts within a single millisecond.

---

## Configuration Reference

The following parameters control storage engine behavior, compaction policy, write-ahead log sizing, and cluster coordination. All values listed are defaults.

### Storage Engine

| Parameter | Default | Description |
|-----------|---------|-------------|
| memtable_size_threshold | 64 MB | Size at which the active memtable is frozen and queued for flush |
| memtable_time_threshold | 5 min | Maximum time before a memtable is frozen regardless of size |
| block_size_target | 4 KB | Target size for SSTable data blocks |
| bloom_filter_fp_rate | 1% | Bloom filter false positive rate per SSTable |

### Write-Ahead Log

| Parameter | Default | Description |
|-----------|---------|-------------|
| wal_segment_size | 32 MB | Size of each WAL segment file |
| wal_max_total_bytes | 512 MB | Total WAL size that triggers a forced memtable flush |
| wal_backpressure_threshold | 256 MB | Total WAL size at which incoming writes are stalled |
| group_commit_interval | 200 us | Maximum wait time to batch multiple writes into a single WAL commit |
| group_commit_max_bytes | 256 KB | Maximum batch size for a single group commit |

### Compaction

| Parameter | Default | Description |
|-----------|---------|-------------|
| l0_compaction_trigger | 4 files | Number of L0 files that triggers a compaction |
| l0_slow_limit | 8 files | Number of L0 files at which writes are throttled |
| l0_hard_limit | 12 files | Number of L0 files at which writes are fully stalled |
| compaction_sag | 1.75 | Space amplification goal; compaction is scheduled to keep actual/logical ratio below this |
| tombstone_ttl | 7 days | Duration after which tombstones are eligible for removal during compaction |

### Cluster and Coordination

| Parameter | Default | Description |
|-----------|---------|-------------|
| lease_ttl | 30 s | Duration of a partition ownership lease |
| lease_renewal_interval | 10 s | How frequently a node renews its partition leases |
| replication_factor | 3 | Number of replicas for each partition |

### Request Handling

| Parameter | Default | Description |
|-----------|---------|-------------|
| idempotency_retention_ttl | 10 min | Window during which duplicate write tokens are detected and rejected |
| max_s3_gets_per_read | 8 | Maximum number of S3 GET requests allowed for a single read operation |
| default_page_size_bytes | 2 MB | Default response page size for scan and list operations |
| max_page_size_bytes | 8 MB | Maximum allowed response page size |
| manifest_snapshot_interval | 100 | Number of manifest versions between full snapshots |

### Latency SLOs

| Parameter | Default | Description |
|-----------|---------|-------------|
| target_latency_slo | configurable | p99 latency target; the system uses adaptive measures (throttling, cache warming) to stay within this bound |
| max_latency_slo | configurable | Absolute latency ceiling; requests exceeding this are terminated |

---

## Key Metrics

### Storage Engine

| Metric | What to watch |
|--------|---------------|
| Write throughput (ops/sec, bytes/sec) | Sustained throughput should be stable. Sudden drops indicate backpressure from WAL or L0 limits. |
| Read latency (p50, p95, p99) | p99 spikes often correlate with cache misses or compaction contention. Compare against configured SLOs. |
| Memtable flush frequency and duration | Frequent flushes with short durations are healthy. Long flush durations suggest S3 upload slowness or large memtables. |
| Compaction throughput and space amplification | Track the actual space amplification ratio against the configured `compaction_sag` target. Rising amplification means compaction is falling behind. |
| Cache hit rate per tier (DRAM, NVMe) | Low DRAM hit rates push reads to NVMe or S3. Low NVMe hit rates mean most reads go to S3 and latency will suffer. |

### Cluster

| Metric | What to watch |
|--------|---------------|
| Partitions owned/followed per node | Should be balanced across the cluster. Significant skew indicates a rebalance problem. |
| WAL replication latency to followers | Sustained increases risk data loss during failover. Should stay well below lease TTL. |
| Failover time (detection through serving resumed) | Measures total unavailability window. Composed of lease expiry detection, leader election, and WAL replay. |
| Rebalance duration on node join/leave | Long rebalances extend the period of uneven load. Monitor alongside partition count per node. |
| Gossip propagation delay | High delay means nodes have stale views of cluster membership, which can cause misrouted requests. |
| Lease renewal success/failure rate | Renewal failures are a leading indicator of node isolation or S3 connectivity issues. Consecutive failures lead to partition handoff. |
| Metadata cache hit rate and S3 fetch count | Frequent S3 fetches for metadata suggest the cache is undersized or invalidation is too aggressive. |

### CDC (Change Data Capture)

| Metric | What to watch |
|--------|---------------|
| Events enqueued vs dropped | Drops indicate the CDC buffer is overflowing. Increase buffer size or add sink capacity. |
| Sink send latency and error rate | Rising latency or errors in the downstream sink can cause buffer backpressure and eventually dropped events. |
| Buffer utilization percentage | Sustained high utilization (above 80%) is a warning that drops are imminent under load spikes. |
