# Roadmap

This document outlines planned future work for flushdb beyond the initial release. Items are grouped by theme rather than strict priority order, and scope may evolve as the project matures.

---

## Cluster Coordination

Full distributed coordination for multi-node deployment.

- **SWIM gossip protocol** for node discovery and failure detection, targeting roughly 2-3 second detection latency.
- **Consistent hashing ring** for partition distribution across nodes, ensuring balanced data placement.
- **S3-based distributed leases** for partition ownership, removing the need for any external coordination service such as ZooKeeper or etcd.
- **WAL replication to followers** to protect data that has not yet been flushed to S3.
- **Failover with WAL reconciliation** -- reads restored in approximately 3-4 seconds, writes in approximately 8-12 seconds after a node failure.
- **Rebalancing on node join/leave** -- only the affected ring segment moves, minimizing data transfer during topology changes.

---

## Value Separation and Large Values

A tiered strategy to reduce write amplification when storing large values.

- **Values between 32KB and 4MB** are separated into S3 blob objects. SSTables store lightweight pointers instead of the full value.
- **Values 4MB and above** are chunked into 4MB S3 objects for reliable upload and parallel retrieval.
- **Adaptive separation threshold** that adjusts automatically based on observed write amplification ratio.
- **Blob garbage collection** using reference counting with deferred deletion to avoid pauses.
- **Parallel value reads** supporting up to 32 concurrent S3 GETs for large-value reconstruction.

---

## Change Data Capture

Fire-and-forget event streaming from the storage engine.

- **Bounded ring buffer emitter** holding up to 64K events, with oldest events overwritten when full.
- **Gap detection** via monotonic sequence numbers and periodic checkpoint heartbeats, so consumers can identify and handle missed events.
- **Pluggable sink model** with Kafka as the initial implementation target.
- **Per-record ordering** within Kafka partitions to preserve causal consistency for individual keys.
- **Topic naming convention**: `flushdb.{namespace}.changes`.

---

## Production Hardening

Work aimed at operational maturity and predictable performance under sustained load.

- **Full shard-per-core scheduler** with preemption and priority-based cooperative scheduling to eliminate cross-core contention.
- **I/O scheduling groups** with token buckets for S3 bandwidth management, preventing any single operation from monopolizing throughput.
- **Compaction rate limiting** to avoid saturating S3 bandwidth during heavy compaction cycles.
- **Remote stateless compaction** allowing compaction work to scale elastically via serverless workers, decoupling compaction capacity from the serving tier.
- **Deterministic simulation testing** in the style of FoundationDB for rigorous verification of distributed protocol correctness.

---

## Advanced Optimizations

Performance and efficiency improvements across the storage engine.

- **Cross-partition queries** with coordinator fan-out and bloom filter pruning to reduce unnecessary partition reads.
- **Ribbon filters** for compaction-produced SSTables, offering approximately 30% smaller filter size compared to standard bloom filters.
- **NVMe cache tier** with sequential scan readahead for workloads that benefit from local storage.
- **Merge operators** for update-heavy workloads such as counters and append-to-list, eliminating read-before-write overhead.
- **Compaction filters for TTL-based garbage collection**, piggybacked on existing compaction I/O to avoid dedicated cleanup passes.
- **SSTable encoding optimizations** including varint encoding, delta-encoded timestamps, and record ID deduplication, targeting up to 53% file size reduction in favorable cases.
- **L0 sublevels with flush split keys** enabling concurrent non-overlapping compactions from L0 into L1.

---

## Long-Term Vision

Directions under consideration for the longer term. These represent areas of exploration rather than committed plans.

- **Tablets** -- per-sub-range independent LSM trees for granular data migration and independent compaction scheduling.
- **Distributed cache** with consistent hashing for sharing warm blocks across nodes, reducing redundant S3 reads.
- **Raft-based metadata coordination** as an alternative to gossip combined with S3 leases, for deployments that prefer stronger consistency guarantees on metadata.
- **Point-in-time query support** via delta/image layers, inspired by the Neon architecture, enabling reads at arbitrary past timestamps.
- **Trie-based partition index** for O(key length) lookups, replacing the current sparse binary search index for improved worst-case lookup performance.
- **Log-only replicas** for faster failover, inspired by the DynamoDB approach of maintaining replicas that replay the write-ahead log without building full LSM state.
