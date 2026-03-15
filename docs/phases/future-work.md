# Future Work

Scoped for after Phase 7 (working network-accessible server). Prioritize based on production needs.
**Design references:** STORAGE_DESIGN.md §14, §16, §21

---

## Cluster Coordination

**Design reference:** STORAGE_DESIGN.md §16

Full distributed coordination to protect the unflushed window across multiple nodes:

- **SWIM gossip:** Node discovery and failure detection (~2-3s detection). Nodes exchange health/metadata over UDP. Gossip payload (~200 bytes): node ID, gRPC address, ring version, owned partitions, per-partition manifest versions, lifecycle status (JOINING/ACTIVE/LEAVING/DEAD).
- **Consistent hashing ring:** Virtual partitions mapped to physical nodes via ring. `vnode_id % num_cores` for core affinity.
- **S3-based distributed leases:** Partition ownership via versioned S3 lease keys with `If-None-Match: *` conditional writes. 30-second TTL, 10-second renewal intervals. No external coordination service required.
- **WAL replication:** Before ACKing, owner replicates WAL entries to W-1 followers. Three consistency levels: ONE / QUORUM / ALL. Follower catchup protocol for gap detection and recovery.
- **Failover:** Gossip detection (2-3s) → lease acquisition (~1s) → WAL reconciliation (0-3s) → write barrier (~5s). Total: reads in ~3-4s, writes in ~8-12s. Epoch-based fencing prevents zombie writers.
- **Rebalancing:** On node join/leave, only partitions on the affected ring segment move. S3 stores authoritative ring state.

---

## Value Separation + Large Values

**Design reference:** STORAGE_DESIGN.md §14

Tiered strategy to reduce write amplification for large values:

| Value Size | Strategy |
|-----------|---------|
| < 32 KB (default, adaptive) | Inline in SSTables |
| 32 KB – 4 MB | Separated into blob objects on S3, SSTable stores `BlobRef` pointer |
| >= 4 MB | Chunked into 4 MB S3 chunk objects |

- **Adaptive separation threshold:** Adjusts per namespace based on write amplification ratio. Hysteresis: changes require 10-minute persistence, move in 8 KB steps.
- **Blob GC:** Sample blob files for live/dead ratio. Rewrite when dead > 50%. Reference counting via append-only S3 refcount log. Deferred deletion with 30-minute safety fallback.
- **Chunking:** Values >= 4MB split into 4MB parts. Parallel fetch on read (up to 32 concurrent GETs). GC via orphan detection with 4-hour grace period.

---

## CDC + Production Hardening

**Design reference:** STORAGE_DESIGN.md §21, §4

### Change Data Capture
- **Ring buffer emitter:** Bounded 64K events, background drain loop, 100ms batch timer. Oldest events silently overwritten when full.
- **Gap detection:** Monotonic `cdc_sequence_number` per partition. 1-second heartbeat checkpoints with drop counts and affected record IDs (bounded 10K LRU set).
- **Kafka sink:** Async producer, same-record events routed to same Kafka partition. Topic: `flushdb.{namespace}.changes`.

### Shard-Per-Core Scheduler
- Core-pinned data structures, SPSC (single-producer single-consumer) queues for cross-core communication
- Priority-based cooperative scheduling: P0 (reads/writes, immediate), P1 (flush/WAL commit, 10μs yield), P2 (compaction/GC, 50μs yield)
- Compaction checks preemption flag every 50μs, bounds read latency to 50μs worst case

### Additional Optimizations
- **Cross-partition queries:** Coordinator fan-out with bloom filter pruning, progressive fan-out with early termination
- **Ribbon filters:** For compaction-produced SSTables (L1+), 30% smaller than bloom at same FPR. Higher construction cost acceptable at compaction time.
- **NVMe cache tier:** Warm blocks evicted from DRAM, readahead for sequential scans (prefetch next 4 blocks after 3+ consecutive reads)
- **Metrics:** Write throughput (ops/sec, bytes/sec), read latency (p50/p95/p99), memtable flush frequency/duration, compaction throughput, cache hit rate, partition ownership, WAL replication latency, failover time

---

## Future Dependencies

| Crate | Purpose |
|-------|---------|
| crossbeam | Lock-free SPSC queues (shard-per-core) |
| xorf | Ribbon/Xor filters for compacted SSTables |
| rdkafka | Kafka sink for CDC |
