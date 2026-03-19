# flushdb

**A distributed key-value database that stores data durably in S3 while delivering fast local writes through an LSM-tree storage engine.**

flushdb separates storage durability from the database cluster. Instead of managing disk replication, replica repair, and anti-entropy protocols internally, flushdb delegates durability to S3 -- which provides 11 nines of durability, strong read-after-write consistency, and unlimited capacity -- and focuses the cluster coordination layer solely on protecting in-flight writes that have not yet reached object storage.

---

## Why flushdb

Traditional distributed key-value stores like Cassandra and DynamoDB couple storage durability with the database cluster itself. This creates operational overhead: disk provisioning, replica repair, anti-entropy, and multi-node consistency protocols that exist only to keep data safe on local disks.

S3 solves the durability problem, but its latency (50-200ms per request) and lack of point write support make it unsuitable as a direct database backend.

flushdb bridges this gap. Writes land in a local write-ahead log and memtable in microseconds, are replicated to followers for redundancy, and flush to S3 in the background. The result is a system with the write performance of a local database, the durability of object storage, and a dramatically simpler operational model.

---

## Key Features

- **S3 as source of truth** -- All durable state lives in S3. Nodes are stateless from a durability perspective. Lose a node, and recovery is a manifest fetch plus WAL replay.

- **Fast local writes** -- Writes go to a WAL and in-memory memtable before acknowledgment. Group commit batches hundreds of writes into a single fsync, delivering high throughput without sacrificing durability.

- **Three-tier caching** -- A DRAM, NVMe, and S3 cache hierarchy keeps hot data in memory (sub-millisecond), warm data on local SSD (under 5ms), and cold data in S3. A W-TinyLFU admission policy ensures scans do not evict frequently accessed data.

- **Shard-per-core architecture** -- Each partition's data structures are pinned to a dedicated CPU core. Zero lock acquisitions on the read or write hot path.

- **Multi-tenant with namespace isolation** -- Each namespace gets its own partition key schema, S3 path prefix, compaction lifecycle, and performance tuning. Tenants are isolated at the storage layer, not just logically.

- **Two-level data model** -- Records identified by string IDs, each containing a sorted map of key-value items. This single primitive supports simple key-value lookups, sorted event streams, adjacency lists, named sets, versioned records, and more.

- **gRPC API** -- Four operations cover the full read/write surface: `PutItems`, `GetItems`, `DeleteItems`, and `ScanItems`, all scoped to a namespace and record.

- **Epoch-based fencing** -- Prevents zombie writers from corrupting state after ownership changes. Manifest updates are protected by S3 conditional writes and monotonic epoch numbers.

- **Leveled compaction** -- Background compaction merges SSTables across four levels (L0 through L3) using SSTable run fragments that bound temporary disk and S3 space usage.

- **Change Data Capture** -- A fire-and-forget CDC emitter pushes change events to pluggable sinks (e.g., Kafka) with per-record ordering and gap detection.

---

## How it Works

When a client writes data, the request is routed to the partition owner, which appends the entry to a local write-ahead log and inserts it into an in-memory memtable. The write is replicated to follower nodes and acknowledged to the client. Periodically, the memtable is frozen and flushed as an SSTable to S3. A versioned manifest in S3 tracks which SSTables are live at each level. Manifest updates use S3 conditional writes for optimistic concurrency control -- no external coordination service required.

Reads merge results across the memtable, frozen memtables, and SSTables from newest to oldest. Bloom filters on record IDs eliminate SSTables that cannot contain the target record before any data is fetched. For SSTables not in cache, the system issues targeted S3 byte-range reads to fetch only the relevant data blocks. The three-tier cache absorbs the vast majority of read traffic, keeping S3 latency off the critical path for hot data.

Cluster coordination is minimal by design. Partition ownership is tracked via S3-based distributed leases. Node discovery and failure detection use the SWIM gossip protocol. When a node fails, a follower acquires the lease, replays its local WAL, and resumes serving -- typically within 8-12 seconds for writes.

---

## Who is it For

- **Teams that want S3-tier durability without managing disk replication.** If your operational burden is dominated by disk provisioning, replica repair, and storage cluster maintenance, flushdb eliminates that layer entirely.

- **Write-heavy workloads that can tolerate eventual flush to object storage.** Writes are durable as soon as they are acknowledged (WAL plus quorum replication), but the data reaches S3 on a background schedule.

- **Multi-tenant platforms that need per-tenant isolation.** Namespace-level partitioning, compaction, and storage paths keep tenants separated without running separate clusters.

- **Systems that need change events from their key-value layer.** CDC support means you can react to data changes without bolting on a separate change tracking system.

---

## Documentation Guide

| Section | What you will find |
|---------|--------------------|
| [Data Model](concepts/data-model.md) | The two-level record/item data model, composite key encoding, and supported data patterns |
| [Multi-Tenancy](concepts/multi-tenancy.md) | Namespace configuration, partition key strategies, and per-tenant isolation |
| [API Reference](api.md) | gRPC operations, predicates, pagination, idempotency, and cross-partition queries |
| [Architecture Overview](architecture/overview.md) | Storage engine internals, write and read paths, compaction, and caching |
| [Cluster Coordination](cluster.md) | Partition ownership, S3-based leases, gossip protocol, WAL replication, and failover |
| [Operations](operations.md) | Deployment, configuration, monitoring, and troubleshooting |
| [Design Decisions](design-decisions.md) | Rationale behind key architectural choices |
| [Roadmap](roadmap.md) | Planned features and future directions |
