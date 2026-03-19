# Multi-Tenancy

flushdb provides tenant isolation through **namespaces**. A namespace is a self-contained unit of data ownership, storage configuration, and operational lifecycle. There is no shared state between namespaces at the storage layer.

---

## Namespaces

A namespace is the primary unit of tenant isolation in flushdb. Each namespace owns:

- **Partition key schema** -- defines how record IDs map to partitions.
- **S3 path prefix** -- all objects (SSTables, manifests, blobs, chunks, leases) live under a namespace-scoped prefix.
- **Partition set** -- a fixed number of virtual partitions, independently owned and managed.
- **Manifest** -- the versioned metadata file that tracks all live SSTables, blob files, compaction watermarks, and sequence state.
- **Compaction lifecycle** -- each namespace runs its own leveled compaction pipeline (L0 through L3) with independent triggers and thresholds.

Data written to one namespace is invisible to every other namespace. There is no cross-namespace query path, no shared SSTable, and no shared manifest. Namespace boundaries are enforced at the S3 object layout level -- the path prefix makes accidental cross-reads structurally impossible.

---

## Namespace Configuration

A namespace is defined by a set of configuration parameters at creation time. Some parameters are immutable after creation (noted below); others can be tuned at runtime.

| Parameter | Description | Default | Notes |
|-----------|-------------|---------|-------|
| Partition key strategy | How record IDs are mapped to partitions. One of: Simple, Composite, Prefix, or Custom Hash. | -- | **Immutable after creation.** |
| Partition count | Number of virtual partitions. Must be a power of 2. | -- | **Immutable after creation.** |
| Storage layers | Ordered list of storage layers, each with a consistency scope (LOCAL or GLOBAL) and a consistency target (READ_YOUR_WRITES or EVENTUAL). Layers may optionally define a default TTL. | -- | Defines the persistence and caching topology. |
| Memtable size threshold | Size in bytes at which the active memtable is frozen and flushed to S3. | 64 MB | Runtime-tunable. |
| Bloom filter false positive rate | Target false positive rate for bloom filters on SSTable record IDs. | 1% (0.01) | Lower rates use more memory per SSTable. |
| Default page size | Byte budget for paginated read responses. | 2 MB | Runtime-tunable. |
| Max page size | Upper bound on page size a client can request. | 8 MB | Runtime-tunable. |
| Target latency SLO | Target p99 latency for read operations. Used by the read path to trigger early returns on partial pages. | Namespace-specific | Runtime-tunable. |
| Max latency SLO | Hard ceiling on request latency. The server stops issuing storage reads as the deadline approaches. | Namespace-specific | Runtime-tunable. |
| Write consistency level | Number of replicas that must acknowledge a write before it is considered durable. One of: ONE, QUORUM, or ALL. | QUORUM | Trades durability for write latency. |
| Replication factor | Number of nodes holding WAL replicas for each partition. | 3 | Determines the replica set size for write consistency and failover. |

---

## Partition Key Strategies

The partition key strategy determines how a record ID is turned into a partition assignment. It is chosen once at namespace creation and cannot be changed afterward.

### Simple

The record ID is used as the partition key directly. The hash of the full record ID determines the partition. Suitable when record IDs are already well-distributed (UUIDs, hashed identifiers).

### Composite

The record ID is treated as multiple fields joined by a delimiter. Only a subset of fields (the partition key fields) are hashed for partition assignment. For example, a record ID of `tenant-42:us-east-1:order-789` with a composite strategy on the first two fields would partition by `tenant-42:us-east-1`. This groups related records onto the same partition, enabling efficient range scans across records that share a partition key prefix.

### Prefix

The first N characters of the record ID are used as the partition key. Useful when record IDs have a natural hierarchical structure (paths, dotted names) and co-location of records sharing a prefix is desirable.

### Custom Hash

A named hash function is applied to the record ID before partition assignment. This allows operators to plug in domain-specific hashing logic -- for example, extracting a tenant ID embedded at an arbitrary position in the record ID, or applying a hash that accounts for known hot-key patterns.

### Constraints

- The partition key schema is **immutable** after namespace creation. Changing the strategy would require redistributing all data across partitions.
- The partition count must be a **power of 2**. This enables efficient bitwise partition assignment after hashing.

---

## Isolation Guarantees

Namespace isolation is not just logical -- it extends through every operational layer of the system.

**Storage isolation.** All S3 objects are scoped to the namespace path prefix. SSTables, manifests, blob files, chunk groups, and lease objects for one namespace are structurally separated from another. There is no shared object.

**Compaction isolation.** Each namespace runs its own compaction pipeline with independent level sizing, trigger thresholds, and scheduling. A namespace with heavy write amplification does not affect compaction progress in other namespaces.

**Flush isolation.** Memtable freeze and flush cycles are per-namespace. A namespace that flushes frequently (small memtable threshold, high write volume) does not create backpressure on other namespaces.

**Cache isolation.** Cache budgets and eviction decisions are scoped per namespace. A scan-heavy namespace cannot evict hot blocks belonging to a point-read-heavy namespace.

**Performance tuning isolation.** Bloom filter rates, page sizes, latency SLOs, write consistency levels, and memtable thresholds are all configured independently per namespace. Operators can tune each namespace for its specific workload characteristics without side effects on other tenants.

**Partitioning isolation.** Each namespace chooses its own partition key strategy and partition count. A namespace using composite keys with 256 partitions operates independently from a namespace using simple keys with 16 partitions.
