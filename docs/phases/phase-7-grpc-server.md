# Phase 7: gRPC Server + Namespaces + Partitioning

**Complexity: XL**
**Crate:** `flushdb-server`
**Design references:** STORAGE_DESIGN.md §4, §17, §19, §20, §23

---

## Goal

Expose the embedded KV engine over the network as a multi-tenant gRPC service with namespace isolation, partition key routing, and S3 as the production StorageBackend. After this phase, flushdb is a network-accessible database server.

---

## 1. gRPC Server

Built with `tonic`. Implements the four operations defined in Phase 1's protobuf:

### PutItems
- Route to partition owner based on `(namespace, record_id)`
- Check idempotency token (in-memory dedup → SSTable dedup block check)
- Write path: WAL append → memtable insert → generate OrderedKey version → ACK
- Return `PutItemsResponse { version }`

### GetItems
- Route to partition owner
- Execute merge-read path (Phase 5c) against the partition's engine instance
- Apply predicate filtering (match_keys / match_range / match_all)
- Apply selection (page_size_bytes, item_limit, exclude_values)
- Return paginated results with optional `next_page_token`

### DeleteItems
- Route to partition owner
- Write tombstone(s) based on predicate:
  - `match_all` → single record-level tombstone (constant latency)
  - `match_range` → single range tombstone covering `[start, end)`
  - `match_keys` → per-item tombstones with TTL + random jitter
- Return `DeleteItemsResponse { version }`

### ScanItems
- Server-side streaming variant of GetItems
- Streams batches of items using gRPC server streaming
- No page tokens — continuous stream until predicate exhausted or client cancels

---

## 2. Namespace Configuration

Each namespace provides tenant isolation with its own configuration:

```
NamespaceConfig {
  name:                    string
  partition_key_strategy:  SIMPLE | COMPOSITE | PREFIX | CUSTOM_HASH
  partition_count:         uint32       // must be power of 2
  s3_path_prefix:          string

  // Storage configuration
  persistence: List<StorageLayer> {
    id:     string
    type:   S3 | CACHE
    config: {
      consistency_scope:   LOCAL | GLOBAL
      consistency_target:  READ_YOUR_WRITES | EVENTUAL
      default_ttl:         optional<duration>
    }
  }

  // Performance tuning
  memtable_size_threshold: uint64      // bytes, default 64MB
  compaction_strategy:     LEVELED
  bloom_filter_fp_rate:    float       // default 0.01
  default_page_size_bytes: uint32      // default 2MB
  max_page_size_bytes:     uint32      // default 8MB
  target_latency_slo:      duration    // e.g., 10ms for p99
  max_latency_slo:         duration    // e.g., 500ms

  // Replication (for future cluster mode)
  write_consistency:       ONE | QUORUM | ALL
  replication_factor:      uint32      // default 3
}
```

**Isolation guarantees:** Each namespace has its own:
- S3 path prefix — no cross-namespace data access
- Manifest — independent versioning and flush lifecycle
- Compaction schedule — one namespace's compaction doesn't affect another
- Partition set — independent partitioning strategy

**Immutability:** Partition key strategy and partition count are immutable after namespace creation. Performance tuning fields are updatable at runtime.

---

## 3. Partition Key Strategies

Four strategies for mapping record IDs to partitions:

| Strategy | Description | Use Case |
|----------|-------------|----------|
| **Simple** | `hash(record_id) % partition_count` | Default. Uniform distribution. |
| **Composite** | Multiple fields via delimiter (e.g., `tenant:region`). Hash on extracted fields. | Multi-tenant with locality. |
| **Prefix** | First N characters of record_id → partition key | Natural prefix grouping (e.g., geo prefixes). |
| **Custom Hash** | Named hash function applied before partition assignment | When the client controls distribution. |

**Partition key → vnode mapping:** `hash(partition_key) % partition_count`. Partition count must be a power of 2 for efficient modular arithmetic.

---

## 4. Per-Partition Engine Instances

Each partition runs an independent engine instance:

```
Partition {
  partition_id:   u32
  namespace:      String
  engine:         Engine         // Phase 5 embedded KV engine
  wal:            WalWriter      // Phase 2 WAL
  active_memtable: Memtable     // Phase 3
  frozen_memtables: Vec<Memtable>
  manifest:       Manifest      // Phase 5a
  cache:          Cache         // Phase 6
}
```

The server maps incoming requests to partitions via the namespace's partition key strategy and dispatches to the correct engine instance. Each engine instance is self-contained — its own WAL, memtable, manifest, and compaction lifecycle.

---

## 5. Idempotency Enforcement

Full deduplication across the unflushed and flushed windows:

1. **In-memory dedup (hot path):** Check active + frozen memtable token sets (Phase 3)
2. **SSTable dedup blocks:** Check L0/L1 dedup blocks (pinned in DRAM). L2/L3 loaded on demand.
3. **Retention TTL:** Tokens older than `idempotency_retention_ttl` (default 10 minutes) accepted unconditionally — no dedup check needed.
4. **All-zero token:** Bypass dedup entirely.

---

## 6. OrderedKey Generation

The server generates 12-byte ordered keys for each write:

```
OrderedKey = [timestamp_ms: 8B big-endian] [node_id: 2B] [sequence: 2B]
```

- Monotonically increasing within a node
- ~65K keys per millisecond per node
- Node IDs assigned from a monotonic counter (could be stored in S3 via CAS for cluster mode)
- Returned to clients as the `version` in write responses

---

## 7. S3 StorageBackend

Production implementation of the `StorageBackend` trait targeting Amazon S3:

### S3 Object Layout

```
s3://{bucket}/{hash(object_id) % 128}/flushdb/{namespace}/
  ├── manifests/
  │   ├── manifest-00000000000000000001.json
  │   └── ...
  ├── sstables/
  │   ├── L0/{ulid}.sst
  │   ├── L1/{run-id}/frag-{index}.sst
  │   ├── L2/...
  │   └── L3/...
  └── manifest.json          (pointer to current manifest)
```

### Hash-Based Prefix Sharding

S3 has per-prefix throughput limits. All object keys are sharded across 128 hash prefixes:
- `hash(object_id) % 128` produces the prefix directory
- 128 prefixes yield up to 448K PUTs/s and 704K GETs/s before throttling

### Method Mapping

| StorageBackend Method | S3 Operation |
|----------------------|--------------|
| `put` | `PutObject` |
| `get` | `GetObject` |
| `get_range` | `GetObject` with `Range: bytes=start-end` header |
| `delete` | `DeleteObject` |
| `conditional_put` | `PutObject` with `If-None-Match: *` |
| `list_prefix` | `ListObjectsV2` with prefix filter |

### SSTable Upload Strategy

For SSTables > 16 MB, use S3 multipart upload:
- Part size: 16 MB
- Concurrency: Build part N+1 while uploading part N (double buffering)
- Peak memory: O(32 MB) regardless of SSTable size
- SSTables < 16 MB: single `PutObject`

---

## 8. Client-Server Signaling

### Server → Client (via handshake, refreshed periodically)
- Target/max latency SLOs
- Supported compression codecs
- Max page size
- Feature flags

### Client → Server (with each request)
- Compression capability
- Chunking support flag
- Client version

---

## 9. SLO-Aware Pagination

Byte-based pagination with SLO awareness:
- If accumulating items approaches the request's latency SLO (configured per namespace), stop early and return a partial page with a page token
- If gRPC deadline is nearly exhausted, stop issuing further StorageBackend reads
- Returns partial results rather than timing out completely

---

## New Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| aws-sdk-s3 | 1 | S3 source of truth, conditional PUTs |
| aws-config | 1 | AWS credential/region resolution |
| tracing-subscriber | 0.3 | Log formatting/filtering for server |
| metrics | 0.24 | Observability facade |
| metrics-exporter-prometheus | 0.18 | Prometheus metrics export |
| parking_lot | 0.12 | Fast mutexes (non-hot-path locks like namespace config) |
| dashmap | 6 | Concurrent hash maps (namespace → partition routing) |

---

## Future Work Considerations

When building Phase 7, keep the following post-Phase 7 dependencies in mind:

| What You're Building | Who Needs It Later | What To Watch For |
|---------------------|-------------------|-------------------|
| **Partition routing** | Consistent hashing ring (future cluster) | Currently `hash(record_id) % partition_count` routes to a local partition. Future cluster mode adds a consistent hashing ring where partitions map to physical nodes. Design the routing layer as a trait or pluggable strategy — `resolve(namespace, record_id) -> PartitionOwner` — so cluster mode can swap in ring-based routing without changing the gRPC handlers. |
| **Per-partition engine instances** | Partition migration (future cluster) | Future rebalancing moves partitions between nodes. Each partition must be independently startable/stoppable — clean lifecycle with `start()`, `freeze()`, `drain()`, `stop()`. Don't embed partition state in server-global structures. |
| **S3 StorageBackend** | WAL replication (future cluster) | Future cluster mode replicates WAL entries to follower nodes before ACKing. The write path currently does: WAL append → memtable insert → ACK. Cluster mode inserts replication between WAL append and ACK. Design the write path with a hook point for replication — don't hardcode the "WAL done = ACK" assumption. |
| **Namespace config** | CDC sinks (future), Dynamic reconfiguration | Future CDC adds sink configuration per namespace (Kafka topic, webhook URL). The `NamespaceConfig` struct should be extensible — use `#[serde(default)]` for new optional fields so existing serialized configs remain loadable. |
| **OrderedKey generation** | Cluster-wide ordering (future) | Single-node `node_id` assignment is trivial. Future cluster mode needs unique `node_id` per node — consider using S3 CAS for a monotonic node ID counter. The `node_id` field is already in OrderedKey; just make sure the assignment is pluggable. |
| **Idempotency enforcement** | Exactly-once delivery (future CDC) | CDC consumers need to know whether a write was a retry or a new write. Preserve the idempotency token in CDC events so downstream sinks can dedup independently. |
| **Metrics/observability** | Operational dashboards (future) | Future work adds Prometheus metrics and a Grafana dashboard. Instrument all critical paths with `tracing` spans and `metrics` counters now: write latency, read latency, flush duration, compaction throughput, cache hit rate, L0 file count. Adding instrumentation retroactively is painful. |
| **Graceful shutdown** | Zero-downtime deploys (future cluster) | Future cluster mode requires graceful ownership handoff. Shutdown must: (1) stop accepting writes, (2) flush active memtable, (3) release partition leases, (4) drain in-flight reads. Design shutdown as a multi-phase sequence, not a hard kill. |

---

## Done When

- gRPC client performs full CRUD cycle: PutItems → GetItems → DeleteItems → GetItems (confirms deletion)
- Multiple namespaces are isolated — writes to namespace A are invisible from namespace B
- Each namespace has its own manifest, WAL, and compaction lifecycle
- Partition key strategies work: Simple distributes uniformly, Composite extracts fields correctly
- Partitioning distributes data correctly — records route to the expected partition
- S3 backend works against real S3 (or LocalStack/MinIO for testing)
- S3 conditional_put correctly detects conflicts (returns PreconditionFailed)
- S3 hash-prefix sharding distributes objects across 128 prefixes
- Idempotency enforcement rejects duplicate tokens within retention window
- OrderedKey generation produces monotonically increasing versions
- ScanItems streams batches without requiring page tokens
- SLO-aware pagination returns partial results under deadline pressure
- Server starts, accepts connections, and gracefully shuts down
