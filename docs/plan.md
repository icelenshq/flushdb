# flushdb — Implementation Plan

## Context

Greenfield Rust project. No code exists yet — only PRD.md and STORAGE_DESIGN.md. This plan defines the incremental build order for a distributed KV store using S3 as source of truth with LSM-tree architecture.

## Architectural Decisions Baked In From Phase 1

1. **StorageBackend trait** with LocalFs impl — everything builds against a trait, S3 is just a production impl
2. **Single-owner data structures** — no Arc<Mutex<>>, shard-per-core is architectural
3. **Manifest as first-class concern** — not an afterthought, it touches flush/compaction/recovery/fencing
4. **SSTable entry format reserves BlobRef variant** — no format migration needed later
5. **Protobuf API types defined early** — storage engine implements the gRPC contract from the start

## Workspace Structure

```
flushdb/
  Cargo.toml                (workspace root)
  crates/
    flushdb-proto/          (protobuf + tonic generated code)
    flushdb-types/          (core types, traits, errors, StorageBackend)
    flushdb-wal/            (WAL writer/reader, group commit, segments)
    flushdb-storage/        (SSTable format, bloom filters, blocks)
    flushdb-engine/         (memtable, manifest, flush, compaction, read path, cache)
    flushdb-server/         (gRPC server, namespaces, partitioning, coordination)
    flushdb-test/           (integration tests)
```

---

## Phase 1: Foundation — Types, Traits, API Contract
**Complexity: M**

- CompositeKey (0x00 separator, record_id max 256B, item_key max 4096B, sort = memcmp)
- Item, MemtableEntry, EntryType (PUT/DELETE/RANGE_DELETE)
- IdempotencyToken (24B: 8B generation_time + 16B UUID)
- OrderedKey (12B: 8B timestamp + 2B node_id + 2B sequence)
- EntryValue enum: Inline(Bytes) | BlobRef { blob_id, offset, size }
- FlushError enum (IO, KeyTooLong, NotFound, PreconditionFailed, CrcMismatch, etc.)
- StorageBackend trait: put, get, get_range, delete, conditional_put, list_prefix
- LocalFsBackend implementation (filesystem-backed, for all testing)
- Protobuf: PutItems, GetItems, DeleteItems, ScanItems, Predicate, Selection

**Done when:** `cargo build --workspace` succeeds. CompositeKey round-trips. LocalFsBackend passes all trait methods. Proto compiles.

---

## Phase 2: WAL — Durable Local Writes
**Complexity: L**

- Segment-based WAL (32MB segments, `segment-{012d}.wal`)
- Entry wire format: length-prefixed + CRC32-protected (per STORAGE_DESIGN 5.2)
- WAL writer: append, rotate on segment size
- WAL reader: iterate with CRC validation, detect partial tail writes
- Group commit: buffer (200us / 256KB), batch fsync, batch notification
- Fsync modes: SYNC (default) and BATCH_SYNC
- Dirty segment tracking: generation_id -> highest_sequence per segment
- Segment deletion only when all referencing generations are flushed

**Done when:** Write 10K entries across 3+ segments, kill mid-write, replay recovers all valid entries. Group commit measurably batches fsyncs. Dirty tracking prevents premature segment deletion.

---

## Phase 3: Memtable — In-Memory Sorted Store
**Complexity: L**

- Skip list: single-owner, max height 12, probability 1/4
- Arena allocator: 1MB blocks, O(1) deallocation
- Point lookup, range scan iterator, full record scan
- RangeTombstoneIndex: sorted Vec, covers() check
- Freeze/swap: size (64MB) OR time (5 min) triggers
- Per-partition monotonic sequence number
- Idempotency dedup: HashSet<IdempotencyToken> on active + frozen memtables
- WAL backpressure: stall writes at 256MB WAL size

**Done when:** 100K random entries match BTreeMap oracle. Iteration is sorted. Range tombstones correctly hide covered keys. Freeze prevents inserts. Dedup rejects duplicate tokens.

---

## Phase 4: SSTable — Persistent Sorted Files
**Complexity: L**

- SSTable binary format: Header (magic 0x464C4442) -> Data Blocks (4KB) -> Dedup Block -> Bloom Filter -> Index Block -> Footer (80B)
- BlockBuilder: accumulate entries, compress (ZSTD/Snappy/None)
- BlockReader: decompress, iterate entries
- SSTableWriter: builds full SSTable, uploads to StorageBackend
- SSTableReader: read footer + index, seek to data block, iterate
- Bloom filter: over record_ids, 1% FPR, ~10 bits/key
- Dedup block: compact hash set, 128-bit per token
- Entry format supports EntryValue::Inline and EntryValue::BlobRef

**Done when:** Flush a memtable to SSTable via StorageBackend, read it back — all entries match. Bloom filter correctly filters absent record IDs. Dedup block round-trips.

---

## Phase 5: Manifest + Flush + Read Path + Compaction
**Complexity: XL** _(this is the critical phase — working embedded KV store)_

- **Manifest:** JSON with per-level SSTable metadata, key ranges, run IDs, writer_epoch, compactor_epoch, last_flushed_sequence. Versioned (20-digit zero-padded IDs). CAS via conditional_put. Snapshots every 100 versions. Prune old manifests
- **Flush:** Frozen memtable -> SSTableWriter -> StorageBackend -> manifest CAS -> WAL segment cleanup
- **Read path:** Merge-read across active memtable, frozen memtable(s), L0-L3 SSTables. Bloom filter elimination, index seek, merge-sort by composite key, tombstone filtering, byte-based pagination with page tokens
- **Compaction:** Leveled — L0(4 overlapping) -> L1(10, 640MB) -> L2(100, 6.4GB) -> L3(unbounded). Fragment-based (~1GB). Trivial move when no overlap. Partial range compaction
- **L0 write stalling:** soft(4) = trigger compaction, slow(8) = throttle, hard(12) = stall
- **Recovery:** Fetch manifest -> replay WAL beyond last_flushed_sequence -> rebuild memtable

**Done when:** Write 10M keys, kill process, recover from manifest + WAL — reads return correct data. Compaction keeps L0 bounded. Range scans with pagination work. This is a **working embedded KV store**.

---

## Phase 6: Caching + Read Optimizations
**Complexity: L**

- W-TinyLFU DRAM cache: window LRU (1%) + Count-Min frequency sketch + segmented main LRU (99%, 80/20 protected/probation)
- Logical cache: deserialized KV pairs, not raw bytes
- Index block + filter block pinning in DRAM
- Continuity tracking: cached key ranges tagged with manifest version, invalidated on compaction
- Compaction-aware eviction: evict blocks from compacted SSTables
- Coalesced block fetches: merge adjacent byte-range reads
- SSTable GET budget: cap at 8 per read
- Adaptive pagination: estimate items-per-page from avg item size

**Done when:** Repeated reads hit cache (<1ms). Full-record scan doesn't evict hot point-read data. Compaction invalidates stale cache entries.

---

## Phase 7: gRPC Server + Namespaces + Partitioning
**Complexity: XL**

- gRPC server (tonic): PutItems, GetItems, DeleteItems, ScanItems
- Namespace config: partition key strategy (Simple/Composite/Prefix/Custom), partition count (power-of-2), S3 prefix, tuning params
- Per-namespace isolation: separate S3 paths, manifests, compaction
- Partition key -> vnode mapping, per-partition engine instance
- Idempotency enforcement: dedup with 10-min retention TTL
- Client-server signaling: SLO exchange, compression negotiation
- OrderedKey generation per write
- S3 StorageBackend implementation (hash-based prefix sharding, 128 prefixes)
- Byte-based pagination with SLO-aware early return

**Done when:** gRPC client performs full CRUD cycle. Multiple namespaces are isolated. Partitioning distributes data correctly. S3 backend works against real/mock S3.

---

## Phase 8: Cluster — Gossip, Leases, Replication, Failover
**Complexity: XL**

- SWIM gossip: seed nodes, failure detection (2-3s), ~200B payload (node ID, address, ring version, partitions, lifecycle)
- Consistent hashing ring with vnodes
- S3 leases: versioned keys, 30s TTL, 10s renewal, adaptive renewal under brownout, LEASE_ENDANGERED state
- Epoch fencing: writer_epoch + compactor_epoch, startup protocol, write barrier (5s expired / 15s preempted)
- Request routing: coordinator -> partition owner via gossip
- WAL replication: owner replicates to W-1 followers, follower WAL, catchup protocol
- Consistency levels: ONE / QUORUM / ALL per namespace
- Failover: gossip detection -> lease acquire -> manifest fetch -> WAL reconciliation (3s timeout) -> write barrier -> serve (reads ~4s, writes ~12s)
- Follower reads for flushed data (allow_follower_read flag)
- Rebalancing on node join/leave

**Done when:** 3-node cluster. Kill one node. Partition ownership transfers within ~12s. No ACK'd QUORUM writes lost. Reads resume within ~4s.

---

## Phase 9: Value Separation + Large Values
**Complexity: L**

- Value separation: values >= 32KB stored as blob objects, SSTable stores BlobRef pointers
- Blob GC: reference counting via manifest deltas, refcount snapshot materialization, sampling for discard ratio, rewrite at >50% dead
- Large value chunking: values >= 4MB split into 4MB S3 chunk objects, parallel fetch on read
- Adaptive separation threshold: track write amplification, adjust 8KB-64KB with hysteresis
- Parallel value reads: up to 32 concurrent GETs during range scans

**Done when:** 1000 x 100KB values — compaction rewrites only pointers. Blob GC reclaims dead blobs. 5MB value round-trips via chunking.

---

## Phase 10: CDC + Shard-Per-Core Scheduler + Production Hardening
**Complexity: XL**

- **CDC:** Ring buffer emitter (64K events, 100ms drain), gap detection (sequence numbers), checkpoint heartbeats with dropped record IDs, Kafka sink (async, per-record-ID partition routing)
- **Shard-per-core:** Core-pinned data structures, SPSC queues, priority scheduler (P0 reads/writes, P1 flush, P2 compaction), 50us preemption
- **Cross-partition queries:** Partition bloom filters, progressive fan-out with early termination, affinity cache, composite page tokens, max_fan_out budget (64)
- **Ribbon filters:** For compaction SSTables (30% smaller than bloom)
- **Large-record optimization:** Per-block key fences, prefix bloom for records >10K items
- **NVMe cache tier:** Second-level W-TinyLFU, readahead for sequential scans
- **Metrics:** Write throughput, read latency p50/p95/p99, compaction stats, cache hit rate, lease renewals, WAL replication lag, failover timing

**Done when:** CDC events in Kafka within 200ms. Shard-per-core reduces tail latency under contention. Cross-partition queries return correct results. Full metrics dashboard.

---

## Key Dependencies (Rust Crates)

### Already in workspace (Phase 1)

| Crate | Version | Purpose |
|-------|---------|---------|
| tokio | 1 | Async runtime |
| tonic / prost / tonic-build / tonic-prost-build | 0.14 | gRPC + protobuf |
| bytes | 1 | Zero-copy byte buffers |
| crc32fast | 1 | CRC validation (WAL, SSTable) |
| thiserror | 2 | Error types |
| serde / serde_json | 1 | Manifest serialization |
| rand | 0.10 | Skip list random height |
| tracing | 0.1 | Structured logging |
| uuid | 1 | IdempotencyToken UUID generation |
| async-trait | 0.1 | Async trait methods |
| tempfile | 3 | Test temp directories |

### Add per phase

| Crate | Version | Phase | Purpose | Downloads |
|-------|---------|-------|---------|-----------|
| byteorder | 1 | 1 | Big-endian encoding for OrderedKey, CompositeKey | 502M |
| snap | 1 | 4 | Snappy compression (SSTable blocks) | 72M |
| zstd | 0.13 | 4 | Zstd compression (SSTable blocks) | 234M |
| ulid | 1 | 4 | SSTable file naming (sortable + unique) | 13M |
| tokio-util | 0.7 | 5 | Codec framing, async stream helpers | 462M |
| moka | 0.12 | 6 | W-TinyLFU cache (DRAM tier, PRD 5.1) | 60M |
| aws-sdk-s3 | 1 | 7 | S3 source of truth, conditional PUTs | 48M |
| aws-config | 1 | 7 | AWS credential/region resolution | 66M |
| tracing-subscriber | 0.3 | 7 | Log formatting/filtering for server | 331M |
| metrics | 0.24 | 7 | Observability facade (PRD Section 16) | 68M |
| metrics-exporter-prometheus | 0.18 | 7 | Prometheus metrics export | 23M |
| parking_lot | 0.12 | 7 | Fast mutexes (non-hot-path locks) | 652M |
| dashmap | 6 | 7 | Concurrent hash maps (metadata caches, PRD 6.7) | 220M |
| crossbeam | 0.8 | 8 | Lock-free SPSC queues (shard-per-core, PRD 5.3) | 83M |
| xorf | 0.12 | 10 | Ribbon/Xor filters for compacted SSTables (PRD 5.6, 30% smaller than bloom) | 1.8M |
| rdkafka | 0.39 | 10 | Kafka sink for CDC (PRD 9.3) | 23M |

## Milestone Summary

| Phase | Deliverable | Cumulative Result |
|-------|-------------|-------------------|
| 1 | Types + traits + proto | Everything compiles, interfaces locked |
| 2 | WAL | Durable local writes with crash recovery |
| 3 | Memtable | Fast in-memory sorted store |
| 4 | SSTable | Persistent sorted files on StorageBackend |
| **5** | **Manifest + flush + reads + compaction** | **Working embedded KV store** |
| 6 | Cache | Fast repeated reads, scan-resistant |
| 7 | gRPC + namespaces + partitions | Network-accessible multi-tenant server |
| 8 | Cluster coordination | Distributed, fault-tolerant |
| 9 | Value separation | Efficient large value handling |
| 10 | CDC + scheduler + hardening | Production-ready |

## Verification

After each phase, run the "done when" criteria. The critical gate is **Phase 5** — after that, you have a functional embedded KV store that can be benchmarked and tested end-to-end.
