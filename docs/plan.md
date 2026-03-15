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
    flushdb-engine/         (SSTable format, bloom filters, blocks, memtable, manifest, flush, compaction, read path, cache)
    flushdb-server/         (gRPC server, namespaces, partitioning)
    flushdb-test/           (integration tests)
```

---

## Phases

| # | Phase | Complexity | Deliverable | Details |
|---|-------|------------|-------------|---------|
| 1 | [Foundation — Types, Traits, API Contract](phases/phase-1-foundation.md) | M | Types + traits + proto | Everything compiles, interfaces locked |
| 2 | [WAL — Durable Local Writes](phases/phase-2-wal.md) | L | WAL | Durable local writes with crash recovery |
| 3 | [Memtable — In-Memory Sorted Store](phases/phase-3-memtable.md) | L | Memtable | Fast in-memory sorted store |
| 4 | [SSTable — Persistent Sorted Files](phases/phase-4-sstable.md) | L | SSTable | Persistent sorted files on StorageBackend |
| **5** | [**Manifest + Flush + Read Path + Compaction**](phases/phase-5-manifest-flush-read-compaction.md) | **XL** | **Manifest + flush + reads + compaction** | **Working embedded KV store** |
| 6 | [Caching + Read Optimizations](phases/phase-6-caching.md) | L | Cache | Fast repeated reads, scan-resistant |
| 7 | [gRPC Server + Namespaces + Partitioning](phases/phase-7-grpc-server.md) | XL | gRPC + namespaces + partitions | Network-accessible multi-tenant server |
| - | [Future Work](phases/future-work.md) | - | Cluster, CDC, large values | Post-Phase 7 priorities |

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

| Crate | Version | Phase | Purpose |
|-------|---------|-------|---------|
| byteorder | 1 | 1 | Big-endian encoding for OrderedKey, CompositeKey |
| snap | 1 | 4 | Snappy compression (SSTable blocks) |
| zstd | 0.13 | 4 | Zstd compression (SSTable blocks) |
| ulid | 1 | 4 | SSTable file naming (sortable + unique) |
| tokio-util | 0.7 | 5 | Codec framing, async stream helpers |
| moka | 0.12 | 6 | W-TinyLFU cache (DRAM tier) |
| aws-sdk-s3 | 1 | 7 | S3 source of truth, conditional PUTs |
| aws-config | 1 | 7 | AWS credential/region resolution |
| tracing-subscriber | 0.3 | 7 | Log formatting/filtering for server |
| metrics | 0.24 | 7 | Observability facade |
| metrics-exporter-prometheus | 0.18 | 7 | Prometheus metrics export |
| parking_lot | 0.12 | 7 | Fast mutexes (non-hot-path locks) |
| dashmap | 6 | 7 | Concurrent hash maps (metadata caches) |

## Verification

After each phase, run the "done when" criteria. The critical gate is **Phase 5** — after that, you have a functional embedded KV store that can be benchmarked and tested end-to-end.
