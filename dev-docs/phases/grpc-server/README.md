# Phase 7: gRPC Server + Namespaces + Partitioning — Subtask Breakdown

## Overview

Phase 7 transforms the embedded KV engine into a network-accessible, multi-tenant gRPC service. It adds the S3 production StorageBackend, namespace-level isolation with per-tenant partitioning strategies, four gRPC operations (PutItems, GetItems, DeleteItems, ScanItems), SLO-aware pagination, observability, and graceful lifecycle management. After this phase, flushdb is a deployable database server.

**Crates:** `flushdb-server`, `flushdb-test`
**Design references:** STORAGE_DESIGN.md §4, §17, §19, §20, §23

---

## Subtask Execution Order

Tasks are ordered by dependency — each task depends only on tasks above it.

| # | Subtask | File(s) | Crate | Dependencies |
|---|---------|---------|-------|-------------|
| 1 | [S3 StorageBackend](./01-s3-storage-backend.md) | `src/s3_backend.rs` | flushdb-server | none |
| 2 | [S3 Integration Tests (Testcontainers/MinIO)](./02-s3-integration-tests.md) | `tests/s3_backend_tests.rs` | flushdb-test | task 1 |
| 3 | [Namespace Configuration](./03-namespace-config.md) | `src/namespace_config.rs` | flushdb-server | none |
| 4 | [Partition Key Router](./04-partition-key-router.md) | `src/partition_router.rs` | flushdb-server | task 3 |
| 5 | [OrderedKey Generator](./05-ordered-key-generator.md) | `src/version_generator.rs` | flushdb-server | none |
| 6 | [Partition Instance & Lifecycle](./06-partition-instance.md) | `src/partition.rs` | flushdb-server | task 3 |
| 7 | [Namespace Manager](./07-namespace-manager.md) | `src/namespace_manager.rs` | flushdb-server | tasks 3, 4, 5, 6 |
| 8 | [Proto-Engine Conversion Layer](./08-proto-engine-conversions.md) | `src/conversions.rs` | flushdb-server | none |
| 9 | [gRPC Write Handlers (PutItems + DeleteItems)](./09-grpc-write-handlers.md) | `src/handlers/write.rs` | flushdb-server | tasks 5, 7, 8 |
| 10 | [gRPC Read Handlers (GetItems + ScanItems)](./10-grpc-read-handlers.md) | `src/handlers/read.rs` | flushdb-server | tasks 7, 8 |
| 11 | [Observability (Metrics + Tracing)](./11-observability.md) | `src/observability.rs` | flushdb-server | none |
| 12 | [Server Bootstrap & Graceful Shutdown](./12-server-bootstrap.md) | `src/server.rs` | flushdb-server | tasks 7, 9, 10, 11 |
| 13 | [End-to-End Integration Tests (Testcontainers/MinIO)](./13-e2e-integration-tests.md) | `tests/e2e_tests.rs` | flushdb-test | tasks 1, 12 |

---

## Acceptance Criteria (Phase-Level)

All of these must pass before Phase 7 is considered complete:

- [ ] `cargo build --workspace` succeeds with zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] gRPC client performs full CRUD cycle: PutItems → GetItems → DeleteItems → GetItems (confirms deletion)
- [ ] Multiple namespaces are isolated — writes to namespace A are invisible from namespace B
- [ ] Each namespace has its own manifest, WAL, and compaction lifecycle
- [ ] Partition key strategies work: Simple distributes uniformly, Composite extracts fields correctly
- [ ] Partitioning distributes data correctly — records route to the expected partition
- [ ] S3 backend works against MinIO via testcontainers
- [ ] S3 conditional_put correctly detects conflicts (returns PreconditionFailed)
- [ ] S3 hash-prefix sharding distributes objects across 128 prefixes
- [ ] S3 parity tests pass — S3 backend behaves identically to LocalFsBackend
- [ ] Idempotency enforcement rejects duplicate tokens within retention window
- [ ] OrderedKey generation produces monotonically increasing versions
- [ ] ScanItems streams batches without requiring page tokens
- [ ] SLO-aware pagination returns partial results under deadline pressure
- [ ] Server starts, accepts connections, and gracefully shuts down
- [ ] Prometheus metrics endpoint exposes write/read latency, cache hit rate, L0 count

---

## New Dependencies (Phase 7)

| Crate | Version | Purpose |
|-------|---------|---------|
| aws-sdk-s3 | 1 | S3 source of truth, conditional PUTs |
| aws-config | 1 | AWS credential/region resolution |
| tracing-subscriber | 0.3 | Log formatting/filtering for server |
| metrics | 0.24 | Observability facade |
| metrics-exporter-prometheus | 0.18 | Prometheus metrics export |
| parking_lot | 0.12 | Fast mutexes (non-hot-path locks) |
| dashmap | 6 | Concurrent hash maps (namespace → partition routing) |
| testcontainers | 0.23 | MinIO containers for S3 integration tests |
| testcontainers-modules | 0.11 | Pre-built MinIO module |
| tonic (already in workspace) | 0.14 | gRPC server |
