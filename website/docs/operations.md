---
sidebar_position: 5
title: Operations
---

# Operations

## Quick Start

```bash
docker compose --profile flushdb up -d
```

This starts MinIO (S3-compatible storage) on port 9000 and flushdb-server on port 50051 (gRPC) and 9090 (Prometheus metrics). The `minio-init` service creates the required bucket automatically.

### Default Configuration

| Setting | Value |
|---------|-------|
| gRPC address | `0.0.0.0:50051` |
| S3 bucket | `flushdb` |
| Node ID | `1` |
| Default namespace | `ecommerce` (8 partitions) |
| Memtable size | 32 MB |
| WAL fsync mode | `batch_sync` |
| WAL batch sync interval | `10ms` |

### Resource Limits

| Service | CPU | Memory |
|---------|-----|--------|
| MinIO | 1 | 512 MB |
| flushdb-server | 2 | 1 GB |

## Building from Source

```bash
# Prerequisites: Rust 1.93+, protoc
brew install protobuf          # macOS
# apt-get install protobuf-compiler  # Debian/Ubuntu

cargo build --workspace                                   # dev build
cargo build --release -p flushdb-server -p flushdb-demo   # release binaries
```

### Running Locally

Start MinIO, then the server:

```bash
docker compose up -d minio minio-init

FLUSHDB_GRPC_LISTEN_ADDR=0.0.0.0:50051 \
FLUSHDB_S3_BUCKET=flushdb \
FLUSHDB_NODE_ID=1 \
FLUSHDB_DATA_DIR=/tmp/flushdb-data \
FLUSHDB_DEFAULT_NAMESPACES=ecommerce:8 \
FLUSHDB_DEFAULT_MEMTABLE_SIZE_MB=32 \
AWS_ENDPOINT_URL=http://localhost:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
AWS_REGION=us-east-1 \
cargo run --release -p flushdb-server
```

### Docker Build

Multi-stage Dockerfile produces minimal images:

```bash
docker build --target server -t flushdb-server .
docker build --target demo -t flushdb-demo .
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `FLUSHDB_GRPC_LISTEN_ADDR` | gRPC bind address | `0.0.0.0:50051` |
| `FLUSHDB_S3_BUCKET` | S3 bucket name | `flushdb` |
| `FLUSHDB_NODE_ID` | Unique node identifier | `1` |
| `FLUSHDB_DATA_DIR` | Local data directory (WAL, cache) | `/data` |
| `FLUSHDB_DEFAULT_NAMESPACES` | `name:partitions` pairs, comma-separated | — |
| `FLUSHDB_DEFAULT_MEMTABLE_SIZE_MB` | Memtable size before flush | `32` |
| `FLUSHDB_DEFAULT_WAL_FSYNC_MODE` | WAL fsync mode (`sync` or `batch_sync`) | `sync` |
| `FLUSHDB_DEFAULT_WAL_GROUP_COMMIT_INTERVAL_US` | Group commit interval in microseconds | `200` |
| `FLUSHDB_DEFAULT_WAL_BATCH_SYNC_INTERVAL_MS` | Batch sync fsync interval in milliseconds. Must be `> 0`. | `10` |
| `AWS_ENDPOINT_URL` | S3 endpoint (for MinIO) | — |
| `AWS_ACCESS_KEY_ID` | S3 access key | — |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key | — |
| `AWS_REGION` | S3 region | `us-east-1` |

`FLUSHDB_DEFAULT_WAL_BATCH_SYNC_INTERVAL_MS` and the per-namespace `wal_batch_sync_interval_ms` setting must be positive. A value of `0` is rejected during config validation.

---

## Testing

### Run All Tests

```bash
cargo test --workspace
```

### By Crate

```bash
cargo test -p flushdb-types     # core types, composite key, encoding
cargo test -p flushdb-wal       # segments, group commit, recovery
cargo test -p flushdb-engine    # memtable, SSTable, flush, compaction, cache
cargo test -p flushdb-server    # namespace, partition, gRPC handlers, S3 backend
```

### S3 Integration Tests

Uses [testcontainers](https://github.com/testcontainers/testcontainers-rs) with MinIO. Docker must be running.

```bash
cargo test -p flushdb-test
```

The harness automatically starts a MinIO container, creates the bucket, isolates tests with unique key prefixes, and cleans up on exit.

### End-to-End Tests

Spins up an in-process gRPC server with a local filesystem backend:

```bash
cargo test -p flushdb-test -- e2e
```

### Linting

```bash
cargo clippy --workspace
```

### Test Organization

Tests live in separate `tests/` directories per crate (not inline `#[cfg(test)]` blocks). ~71 test files across all crates covering unit, integration, and end-to-end scenarios.

---

## Benchmarks

The `flushdb-demo` crate provides seeding, verification, and benchmarking with an e-commerce dataset — see [Data Model Patterns](./data-model#example-e-commerce-catalog) for the full schema. Data is deterministic (seed 42) for reproducibility.

### Seed Data

```bash
# Docker
docker compose run --rm flushdb-demo seed --products 10000 --concurrency 4

# Cargo
cargo run --release -p flushdb-demo -- seed --products 10000 --concurrency 4
```

### Verify Correctness

Two-phase check: (1) seeded data integrity, (2) put-get-delete-scan round-trips.

```bash
docker compose run --rm flushdb-demo verify --sample-size 50 --product-range 10000
```

### Workloads

```bash
docker compose run --rm flushdb-demo bench <workload> --duration 60 --concurrency 8
```

| Workload | Description |
|----------|-------------|
| `write` | Random product writes at max throughput |
| `read` | Random point reads (info + price keys) |
| `scan` | Range scans over variant keys |
| `mixed` | Configurable mix of read/write/update/delete/scan |
| `ingest` | Bulk-load comparison: flushdb vs Cassandra |
| `compare` | Full mixed-workload comparison: flushdb vs Cassandra |

### Cassandra Comparison

Use the sequential Docker-only benchmark runner:

```bash
./scripts/benchmark.sh --duration 30 --warmup 5
```

The script runs a 10-step ladder (`1,000` to `100,000` seeded products) measuring both backends sequentially. Each step: seeds data, runs the `compare` workload against flushdb (saving results), tears down, then runs against Cassandra (loading the saved results for comparison). Artifacts (JSON snapshots, logs, CSV manifest) are saved to `--output-dir`.

The `compare` workload now operates in two phases via `--phase`:
- `--phase flushdb` — benchmarks flushdb only, writes results to `--output`
- `--phase cassandra` — benchmarks Cassandra only, loads flushdb results from `--flushdb-results` for comparison

**Fair testing protocol:**
- Equal resources (2 CPUs, 1 GB per database)
- MinIO isolated to the flushdb phase (1 CPU, 512 MB)
- Docker-only benchmark client (`flushdb-demo`)
- Warmup phase before each measured backend phase
- Identical workload for each scale step
- Sequential measurement with teardown between phases

### Metrics

Benchmarks report throughput (ops/s), latency distribution (p50, p99 via HDR histograms), and speedup ratios for comparison workloads.

### Demo Tool Flags

| Flag | Env Var | Default |
|------|---------|---------|
| `--server-addr` | `FLUSHDB_SERVER_ADDR` | `http://127.0.0.1:50051` |
| `--namespace` | `FLUSHDB_NAMESPACE` | `ecommerce` |
