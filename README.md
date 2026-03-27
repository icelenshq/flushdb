# flushdb

A distributed key-value database written in Rust. S3 is the source of truth — nodes are stateless, durability is delegated to object storage, and the coordination layer only exists to protect writes that haven't flushed yet.

---

## Why this exists

Running Cassandra or a similar distributed KV store means you're also running a replication protocol, managing disk space across nodes, dealing with anti-entropy repairs, and handling node failures with complex consistency windows. That's a lot of infrastructure just to not lose data.

S3 already has 11 nines of durability, strong read-after-write consistency, and essentially unlimited capacity. The problem is you can't write individual keys to it — you have to batch writes into files. flushdb does exactly that: local writes land in a WAL for immediate durability, build up in a memtable, then get flushed to S3 as SSTables. Nodes become stateless from a durability standpoint. Lose a node, spin up a new one, it reads the manifest from S3 and picks up where things left off.

The tradeoff: flushdb is designed for workloads where writes are frequent, reads can tolerate a multi-tier cache miss path (DRAM → NVMe → S3), and you'd rather pay for S3 storage than operate disk replication.

---

## Data model

The primitive is a two-level sorted map:

```
record_id  →  SortedMap<item_key, item_value>
```

A record is identified by a string ID (also the partition key input). Items inside the record are sorted byte-order on their keys. This maps cleanly to simple KV, named fields, time-series (timestamp as item key), sorted sets, versioned documents — all reduce to the same primitive.

Range queries within a record are first-class — `match_range(start, end)` is a contiguous scan within a single partition, no scatter-gather. Deleting a range writes one tombstone, not N individual deletions.

---

## API

Four gRPC operations, all scoped to a namespace and record ID:

| Operation | What it does |
|---|---|
| `PutItems` | Upsert items into a record. Idempotent with an optional token. |
| `GetItems` | Read items by predicate with byte-based pagination. |
| `DeleteItems` | Delete items by predicate (tombstone-based). |
| `ScanItems` | Server-side streaming variant of `GetItems`. |

Read predicates: `match_keys` (multi-get by name), `match_range` (contiguous range), `match_all` (full record).

---

## Getting started

Requires Docker and Docker Compose.

```bash
# Start MinIO (local S3) and the server
docker compose up -d minio minio-init flushdb-server

# Seed some data
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo seed --products 1000 --concurrency 4

# Verify correctness
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo verify --sample-size 50 --product-range 1000

# Tear down
docker compose down -v
```

## Benchmarking

```bash
# Read, scan, or mixed workload
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo bench read --duration 30 --concurrency 4 --product-range 1000

# Full sequential Docker-only benchmark with incremental scale
./scripts/benchmark.sh --duration 30 --warmup 5
```

The published comparison is collected by [`scripts/benchmark.sh`](scripts/benchmark.sh), which runs:

- `flushdb-server` in Docker with MinIO
- `cassandra` in Docker with matching 2 CPU / 1 GiB limits
- `flushdb-demo` in Docker as the benchmark client

Each scale step is measured sequentially: smoke checks first, then flushdb alone, then Cassandra alone, with teardown between phases to avoid resource starvation and benchmark noise pollution.

See [`crates/flushdb-demo/README.md`](crates/flushdb-demo/README.md) for the full list of bench subcommands and flags.

---

## Configuration

All config via environment variables:

| Variable | Default | Description |
|---|---|---|
| `FLUSHDB_GRPC_LISTEN_ADDR` | `0.0.0.0:50051` | gRPC listen address |
| `FLUSHDB_METRICS_LISTEN_ADDR` | `0.0.0.0:9090` | Prometheus metrics endpoint |
| `FLUSHDB_NODE_ID` | `0` | Unique node ID |
| `FLUSHDB_DATA_DIR` | `./data` | Local dir for WAL segments |
| `FLUSHDB_S3_BUCKET` | — | S3 bucket (required) |
| `FLUSHDB_DEFAULT_NAMESPACES` | — | `name:partition_count` pairs, comma-separated |
| `FLUSHDB_DEFAULT_MEMTABLE_SIZE_MB` | `64` | Flush threshold |
| `FLUSHDB_LOG_LEVEL` | `info` | `trace` / `debug` / `info` / `warn` / `error` |
| `FLUSHDB_LOG_FORMAT` | `json` | `json` or `text` |

AWS credentials use the standard SDK chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `AWS_ENDPOINT_URL`). Set `AWS_ENDPOINT_URL` to point at MinIO or any S3-compatible store.

Partition count is immutable after namespace creation.

---

## Architecture

**Write path:** WAL → memtable (skip list) → SSTable flush to S3. WAL uses group commit to batch concurrent writes into a single fsync. Memtables freeze when they hit the size threshold and flush in the background.

**Read path:** Memtable (current + frozen) → SSTable levels. Each SSTable has a Bloom filter on record IDs and a sparse index. Three-tier block cache (DRAM → NVMe → S3) sits in front.

**Levels:** L0 allows overlapping key ranges, L1+ are non-overlapping. Compaction merges files, deduplicates updates, and drops expired tombstones. A versioned manifest in S3 (updated via conditional PUT) tracks live SSTables and prevents split-brain.

**Partitioning:** Fixed partitions per namespace, assigned by consistent hash of record ID. Each partition owns a dedicated engine instance — no shared mutable state, no `Arc<Mutex<>>` in the hot path.

**Observability:** Prometheus metrics at `:9090/metrics`, structured JSON logs to stdout.

---

## Crate structure

```
flushdb-proto     protobuf definitions + tonic-generated stubs
flushdb-types     core types, traits, errors
flushdb-wal       WAL writer/reader, segment management, group commit
flushdb-engine    SSTable, bloom filters, memtable, manifest, flush, compaction, cache
flushdb-server    gRPC server, namespaces, partitioning, S3 backend, config
flushdb-test      integration tests (real MinIO via testcontainers)
flushdb-demo      seed / verify / benchmark CLI
```

---

## Development

```bash
cargo build --workspace
cargo test --workspace   # integration tests require Docker
cargo clippy --workspace
```
