# flushdb-demo

CLI tool for seeding, benchmarking, and verifying a running flushdb instance.

## Prerequisites

- Docker & Docker Compose
- Rust toolchain (only if building locally)

## Quick start (Docker Compose)

### 1. Start the server stack

```bash
docker compose up -d minio minio-init flushdb-server
```

Wait until healthy:

```bash
docker compose ps
```

`minio` should show healthy and `flushdb-server` running on port 50051.

### 2. Seed data

```bash
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo seed --products 1000 --concurrency 4
```

### 3. Verify correctness

```bash
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo verify --sample-size 50 --product-range 1000
```

### 4. Run benchmarks

```bash
# Read benchmark
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo bench read --duration 30 --concurrency 4 --product-range 1000

# Scan benchmark
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo bench scan --duration 30 --concurrency 4 --product-range 1000

# Mixed workload (all operation types)
docker compose run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  flushdb-demo bench mixed --duration 30 --concurrency 4 --product-range 1000 \
    --write-ratio 0.15 --update-ratio 0.15 --delete-ratio 0.05 --scan-ratio 0.05
```

### 5. Compare with Cassandra

Start Cassandra alongside the flushdb stack using the `benchmark` profile:

```bash
docker compose --profile benchmark up -d
# Wait for Cassandra to be healthy (~30-60s)
docker compose --profile benchmark run --rm \
  -e FLUSHDB_SERVER_ADDR=http://flushdb-server:50051 \
  -e CASSANDRA_ADDR=cassandra:9042 \
  flushdb-demo bench compare --duration 30 --concurrency 4 --product-range 1000 \
    --seed-products 1000 --warmup-secs 10
```

The benchmark protocol ensures a fair comparison:

1. **Equal resources**: Both database processes get 2 CPUs + 1 GB memory (set in `docker-compose.yml`). MinIO (flushdb's S3 backend) runs separately with 1 CPU + 512 MB.
2. **Warmup phase**: Runs the workload for `--warmup-secs` (default 10s) against each backend before measuring. This eliminates JVM cold-start bias for Cassandra and warms connection pools for both.
3. **Identical workload**: Same RNG seed, same product range, same operation mix for both backends.
4. **Sequential measurement**: Each backend is benchmarked independently (no resource contention between them during measurement).

### 6. Tear down

```bash
docker compose --profile benchmark down -v
```

## Running locally

Start MinIO and the server via Docker, then build and run the demo binary directly:

```bash
docker compose up -d minio minio-init flushdb-server
cargo build --release -p flushdb-demo

./target/release/flushdb-demo seed --products 1000 --concurrency 4
./target/release/flushdb-demo verify --sample-size 50 --product-range 1000
./target/release/flushdb-demo bench read --duration 30 --concurrency 4 --product-range 1000
```

Defaults: `--server-addr http://127.0.0.1:50051`, `--namespace ecommerce`.

## Commands

### `seed`

Populates the database with deterministic product data.

| Flag | Default | Description |
|------|---------|-------------|
| `--products` | 1000000 | Number of products to write |
| `--concurrency` | 8 | Parallel workers |

### `verify`

Runs correctness checks against seeded data and performs round-trip tests.

| Flag | Default | Description |
|------|---------|-------------|
| `--sample-size` | 100 | Products to sample for seeded-data checks |
| `--product-range` | 1000000 | Upper bound of product ID range |

Exits with code 1 if any check fails.

**Phase 1 — Seeded data checks** (samples random products written by `seed`):

| Check | What it validates |
|-------|-------------------|
| presence | info, price, inventory:\*, variant:\* keys exist |
| item-count | stored count matches deterministic generator output |
| value-integrity | byte-for-byte comparison against regenerated expected values |
| json-schema | required fields, types, and business rules in JSON values |

**Phase 2 — Round-trip checks** (writes its own isolated data):

| Check | What it validates |
|-------|-------------------|
| put-get-roundtrip | write then read returns exact data |
| delete-correctness | delete removes all items |
| scan-correctness | variant:\* range scan returns all variants with correct values |
| selective-get | MatchKeys returns only the requested keys |
| overwrite | second put to same record overwrites first put's values |

### `bench <workload>`

Runs performance benchmarks. Available workloads:

| Workload | Description |
|----------|-------------|
| `write` | Continuous writes of random products |
| `read` | Random reads of info + price keys |
| `scan` | Range scans over variant:\* keys |
| `mixed` | Configurable mix of reads, writes, updates, deletes, and scans |
| `ingest` | Bulk-load products into both backends, compare throughput at milestones |
| `compare` | Run mixed workload against both flushdb and Cassandra, print comparison |

Common flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--duration` | 60 | Seconds to run |
| `--concurrency` | 8 | Parallel workers |
| `--product-range` | 1000000 | Product ID range for random selection |

Mixed-only flags:

| Flag | Default | Description |
|------|---------|-------------|
| `--write-ratio` | 0.15 | Fraction of ops that are full product writes |
| `--update-ratio` | 0.15 | Fraction of ops that are partial updates (price + inventory) |
| `--delete-ratio` | 0.05 | Fraction of ops that are full record deletes |
| `--scan-ratio` | 0.05 | Fraction of ops that are variant range scans |

Remaining fraction (default 60%) is reads. Ratios must sum to <= 1.0.

Ingest-only flags:

| Flag | Default | Env var | Description |
|------|---------|---------|-------------|
| `--total-products` | 15,000,000 | | Products to ingest per backend |
| `--report-interval` | 500,000 | | Print progress every N products |
| `--cassandra-addr` | `127.0.0.1:9042` | `CASSANDRA_ADDR` | Cassandra contact point |

Compare-only flags:

| Flag | Default | Env var | Description |
|------|---------|---------|-------------|
| `--cassandra-addr` | `127.0.0.1:9042` | `CASSANDRA_ADDR` | Cassandra contact point |
| `--seed-products` | 1000 | | Products to seed before benchmark |
| `--warmup-secs` | 10 | | Warmup duration per backend (results discarded) |

## Global flags

| Flag | Default | Env var | Description |
|------|---------|---------|-------------|
| `--server-addr` | `http://127.0.0.1:50051` | `FLUSHDB_SERVER_ADDR` | gRPC server address |
| `--namespace` | `ecommerce` | `FLUSHDB_NAMESPACE` | Target namespace |
