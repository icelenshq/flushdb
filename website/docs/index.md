---
slug: /
sidebar_position: 1
title: Introduction
---

# flushdb

A distributed key-value database written in Rust that uses **S3 as its durable source of truth** with an LSM-tree storage engine.

## What is it?

flushdb is designed for workloads where records contain many items (e.g., a product with variants, pricing, inventory) and need to be read, written, and scanned efficiently. It combines the write throughput of an LSM-tree with the durability and economics of object storage.

**Key properties:**

- **S3-native durability** — All persistent state lives in S3. Compute nodes are stateless and can be replaced without data loss.
- **LSM-tree write path** — Writes go to a WAL, then an in-memory memtable, then flush to SSTables on S3. This gives high write throughput with sequential I/O.
- **Three-tier caching** — Hot data is served from DRAM (&lt;1ms), warm data from NVMe (&lt;5ms), and cold data from S3 (50-200ms).
- **Multi-tenant** — Namespaces isolate tenants with independent partitioning, compaction, and resource limits.
- **gRPC API** — Simple four-operation API: `PutItems`, `GetItems`, `DeleteItems`, `ScanItems`.

## Data Model

flushdb uses a **two-level sorted map**:

```
Namespace → Record (by ID) → Items (sorted by key)
```

A **record** is identified by a string ID (e.g., `product-42`). Each record contains a sorted set of **items**, where each item has a binary key, value, and optional metadata.

This is similar to a Cassandra partition: the record ID is the partition key, and item keys are clustering columns. The difference is that flushdb stores everything on S3 and uses an LSM-tree engine locally.

## Project Structure

flushdb is a Cargo workspace with 7 crates:

| Crate | Purpose |
|-------|---------|
| `flushdb-proto` | gRPC protocol definitions (protobuf + tonic) |
| `flushdb-types` | Core types, traits, and error handling |
| `flushdb-wal` | Write-Ahead Log with group commit |
| `flushdb-engine` | Storage engine: memtable, SSTable, manifest, flush, compaction, cache |
| `flushdb-server` | gRPC server, namespace management, S3 backend, partitioning |
| `flushdb-test` | Integration tests with testcontainers (MinIO) |
| `flushdb-demo` | CLI tool for seeding, verification, and benchmarking |

### Dependency Graph

```
flushdb-proto       → (standalone, generated code)
flushdb-types       → bytes, thiserror, tokio
flushdb-wal         → flushdb-types
flushdb-engine      → flushdb-types, flushdb-wal
flushdb-server      → flushdb-engine, flushdb-proto
flushdb-test        → all crates
flushdb-demo        → flushdb-proto
```
