# flushdb

**flushdb** is a distributed key-value database that uses S3 as its durable source of truth while maintaining fast local writes through an LSM-tree architecture.

## Key Design Decisions

- **S3 as source of truth** — SSTables are stored in S3, enabling zero-coordination reads and cheap storage
- **LSM-tree write path** — WAL + memtable + flush pipeline for fast local writes
- **Single-owner, shard-per-core** — no `Arc<Mutex<>>`, each partition runs on a dedicated core
- **gRPC API** — PutItems, GetItems, DeleteItems, ScanItems with namespace-based multi-tenancy

## Documentation

- [Product Requirements](PRD.md) — high-level problem statement, data model, API surface, non-goals
- [System Design](STORAGE_DESIGN.md) — authoritative reference for data model, storage engine, cluster coordination, and API
- [Implementation Plan](plan.md) — phased build plan with acceptance criteria
- [Testing Standards](TESTING.md) — test patterns, S3 test categories, container cleanup
