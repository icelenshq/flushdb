# flushdb — Product Requirements Document

## Overview

flushdb is a distributed key-value database that uses S3 as its durable source of truth while maintaining fast local writes through an LSM-tree architecture. It draws from Cassandra's storage model and Netflix's KV Data Abstraction Layer patterns.

For complete system design and implementation details, see **STORAGE_DESIGN.md**.

---

## Problem Statement

Traditional distributed KV stores couple storage durability with the database cluster itself — replication, disk management, and failure recovery are all internal concerns. This creates operational overhead: disk provisioning, replica repair, anti-entropy, and complex multi-node consistency protocols.

S3 offers 11 nines of durability, strong read-after-write consistency, and effectively unlimited capacity — but it has high latency (~50ms per request) and no support for point writes. flushdb bridges this gap: fast local writes with WAL-backed durability, background flushes to S3 for permanent storage, and a thin coordination layer that only protects the unflushed window.

---

## Target Users

- Teams that need a KV store with S3-tier durability without managing disk replication
- Applications with write-heavy workloads that can tolerate eventual flush to object storage
- Multi-tenant platforms that need namespace-level isolation with per-tenant partitioning strategies
- Systems that need CDC event streams from their KV layer without running a separate change tracking system

---

## Data Model

Two-level sorted map: records identified by string IDs, each containing sorted key-value items with optional metadata. Composite storage key `record_id + 0x00 + item_key` enables co-located range scans within a record.

Supported patterns: simple KV, named sets, sorted events, versioned records, adjacency lists, counters, prefix trees — all reduce to the same two-level map primitive.

See STORAGE_DESIGN.md §2–3 for full data model and composite key encoding.

---

## API Surface

Four gRPC operations, all scoped to a namespace and record ID:

| Operation | Description |
|-----------|-------------|
| **PutItems** | Upsert items into a record (idempotent via token) |
| **GetItems** | Read items by predicate (match_keys, match_range, match_all) with byte-based pagination |
| **DeleteItems** | Delete items by predicate (tombstone-based) |
| **ScanItems** | Server-side streaming variant of GetItems |

Three query predicates on item keys: **match_keys** (multi-get), **match_range** (contiguous scan), **match_all** (full record).

See STORAGE_DESIGN.md §19 for full message schemas and semantics.

---

## Core Architecture

- **S3 as source of truth** — all durable state in S3, nodes are stateless from a durability perspective
- **LSM-tree write path** — WAL → memtable → SSTable flush to S3
- **Three-tier cache** — DRAM (<1ms) → NVMe (<5ms) → S3 (50-200ms), with W-TinyLFU admission policy
- **Leveled compaction** — L0 (overlapping) → L1 → L2 → L3 (non-overlapping), fragment-based
- **Manifest-based coordination** — versioned manifests with CAS via S3 conditional writes
- **Epoch-based fencing** — prevents zombie writers after ownership changes
- **Multi-tenancy** — namespace-level isolation with per-tenant partitioning, compaction, and S3 paths

See STORAGE_DESIGN.md for complete architecture details.

---

## Non-Goals

- No SQL, joins, or multi-record transactions
- No exactly-once CDC delivery — sinks handle their own semantics
- No schema migration for partition keys — immutable after namespace creation
- No management of external sink infrastructure (Kafka clusters, webhook endpoints, etc.)
