---
sidebar_position: 3
title: Storage Engine Internals
---

# Storage Engine Internals

This section walks through every layer of the storage engine, from the moment a write arrives to how it eventually lands on S3 and gets read back. Each page builds on the previous one.

| Layer | What it does |
|-------|-------------|
| [Composite Key](./composite-key) | The encoding that underpins every data structure |
| [WAL](./wal) | Segment-based durability with group commit |
| [Memtable](./memtable) | In-memory skip list with arena allocator |
| [SSTable](./sstable) | Immutable sorted files on S3 |
| [Manifest](./manifest) | Single source of truth for live SSTables, CAS coordination |
| [Flush Pipeline](./flush) | Frozen memtable → SSTable → S3 |
| [Compaction](./compaction) | Leveled merge across L0–L3 |
| [Cache](./cache) | Three-tier W-TinyLFU with continuity tracking |
| [S3 Storage Layer](./s3) | Object layout, leases, large values, GC |
| [Data Flow](./data-flow) | End-to-end write, read, and recovery paths |
