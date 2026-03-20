# Storage Engine Internals

This section walks through every layer of the storage engine, from the moment a write arrives to how it eventually lands on S3 and gets read back. Each page builds on the previous one.

| Layer | What it does |
|-------|-------------|
| [Composite Key](./internals/composite-key.md) | The encoding that underpins every data structure |
| [WAL](./internals/wal.md) | Segment-based durability with group commit |
| [Memtable](./internals/memtable.md) | In-memory skip list with arena allocator |
| [SSTable](./internals/sstable.md) | Immutable sorted files on S3 |
| [Manifest](./internals/manifest.md) | Single source of truth for live SSTables, CAS coordination |
| [Flush Pipeline](./internals/flush.md) | Frozen memtable → SSTable → S3 |
| [Compaction](./internals/compaction.md) | Leveled merge across L0–L3 |
| [Cache](./internals/cache.md) | Three-tier W-TinyLFU with continuity tracking |
| [S3 Storage Layer](./internals/s3.md) | Object layout, leases, large values, GC |
| [Data Flow](./internals/data-flow.md) | End-to-end write, read, and recovery paths |
