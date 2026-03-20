# Flush Pipeline

The complete sequence from frozen memtable to durable S3 state.

## Flow

```
  Frozen Memtable                                 S3
  (sorted entries)
       │
       │  iterate in                    ┌───────────────────────┐
       │  sort order                    │                       │
       ▼                                │                       │
  ┌──────────┐                          │                       │
  │ Block    │  4KB blocks              │   sstables/L0/        │
  │ Builder  │──────────────────────────┼──►  {ulid}.sst        │
  │          │  compress, CRC           │                       │
  └──────────┘                          │                       │
       │                                │                       │
       ├── record_ids ──► Bloom Filter ─┤                       │
       ├── first_keys ──► Index Block ──┤                       │
       └── tokens ──────► Dedup Block ──┤                       │
                                        │                       │
                          Footer ───────┤                       │
                                        │                       │
                                        │   manifests/          │
           CAS: v(N+1) ────────────────►│     v42.json (add L0) │
           If-None-Match: *             │                       │
                                        └───────────────────────┘
       │
       │  on CAS success
       ▼
  ┌──────────┐        ┌──────────────┐
  │ Truncate │        │ Release      │
  │ WAL segs │        │ frozen arena │
  │ (delete) │        │ (O(1) drop)  │
  └──────────┘        └──────────────┘
       │
       │  if L0 > 4 files
       ▼
  Schedule compaction
```

## Failure Modes

| Crash Point | S3 State | Recovery |
|-------------|----------|----------|
| During build | Unchanged | WAL replay rebuilds memtable |
| During upload | Incomplete multipart | S3 lifecycle aborts it. WAL replay. |
| During CAS | SSTable on S3, not in manifest | Orphan GC deletes it. WAL replay. |
| After CAS, before WAL cleanup | Manifest updated, WAL intact | WAL replay is idempotent |

**The WAL is the safety net. The manifest CAS is the commit point.**
