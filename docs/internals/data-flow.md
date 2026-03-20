# End-to-End Data Flow

## Write

```
Client PutItems(namespace, record_id, items, token)
  │
  ├─► Route to partition owner
  ├─► Append WAL entry (buffered, fsync via group commit)
  ├─► Insert into memtable (composite key sort order)
  ├─► ACK to client with OrderedKey version
  │
  └─► Background:
        ├─► Freeze memtable when full → flush to SSTable on S3
        ├─► CAS manifest → truncate WAL → release memtable
        └─► Compaction merges L0 → L1 → L2 → L3
```

## Read

```
Client GetItems(namespace, record_id, predicate, selection)
  │
  ├─► Route to partition owner
  ├─► Merge-read across layers:
  │     Active memtable
  │     Frozen memtables
  │     L0 SSTables (bloom check, parallel GETs)
  │     L1-L3 SSTables (bloom check, at most 1 per level)
  │
  ├─► For each layer: seek to (record_id, start_key), scan forward
  ├─► Merge-sort by item_key, apply tombstone filtering
  ├─► Accumulate until byte budget exhausted
  └─► Return items + page_token
```

## Recovery

```
Node startup:
  1. Read latest manifest from S3 → restore SSTable level state
  2. Replay WAL entries with sequence > last_flushed_sequence
  3. Rebuild memtable + dedup set from replayed entries
  4. Resume normal operation
```

Zero data loss for ACK'd writes, assuming the WAL survived the crash.
