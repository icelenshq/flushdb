# Data Model

flushdb stores data as a two-level sorted map. This page describes the data model, how keys are encoded, and the access patterns it enables.

---

## Two-Level Map

The fundamental structure is a map of **Records**, where each Record is identified by a string ID and contains a **sorted map of Items**. Items are key-value pairs of raw bytes, sorted by key in ascending byte order within the record.

```
Record ID --> SortedMap
                |-- item_key_0 --> item_value_0
                |-- item_key_1 --> item_value_1
                |-- item_key_2 --> item_value_2
                `-- ...
```

Each Item carries four fields:

| Field | Type | Description |
|-------|------|-------------|
| key | bytes | Sort key within the record |
| value | bytes | Payload |
| metadata | bytes (optional) | Auxiliary data such as content type, schema version, or application-defined headers |
| chunk index | integer | Index for chunked large values; 0 for non-chunked items |

Record IDs are UTF-8 strings. Item keys and values are opaque byte sequences -- flushdb does not interpret their contents. The sort order within a record is raw lexicographic byte comparison on the item key.

---

## Composite Key Encoding

Internally, every record ID and item key pair is combined into a single **composite key**:

```
composite_key = [record_id bytes] [0x00] [item_key bytes]
```

The separator is a single null byte (`0x00`). This encoding is unambiguous because record IDs are valid UTF-8 strings, and flushdb disallows the null character in record IDs. Since null bytes cannot appear in the record ID portion, the first `0x00` in the composite key always marks the boundary between record ID and item key.

Item keys are arbitrary bytes, so `0x00` can appear freely within them -- but only after the separator.

An empty item key (used by the simple KV pattern) produces `[record_id] [0x00]` -- the composite key ends with the separator byte.

### Sort Order Guarantee

Composite keys sort correctly under plain byte comparison (`memcmp`):

1. Keys are ordered first by **record ID** (lexicographic on UTF-8 bytes).
2. Then by **item key** (lexicographic on raw bytes).

This works because `0x00` sorts before any valid UTF-8 continuation byte. All items belonging to record `"aaa"` sort before all items belonging to record `"aab"`, regardless of item key content.

This sort order enables three access patterns directly on the flat keyspace:

- **Point lookup**: binary search for an exact `(record_id, item_key)` pair.
- **Range scan within a record**: seek to `(record_id, start_key)`, scan forward until the record ID changes or the end key is reached.
- **Full record read**: seek to `(record_id, <minimum key>)`, scan forward until the record ID changes.

---

## Supported Data Patterns

The two-level sorted map unifies seven common data patterns into a single primitive. No secondary indexes, no schema changes, no different storage backends -- each pattern is just a convention for how record IDs, item keys, and item values are used.

| Pattern | Record ID | Item Key | Item Value | Example |
|---------|-----------|----------|------------|---------|
| **Simple KV** | entity ID | empty bytes `""` | payload | `user:123 -> {"" -> profile_json}` |
| **Named Set** | set name | member | empty bytes `""` | `followers:alice -> {bob -> "", carol -> ""}` |
| **Sorted Events** | entity ID | timestamp (8-byte big-endian) | event data | `activity:u1 -> {ts1 -> e1, ts2 -> e2}` |
| **Versioned Record** | entity ID | version key (ordered) | snapshot | `doc:42 -> {v001 -> s1, v002 -> s2}` |
| **Adjacency List** | node ID | neighbor ID | edge metadata | `graph:nodeA -> {nodeB -> weight, nodeC -> weight}` |
| **Counter / Aggregation** | entity ID | dimension key | counter bytes | `metrics:api -> {2024-09-18 -> count_bytes}` |
| **Prefix Tree** | root path | sub-path segments | leaf data | `config:/app -> {/db/host -> val, /db/port -> val}` |

The key insight is that sorted item keys let you encode ordering, membership, adjacency, and hierarchy without dedicated data structures for each. A timestamp becomes a sort key. A neighbor ID becomes a set member. A version string becomes a scannable sequence.

---

## Why Sorted Values Matter

The choice to keep items sorted within each record is not incidental. It drives several properties of the system:

**Range queries are first-class.** A range scan within a record resolves to a contiguous read over co-located data. There is no scatter-gather across partitions, no secondary index lookup, and no post-hoc sorting. Scanning items between two keys is a single seek-and-scan operation.

**Single tombstone for range deletes.** Deleting all items in a key range writes one range tombstone marker rather than N individual tombstones. This avoids the tombstone accumulation problem that plagues systems where range deletes decompose into per-key operations.

**Predictable read amplification.** Items within a record are co-located in the storage files. Reading a record touches a bounded number of files, and bloom filters on record IDs eliminate files that do not contain the target record before any data is read.

**Natural merge during compaction.** Items for the same record from different storage files merge together during background compaction, deduplicating updates and dropping expired tombstones in a single pass.

**Efficient byte-based pagination.** Sorted order means the page token is just the last item key returned. Resuming a paginated read is a seek to `(record_id, last_key + 1)` -- no offset tracking, no cursor state on the server.

---

## Key Length Limits

| Field | Maximum Length | Notes |
|-------|--------------|-------|
| `record_id` | 256 bytes | Used as the partition key input; larger IDs waste space in bloom filters |
| `item_key` | 4,096 bytes | Generous for timestamps, UUIDs, and hierarchical paths |
| `composite_key` | 4,353 bytes | 256 (record ID) + 1 (separator) + 4,096 (item key) |

These limits are enforced at the API layer. Writes exceeding any limit are rejected.

---

## Deletes

Deletes in flushdb are writes. When you delete an item, the system writes a **tombstone** -- a marker that records "this key was deleted at this point in time." The actual data is removed later during background compaction.

There are two granularities of delete:

**Point tombstones** delete individual items. A point tombstone for `(record_id, item_key)` shadows any prior value for that key. Reads encountering a point tombstone return "not found."

**Range tombstones** delete a contiguous range of item keys within a record. A single range tombstone covers `[start_key, end_key)` and shadows all items in that range with earlier timestamps. This is particularly efficient for patterns like "delete all events before timestamp X" or "remove all versions before v005."

Tombstones have a configurable TTL (time-to-live). During background compaction, tombstones that have exceeded their TTL and have no remaining data to shadow in deeper storage levels are garbage collected. Until a tombstone expires, it must be preserved to prevent deleted data from reappearing when storage files are merged.
