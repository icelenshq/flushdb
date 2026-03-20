# Composite Key Encoding

Every data structure in the engine — WAL, memtable, SSTable — operates on a single flat keyspace of composite keys.

```
composite_key = [record_id_bytes] [0x00] [item_key_bytes]
```

The first `0x00` in the key unambiguously marks the boundary between record ID and item key. Record IDs are UTF-8 (no null bytes allowed), so this is safe. Item keys are arbitrary bytes.

**Sort order:** Raw byte comparison gives correct two-level ordering — first by record ID, then by item key within a record. `0x00` sorts before any valid UTF-8 continuation byte, so all items for `"aaa"` sort before all items for `"aab"`.

| Field | Max Length |
|-------|-----------|
| `record_id` | 256 bytes |
| `item_key` | 4,096 bytes |
| `composite_key` | 4,353 bytes |

**Range tombstones** use a synthetic key with a `0xFF` prefix after the separator. This sorts after all valid item keys within the record, keeping tombstone metadata separate from data.
