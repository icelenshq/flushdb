# Phase 1: Foundation — Types, Traits, API Contract

**Complexity: M**
**Crates:** `flushdb-types`, `flushdb-proto`
**Design references:** STORAGE_DESIGN.md §2, §3, §4, §19

---

## Goal

Establish every core type, trait, and protobuf definition the rest of the system builds on. After this phase, `cargo build --workspace` succeeds, all interfaces are locked, and every subsequent phase implements against these contracts.

---

## 1. Data Model Types

### 1.1 CompositeKey

The entire storage engine operates on a single flat keyspace of composite keys. Every data structure (WAL, memtable, SSTable) depends on this encoding.

**Binary format:**
```
composite_key = [record_id_bytes] [0x00] [item_key_bytes]
```

- The separator `0x00` works because record IDs are UTF-8 strings (no embedded nulls), so the first `0x00` unambiguously marks the boundary.
- Item keys are arbitrary bytes — `0x00` can appear in them, but only after the separator.
- An empty item key (simple KV pattern) produces `[record_id] [0x00]`.

**Sort order guarantee:** Raw byte comparison (`memcmp`) produces correct ordering — first by `record_id` (lexicographic UTF-8), then by `item_key` (lexicographic raw bytes). `0x00` sorts before any valid UTF-8 continuation byte.

This enables:
- **Point lookup:** binary search for exact `(record_id, item_key)`
- **Range scan within a record:** seek to `(record_id, start_key)`, scan until `record_id` changes or `end_key` reached
- **Full record read:** seek to `(record_id, MIN_KEY)`, scan until `record_id` changes

**Key length limits:**

| Field | Max Length | Rationale |
|-------|-----------|-----------|
| `record_id` | 256 bytes | Partition key hashing input; larger IDs waste bloom filter bits |
| `item_key` | 4096 bytes | Generous for timestamps, UUIDs, paths; fits in one SSTable data block |
| `composite_key` | 4353 bytes | 256 + 1 + 4096 |

**Range tombstone composite key:** Uses a sentinel prefix `0xFF` after the separator to sort tombstone metadata after all valid item keys within a record:
```
range_tombstone_key = [record_id] [0x00] [0xFF] [start_key]
range_tombstone_val = [end_key] [inclusive_flags]
```

### 1.2 Item

The fundamental unit of data in the two-level map:
```
Item {
  key:      Bytes        // sort key within the record
  value:    Bytes        // payload
  metadata: Bytes        // optional (content type, schema version, etc.)
  chunk:    Integer      // chunk index for large values (0 for non-chunked)
}
```

### 1.3 MemtableEntry

Internal representation for entries in the memtable and WAL:
```
MemtableEntry {
  composite_key:    Bytes           // record_id + 0x00 + item_key
  value:            Bytes           // item value (or tombstone marker)
  metadata:         Bytes           // item metadata
  idempotency_key:  IdempotencyToken
  sequence_number:  u64             // WAL sequence number for ordering
  entry_type:       EntryType       // PUT | DELETE | RANGE_DELETE
}
```

### 1.4 EntryType

Three operation types flow through WAL, memtable, and SSTable:
- **PUT (0):** Standard put — `item_key` is the key, `item_value` is the value
- **DELETE (1):** Point tombstone — `item_key` is deleted, `item_value` empty
- **RANGE_DELETE (2):** Range tombstone — `item_key` is start (inclusive), `item_value` encodes end (exclusive), scoped to `record_id`

### 1.5 EntryValue

The SSTable entry format must reserve the BlobRef variant from day one to avoid format migration later:
```
EntryValue {
  Inline(Bytes)                                    // value stored directly
  BlobRef { blob_id: ULID, offset: u64, size: u32 } // pointer to separated blob
}
```

Phase 1 only implements `Inline`. `BlobRef` is defined but unused until future work (value separation for values >= 32KB).

### 1.6 IdempotencyToken

24-byte token for at-least-once delivery deduplication:
```
IdempotencyToken {
  generation_time: u64    // 8 bytes — client monotonic timestamp (ms)
  token:           [u8; 16] // 16 bytes — UUID v7 nonce
}
```

An all-zero token (24 zero bytes) means "no idempotency" — the entry is always applied. Tokens with clock drift exceeding a configurable threshold (default 5 seconds) are rejected.

### 1.7 OrderedKey

12-byte system-generated version key, monotonically increasing, naturally sorted by time:
```
OrderedKey {
  timestamp_ms: u64   // 8 bytes — big-endian millisecond timestamp
  node_id:      u16   // 2 bytes — big-endian node identifier
  sequence:     u16   // 2 bytes — big-endian per-node sequence counter
}
```

Supports ~65K keys per millisecond per node with up to 65,536 nodes. Returned to clients as the version in write responses.

---

## 2. Error Types

### FlushError

Unified error enum covering all failure modes across the system. Define it now because every crate will depend on it:

- **IO** — filesystem or network I/O failure
- **KeyTooLong** — record_id > 256 bytes or item_key > 4096 bytes
- **InvalidKey** — record_id contains null byte or is empty
- **NotFound** — requested record or item does not exist
- **PreconditionFailed** — CAS conflict (manifest update, lease acquisition)
- **CrcMismatch** — data integrity failure (WAL entry, SSTable block)
- **CorruptedData** — structural corruption (bad magic, invalid format)
- **DuplicateToken** — idempotency token already applied
- **ResourceExhausted** — backpressure (WAL size, L0 stall)
- **EpochFenced** — zombie writer/compactor detected
- **InvalidArgument** — malformed request parameters

---

## 3. StorageBackend Trait

The central abstraction for persistent storage. Everything builds against this trait — S3 is just a production implementation.

**Methods:**

| Method | Semantics |
|--------|-----------|
| `put(key, value)` | Store bytes at key, overwrite if exists |
| `get(key) -> Option<Bytes>` | Retrieve bytes by exact key |
| `get_range(key, offset, length) -> Bytes` | Byte-range read (for SSTable block fetches) |
| `delete(key)` | Remove object at key |
| `conditional_put(key, value, condition)` | CAS write — `If-None-Match: *` semantics for manifest updates |
| `list_prefix(prefix) -> Vec<String>` | List all keys under prefix, lexicographically sorted |

**Design decisions:**
- All methods are async (the trait will use `#[async_trait]`)
- `conditional_put` returns `PreconditionFailed` on conflict — this is the foundation of the manifest CAS protocol
- `get_range` enables S3 byte-range reads for fetching individual SSTable data blocks without downloading entire files
- `list_prefix` returns sorted results — critical for finding the current manifest (highest lexicographic ID)

### LocalFsBackend

Filesystem-backed implementation used for all testing:
- `put` → write to `{base_dir}/{key}`, creating intermediate directories
- `get` → read file at `{base_dir}/{key}`
- `get_range` → seek + read within file
- `delete` → remove file
- `conditional_put` → create file only if it doesn't exist (use `O_CREAT | O_EXCL` for atomicity)
- `list_prefix` → directory listing with prefix filter, sorted

---

## 4. Protobuf API Types

Define the gRPC contract early so the storage engine implements it from the start. Four operations, all scoped to a namespace and record ID:

### PutItems
```
PutItemsRequest {
  idempotency_token: IdempotencyToken
  namespace:         string
  id:                string          // record ID
  items:             List<Item>
}
PutItemsResponse {
  version:           OrderedKey      // system-generated version
}
```

### GetItems
```
GetItemsRequest {
  namespace:         string
  id:                string
  predicate:         Predicate
  selection:         Selection
  signals:           Map<string, bytes>
}
GetItemsResponse {
  items:             List<Item>
  next_page_token:   optional<bytes>
}
```

### DeleteItems
```
DeleteItemsRequest {
  idempotency_token: IdempotencyToken
  namespace:         string
  id:                string
  predicate:         Predicate
}
DeleteItemsResponse {
  version:           OrderedKey
}
```

### ScanItems (streaming)
```
ScanItemsRequest {
  namespace:         string
  id:                string
  predicate:         Predicate
  signals:           Map<string, bytes>
}
ScanItemsResponse {  // server streams
  items:             List<Item>      // batch per stream message
}
```

### Shared Types

**Predicate** — three query modes on item keys:
```
Predicate {
  oneof {
    match_keys:  List<bytes>     // specific item keys (multi-get)
    match_range: Range           // contiguous scan
    match_all:   bool            // all items in the record
  }
}
Range {
  start_key:       bytes
  end_key:         bytes
  start_inclusive:  bool  // default true
  end_inclusive:    bool  // default false
}
```

**Selection** — pagination and projection:
```
Selection {
  page_size_bytes: uint32          // byte budget (default 2MB)
  item_limit:      uint32          // max items (0 = unlimited)
  exclude_values:  bool            // metadata-only mode
  page_token:      optional<bytes>
}
```

---

## 5. Workspace Scaffolding

Create the workspace with all crate shells:

```
flushdb/
  Cargo.toml                (workspace root)
  crates/
    flushdb-proto/          (protobuf + tonic generated code)
    flushdb-types/          (core types, traits, errors, StorageBackend)
    flushdb-wal/            (shell — depends on flushdb-types)
    flushdb-engine/         (shell — depends on flushdb-types, flushdb-wal)
    flushdb-server/         (shell — depends on flushdb-engine, flushdb-proto)
    flushdb-test/           (shell — depends on all)
```

Proto files go in `crates/flushdb-proto/proto/` with a `build.rs` using `tonic-build`.

---

## New Dependencies

| Crate | Version | Purpose |
|-------|---------|---------|
| byteorder | 1 | Big-endian encoding for OrderedKey, CompositeKey |

(All other Phase 1 dependencies — tokio, bytes, thiserror, serde, tonic/prost, uuid, etc. — are in the workspace from initial setup.)

---

## Future Work Considerations

When building Phase 1, keep the following downstream dependencies in mind:

| What You're Building | Who Needs It Later | What To Watch For |
|---------------------|-------------------|-------------------|
| **CompositeKey** | WAL (P2), Memtable (P3), SSTable (P4), Read Path (P5c) | Sort order must be `memcmp`-correct — every layer depends on this. The range tombstone sentinel prefix (`0xFF`) must sort after all valid item keys within a record. |
| **EntryValue::BlobRef** | Value separation (future) | Define the variant fully now (blob_id, offset, size) even though only `Inline` is used. Changing the enum later would require SSTable format migration. |
| **StorageBackend trait** | SSTable I/O (P4), Manifest CAS (P5a), S3 backend (P7) | `get_range` enables block-level fetches — SSTable readers depend on this for random access. `conditional_put` is the foundation of manifest CAS — its conflict semantics must be exact. `list_prefix` must return sorted results — manifest recovery picks the highest ID. |
| **FlushError** | Every crate | `EpochFenced` and `ResourceExhausted` won't be used until P5/P7, but defining them now avoids error type refactoring later. Make sure error variants carry enough context (e.g., expected vs actual epoch) for useful diagnostics. |
| **IdempotencyToken** | WAL (P2), Memtable dedup (P3), SSTable dedup block (P4), Server dedup (P7) | The 24-byte format and all-zero bypass convention must be stable — it's written into WAL entries and SSTable dedup blocks, which are persistent formats. |
| **MemtableEntry** | WAL wire format (P2), Memtable (P3), Flush (P5b) | This struct flows through the entire write path. Include all fields now (including `sequence_number`) even if assignment happens later. |
| **Proto types** | Server (P7) | The gRPC contract is implemented by the server. Proto field IDs are permanent — reserve field numbers generously for future extensions (e.g., CDC fields, cluster metadata). |

---

## Done When

- `cargo build --workspace` succeeds with zero warnings
- CompositeKey encodes and decodes (round-trip) correctly for all cases: empty item key, max-length keys, separator in item key
- CompositeKey sort order matches expected `memcmp` ordering
- CompositeKey rejects oversized record_id (>256B), oversized item_key (>4096B), and record_id containing null bytes
- IdempotencyToken encodes/decodes 24-byte format, all-zero token detected as "no idempotency"
- OrderedKey encodes to 12 bytes big-endian, sorts correctly by time
- EntryValue::Inline and EntryValue::BlobRef variants both serialize/deserialize
- FlushError covers all listed variants
- LocalFsBackend passes all StorageBackend trait methods including conditional_put conflict detection
- Proto compiles and generates Rust types for all four operations
