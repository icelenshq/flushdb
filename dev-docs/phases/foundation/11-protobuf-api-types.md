# Task 11: Protobuf API Types

**Crate:** `flushdb-proto`
**File:** `proto/flushdb.proto`, `build.rs`, `src/lib.rs`
**Depends on:** Task 1 (workspace)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §19 (gRPC API), Phase 1 §4

---

## Goal

Define the gRPC contract early so the storage engine implements it from the start. Four operations, all scoped to a namespace and record ID. Proto field IDs are permanent — reserve field numbers generously for future extensions.

---

## What to Build

### 11.1 Proto File Structure

Single proto file: `crates/flushdb-proto/proto/flushdb.proto`

```
syntax = "proto3";
package flushdb.v1;
```

Use `v1` package to allow future breaking changes via `v2`.

### 11.2 Service Definition

```protobuf
service FlushDb {
  rpc PutItems(PutItemsRequest) returns (PutItemsResponse);
  rpc GetItems(GetItemsRequest) returns (GetItemsResponse);
  rpc DeleteItems(DeleteItemsRequest) returns (DeleteItemsResponse);
  rpc ScanItems(ScanItemsRequest) returns (stream ScanItemsResponse);
}
```

Note: `ScanItems` uses server-side streaming (`stream` on response only).

### 11.3 Message Definitions

#### PutItems

```protobuf
message PutItemsRequest {
  IdempotencyToken idempotency_token = 1;
  string namespace = 2;
  string id = 3;                        // record ID
  repeated Item items = 4;
  // Reserved for future: flush_immediate, consistency_level
  reserved 10 to 20;
}

message PutItemsResponse {
  OrderedKey version = 1;               // system-generated version
  // Reserved for future: applied_at, shard_id
  reserved 10 to 20;
}
```

#### GetItems

```protobuf
message GetItemsRequest {
  string namespace = 1;
  string id = 2;                        // record ID
  Predicate predicate = 3;
  Selection selection = 4;
  map<string, bytes> signals = 5;       // client → server signals
  // Reserved for future: consistency, allow_follower_read
  reserved 10 to 20;
}

message GetItemsResponse {
  repeated Item items = 1;
  bytes next_page_token = 2;            // empty if no more pages
  // Reserved for future: stats, cache_hit_ratio
  reserved 10 to 20;
}
```

#### DeleteItems

```protobuf
message DeleteItemsRequest {
  IdempotencyToken idempotency_token = 1;
  string namespace = 2;
  string id = 3;                        // record ID
  Predicate predicate = 4;
  // Reserved for future: consistency_level
  reserved 10 to 20;
}

message DeleteItemsResponse {
  OrderedKey version = 1;               // system-generated version
  // Reserved for future
  reserved 10 to 20;
}
```

#### ScanItems (Streaming)

```protobuf
message ScanItemsRequest {
  string namespace = 1;
  string id = 2;                        // record ID
  Predicate predicate = 3;
  map<string, bytes> signals = 4;       // client → server signals
  // Reserved for future: batch_size_hint
  reserved 10 to 20;
}

message ScanItemsResponse {
  repeated Item items = 1;              // batch per stream message
  // Reserved for future: progress_hint
  reserved 10 to 20;
}
```

### 11.4 Shared Types

#### Item

```protobuf
message Item {
  bytes key = 1;                        // sort key within the record
  bytes value = 2;                      // payload
  bytes metadata = 3;                   // optional (content type, schema version, etc.)
  uint32 chunk = 4;                     // chunk index (0 = non-chunked)
  // Reserved for future: ttl, version
  reserved 10 to 20;
}
```

#### IdempotencyToken

```protobuf
message IdempotencyToken {
  uint64 generation_time = 1;           // client monotonic timestamp (ms)
  bytes token = 2;                      // 16-byte UUID v7 nonce
  // Reserved for future
  reserved 10 to 20;
}
```

#### OrderedKey

```protobuf
message OrderedKey {
  uint64 timestamp_ms = 1;             // millisecond timestamp
  uint32 node_id = 2;                  // node identifier (stored as u16 in Rust)
  uint32 sequence = 3;                 // per-node sequence (stored as u16 in Rust)
  // Reserved for future
  reserved 10 to 20;
}
```

Note: `node_id` and `sequence` use `uint32` in proto (proto3 doesn't have uint16), but the Rust code will validate they fit in u16.

#### Predicate

```protobuf
message Predicate {
  oneof predicate {
    MatchKeys match_keys = 1;           // specific item keys (multi-get)
    MatchRange match_range = 2;         // contiguous scan
    bool match_all = 3;                 // all items in the record
  }
}

message MatchKeys {
  repeated bytes keys = 1;             // list of specific item keys
}

message MatchRange {
  bytes start_key = 1;
  bytes end_key = 2;
  bool start_inclusive = 3;            // default true
  bool end_inclusive = 4;              // default false
}
```

#### Selection

```protobuf
message Selection {
  uint32 page_size_bytes = 1;          // byte budget (default 2MB)
  uint32 item_limit = 2;              // max items (0 = unlimited)
  bool exclude_values = 3;            // metadata-only mode
  bytes page_token = 4;               // pagination cursor
  // Reserved for future: projection fields
  reserved 10 to 20;
}
```

### 11.5 Build Configuration

#### `crates/flushdb-proto/build.rs`

Use `tonic-build` to compile the proto file:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::compile_protos("proto/flushdb.proto")?;
    Ok(())
}
```

#### `crates/flushdb-proto/Cargo.toml`

Dependencies:
- `tonic` (workspace)
- `prost` (workspace)

Build dependencies:
- `tonic-build` (workspace)

#### `crates/flushdb-proto/src/lib.rs`

Re-export the generated module:

```rust
pub mod flushdb {
    pub mod v1 {
        tonic::include_proto!("flushdb.v1");
    }
}
```

### 11.6 Field Number Strategy

- **Fields 1-9:** Active fields used in Phase 1
- **Fields 10-20:** Reserved for future extensions within each message (CDC fields, cluster metadata, etc.)
- **Never reuse a field number** — removed fields should be `reserved`
- This prevents accidental wire format conflicts when features are added later

### 11.7 Proto Design Decisions

- **`bytes` for keys/values** (not `string`) — item keys and values are arbitrary bytes, not necessarily UTF-8
- **`string` for namespace and record ID** — these are always UTF-8 strings
- **`oneof` for Predicate** — exactly one query mode per request
- **`map<string, bytes>` for signals** — extensible key-value bag for client-server negotiation
- **`repeated Item` in responses** — batch-oriented, not single-item
- **Streaming only on ScanItems** — GetItems uses pagination instead, ScanItems uses server-streaming for unbounded result sets
- **`uint32` for proto fields that are `u16` in Rust** — proto3 doesn't support 16-bit integers. Validation at the Rust boundary.

---

## Tests

Proto compilation is tested implicitly: if `cargo build -p flushdb-proto` succeeds, the proto compiled correctly.

**File:** `crates/flushdb-proto/tests/proto_compilation_tests.rs` (or inline test)

| Test | What It Validates |
|------|-------------------|
| `test_put_items_request_construction` | Can construct a `PutItemsRequest` with all fields |
| `test_get_items_request_construction` | Can construct a `GetItemsRequest` with predicate |
| `test_delete_items_request_construction` | Can construct a `DeleteItemsRequest` |
| `test_scan_items_request_construction` | Can construct a `ScanItemsRequest` |
| `test_predicate_match_keys` | Can create `MatchKeys` predicate |
| `test_predicate_match_range` | Can create `MatchRange` predicate |
| `test_predicate_match_all` | Can create `match_all` predicate |
| `test_selection_construction` | Can create `Selection` with all fields |
| `test_item_construction` | Can create `Item` with all fields |
| `test_ordered_key_construction` | Can create `OrderedKey` proto message |
| `test_idempotency_token_construction` | Can create `IdempotencyToken` proto message |

These tests verify the generated Rust types are usable and have the expected fields. They don't test serialization (prost handles that).

---

## Done When

- [ ] Proto file defines all 4 service RPCs
- [ ] All message types compile via tonic-build
- [ ] Generated Rust types are accessible via `flushdb_proto::flushdb::v1::*`
- [ ] Field numbers reserved for future extensions
- [ ] `cargo build -p flushdb-proto` succeeds
- [ ] Construction tests pass for all message types
