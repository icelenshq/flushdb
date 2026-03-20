# gRPC API

flushdb exposes a single gRPC service with four operations. The proto definition is at `crates/flushdb-proto/proto/flushdb.proto`.

## Service Definition

```protobuf
service FlushDb {
  rpc PutItems(PutItemsRequest)       returns (PutItemsResponse);
  rpc GetItems(GetItemsRequest)       returns (GetItemsResponse);
  rpc DeleteItems(DeleteItemsRequest) returns (DeleteItemsResponse);
  rpc ScanItems(ScanItemsRequest)     returns (stream ScanItemsResponse);
}
```

## Operations

### PutItems

Write one or more items to a record.

```protobuf
message PutItemsRequest {
  IdempotencyToken idempotency_token = 1;
  string namespace = 2;
  string id = 3;                    // Record ID
  repeated Item items = 4;
}

message PutItemsResponse {
  OrderedKey version = 1;           // Assigned version for this write
}
```

The `idempotency_token` ensures exactly-once semantics — retrying the same token is a no-op. The returned `version` is a monotonically increasing key that can be used for ordering.

### GetItems

Read items from a record with optional filtering and pagination.

```protobuf
message GetItemsRequest {
  string namespace = 1;
  string id = 2;                    // Record ID
  Predicate predicate = 3;          // Which items to return
  Selection selection = 4;          // Pagination controls
  map<string, bytes> signals = 5;   // Extensible hints
}

message GetItemsResponse {
  repeated Item items = 1;
  bytes next_page_token = 2;        // Empty when no more pages
}
```

### DeleteItems

Delete items matching a predicate from a record.

```protobuf
message DeleteItemsRequest {
  IdempotencyToken idempotency_token = 1;
  string namespace = 2;
  string id = 3;
  Predicate predicate = 4;          // Which items to delete
}

message DeleteItemsResponse {
  OrderedKey version = 1;
}
```

### ScanItems

Server-streaming scan over items in a record. Unlike `GetItems`, this streams results back without pagination tokens — the server pushes batches as they become available.

```protobuf
message ScanItemsRequest {
  string namespace = 1;
  string id = 2;
  Predicate predicate = 3;
  map<string, bytes> signals = 4;
}

message ScanItemsResponse {
  repeated Item items = 1;
}
```

## Common Types

### Item

```protobuf
message Item {
  bytes key = 1;           // Item key within the record
  bytes value = 2;         // Item value (opaque bytes)
  bytes metadata = 3;      // Optional metadata
  uint32 chunk = 4;        // Chunk index for large values
}
```

### Predicate

Controls which items are returned or deleted.

```protobuf
message Predicate {
  oneof predicate {
    MatchKeys match_keys = 1;       // Specific keys
    MatchRange match_range = 2;     // Key range
    bool match_all = 3;             // All items in record
  }
}

message MatchKeys {
  repeated bytes keys = 1;
}

message MatchRange {
  bytes start_key = 1;
  bytes end_key = 2;
  bool start_inclusive = 3;
  bool end_inclusive = 4;
}
```

### Selection

Controls pagination for `GetItems`.

```protobuf
message Selection {
  uint32 page_size_bytes = 1;   // Max response size in bytes
  uint32 item_limit = 2;        // Max number of items
  bool exclude_values = 3;      // Return keys/metadata only
  bytes page_token = 4;         // Continue from previous page
}
```

Pagination is **byte-based** — set `page_size_bytes` to control response size. Pass the returned `next_page_token` back to get the next page. When the token is empty, there are no more results.

### IdempotencyToken

```protobuf
message IdempotencyToken {
  uint64 generation_time = 1;   // Client-assigned timestamp
  bytes token = 2;              // 24 bytes: 8-byte client ID + 16-byte UUID
}
```

### OrderedKey

System-generated version assigned to each write.

```protobuf
message OrderedKey {
  uint64 timestamp_ms = 1;     // Millisecond timestamp
  uint32 node_id = 2;          // Node that accepted the write
  uint32 sequence = 3;         // Per-node sequence counter
}
```

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 50051 | gRPC | API endpoint |
| 9090 | HTTP | Prometheus metrics |
