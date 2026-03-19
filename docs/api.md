# flushdb API Reference

## Overview

flushdb exposes four gRPC operations for reading and writing data. Every operation is scoped to a **namespace** (tenant isolation boundary) and a **record ID** (partition key). The wire protocol is Protocol Buffers over gRPC (package `flushdb.v1`).

| Operation | Type | Description |
|-----------|------|-------------|
| PutItems | Unary | Write items to a record |
| GetItems | Unary | Read items from a record with flexible predicates |
| DeleteItems | Unary | Delete items from a record |
| ScanItems | Server-streaming | Stream items from a record in batches |

All write operations (PutItems, DeleteItems) are idempotent when a valid idempotency token is provided. All read operations support three query predicates for selecting items by key.

---

## PutItems

Write one or more items to a record. If items with the same keys already exist, they are overwritten. The operation is idempotent when a non-zero idempotency token is supplied.

### Request

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| idempotency_token | IdempotencyToken | Yes | Token for at-least-once deduplication. All-zero means no deduplication. |
| namespace | string | Yes | Namespace that owns the record. |
| id | string | Yes | Record ID (partition key). Max 256 bytes, UTF-8, no null bytes. |
| items | repeated Item | Yes | One or more items to write. |

### Response

| Field | Type | Description |
|-------|------|-------------|
| version | OrderedKey | System-generated ordered key representing the write version. |

---

## GetItems

Read items from a record. The predicate controls which items are returned. Supports byte-based pagination for large result sets.

### Request

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| namespace | string | Yes | Namespace that owns the record. |
| id | string | Yes | Record ID to read from. |
| predicate | Predicate | Yes | Determines which items to return. See Predicates below. |
| selection | Selection | No | Pagination and projection options. See Selection below. |
| signals | map<string, bytes> | No | Client-to-server signals (compression capability, client version, etc.). |

### Response

| Field | Type | Description |
|-------|------|-------------|
| items | repeated Item | Matching items, sorted by item key in ascending byte order. |
| next_page_token | bytes | Present when more results are available. Pass to the next request's Selection.page_token to continue. |

---

## DeleteItems

Delete items from a record. The predicate controls which items are deleted. Like PutItems, this operation is idempotent when a valid token is provided.

### Request

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| idempotency_token | IdempotencyToken | Yes | Token for at-least-once deduplication. All-zero means no deduplication. |
| namespace | string | Yes | Namespace that owns the record. |
| id | string | Yes | Record ID to delete from. |
| predicate | Predicate | Yes | Determines which items to delete. See Predicates below. |

### Response

| Field | Type | Description |
|-------|------|-------------|
| version | OrderedKey | System-generated ordered key representing the delete version. |

### Delete Behavior by Predicate

The storage cost and performance of a delete depends on the predicate used.

| Predicate | Tombstone Strategy | Performance |
|-----------|--------------------|-------------|
| match_all | Single record-level tombstone | Constant time, independent of item count |
| match_range | Single range tombstone covering the specified interval | Constant time, independent of items in range |
| match_keys | One point tombstone per specified key | Linear in the number of keys |

---

## ScanItems

Server-side streaming variant of GetItems, designed for large result sets. Instead of pagination tokens, the server pushes batches of items as individual stream messages until the predicate is exhausted.

### Request

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| namespace | string | Yes | Namespace that owns the record. |
| id | string | Yes | Record ID to scan. |
| predicate | Predicate | Yes | Determines which items to stream. See Predicates below. |
| signals | map<string, bytes> | No | Client-to-server signals. |

### Response (stream)

Each message in the stream contains:

| Field | Type | Description |
|-------|------|-------------|
| items | repeated Item | A batch of matching items. Multiple messages are sent until all matching items have been delivered. |

---

## Shared Types

### Item

Represents a single key-value entry within a record.

| Field | Type | Description |
|-------|------|-------------|
| key | bytes | Sort key within the record. Max 4096 bytes. Items are ordered by key in ascending byte order. |
| value | bytes | Payload. Values under the separation threshold are stored inline; larger values are stored in blob objects or chunked automatically. |
| metadata | bytes | Optional application-defined metadata (content type, schema version, etc.). |
| chunk | uint32 | Chunk index for large values. 0 for non-chunked items. Managed by the server; clients typically do not set this. |

### OrderedKey

A 12-byte system-generated version identifier. Monotonically increasing and naturally sorted by time.

| Field | Type | Description |
|-------|------|-------------|
| timestamp_ms | uint64 | Millisecond-precision timestamp of when the write was accepted. |
| node_id | uint32 | Identifier of the node that processed the write. |
| sequence | uint32 | Per-node sequence number within the same millisecond. Supports up to 65,535 writes per millisecond per node. |

### Predicate

Exactly one of the following must be set.

| Variant | Type | Description |
|---------|------|-------------|
| match_keys | MatchKeys | Retrieve specific items by their exact keys. |
| match_range | MatchRange | Retrieve a contiguous range of items by key bounds. |
| match_all | bool | Set to true to retrieve all items in the record. |

### MatchKeys

| Field | Type | Description |
|-------|------|-------------|
| keys | repeated bytes | List of exact item keys to retrieve. Keys not found are silently omitted from the response. |

### MatchRange

| Field | Type | Description |
|-------|------|-------------|
| start_key | bytes | Lower bound of the key range. |
| end_key | bytes | Upper bound of the key range. |
| start_inclusive | bool | Whether the start bound is inclusive. Default: true. |
| end_inclusive | bool | Whether the end bound is inclusive. Default: false. |

### Selection

Controls pagination and projection for GetItems responses.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| page_size_bytes | uint32 | 2 MB | Target byte budget for the response page. The server accumulates items until this budget is approximately met. |
| item_limit | uint32 | 0 (unlimited) | Maximum number of items to return per page. 0 means no item count limit; only the byte budget applies. |
| exclude_values | bool | false | When true, return item keys and metadata only, without value payloads. Useful for listing or counting items without transferring data. |
| page_token | bytes | (none) | Continuation token from a previous response's next_page_token. Omit on the first request. |

### IdempotencyToken

| Field | Type | Description |
|-------|------|-------------|
| generation_time | uint64 | Client-assigned monotonic timestamp in milliseconds. Used for token expiration and clock drift detection. |
| token | bytes | 16-byte UUID v7 nonce. Combined with generation_time, forms the full 24-byte dedup identity. |

---

## Idempotency

PutItems and DeleteItems accept an idempotency token for at-least-once delivery deduplication.

### Token Format

The full token is 24 bytes: 8 bytes of generation_time (uint64, millisecond timestamp) followed by 16 bytes of a UUID v7 nonce.

### Bypass

Setting all 24 bytes to zero means "no idempotency." The write is always applied regardless of whether an identical operation was previously processed.

### Deduplication Mechanism

Tokens are checked against multiple tiers:

| Tier | Scope | Description |
|------|-------|-------------|
| Active memtable | In-memory | Tokens stored inline with recent writes. Covers retries within seconds. |
| Frozen memtables | In-memory | Pending-flush memtables still hold their tokens. |
| SSTable dedup blocks | On disk / S3 | Each SSTable contains a dedicated dedup block with a compact hash set of 128-bit token hashes. L0 and L1 dedup blocks are pinned in memory. |

### Retention

Tokens older than the retention TTL (default 10 minutes) are accepted unconditionally, meaning no dedup check is performed. Expired tokens are dropped from dedup blocks during compaction.

### Clock Drift

If a token's generation_time differs from the server's clock by more than a configurable threshold (default 5 seconds), the request is rejected.

---

## Pagination

flushdb uses byte-based pagination rather than fixed row counts.

### How It Works

The client specifies a target page size in bytes (default 2 MB) and an optional item limit. The server accumulates items until the byte budget is approximately met, then returns the page along with a continuation token if more results remain.

### Adaptive Sizing

The server maintains cached average item sizes per namespace. On the first request, it estimates how many items to fetch based on this average. Subsequent pages carry the observed average in the page token for more accurate estimation.

### SLO-Aware Early Return

If the server detects that the request's latency budget is nearly exhausted (based on namespace-configured SLOs or gRPC deadline), it stops accumulating and returns a partial page with a valid continuation token. This prevents timeouts on large reads.

### Page Token Encoding

The page token encodes the last item key returned. Resuming a paginated read is a forward seek to the next key after the encoded position, with no re-scanning of previously returned items.

### Cross-Partition Page Tokens

For queries that span multiple partitions, the page token encodes independent per-partition cursors. Each partition resumes from where it left off, and the coordinator merges the results.

---

## Cross-Partition Queries

When a query cannot be resolved to a single partition, the coordinator fans out to the relevant partition owners in parallel.

### Fan-Out Reduction

| Strategy | Description |
|----------|-------------|
| Partition bloom filters | Approximately 100 KB per partition, over contained record IDs. Reduces point query fan-out to typically one partition. |
| Progressive fan-out | Partitions are queried in batches of 16. Remaining batches are cancelled once the byte budget is satisfied. |
| Partition affinity cache | An LRU cache (100K entries) mapping record IDs to partition IDs, eliminating fan-out for repeat queries. |
| Max fan-out budget | Configurable per namespace (default 64). Limits the maximum number of partitions contacted per query. |

---

## Client-Server Signaling

flushdb supports a bidirectional signaling mechanism for capability negotiation between clients and servers.

### Server Signals

Delivered to the client during connection handshake and refreshed periodically.

| Signal | Description |
|--------|-------------|
| Latency SLOs | Target and maximum latency for the namespace. |
| Supported compression codecs | Which compression algorithms the server can decode. |
| Max page size | Upper bound on page_size_bytes the server will honor. |
| Feature flags | Server-side feature toggles relevant to the client. |

### Client Signals

Sent with each request via the signals field.

| Signal | Description |
|--------|-------------|
| Compression capability | Which compression algorithms the client can decode. |
| Chunking support | Whether the client can reassemble chunked large values. |
| Client version | Client library version for compatibility decisions. |
