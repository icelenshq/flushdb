---
sidebar_position: 3
title: Data Model Patterns
---

# Data Model Patterns

flushdb's core abstraction is a **two-level sorted map**: each record contains a sorted set of items. This page shows how the same structure serves wide-row workloads (e-commerce catalogs) and graph workloads (adjacency lists). See [Architecture](./architecture#data-model) for the composite key encoding that makes this possible.

## Benchmark Dataset: E-Commerce Catalog

The `flushdb-demo` benchmark seeds an e-commerce product catalog. Each product is one record with multiple sorted item keys covering different facets of the product.

**Record ID:** `product:000042` (zero-padded product ID)

**Items per record (8-18 items, all JSON values):**

| Item Key Pattern | Value | Count |
|---|---|---|
| `info` | `{name, description, category, brand}` | 1 |
| `price` | `{current_cents, original_cents, currency}` | 1 |
| `inventory:<warehouse>` | `{count, reserved}` | 1-3 |
| `variant:<color>:<size>` | `{sku_suffix, extra_cents, in_stock}` | 2-6 |
| `review:<timestamp>` | `{rating, author, title, body}` | 1-5 |

Because item keys sort lexicographically within a record, the physical order on disk is:

```
info
inventory:warehouse-central
inventory:warehouse-east
inventory:warehouse-west
price
review:1710000042000
review:1710000042001
variant:blue:M
variant:red:XL
```

All inventory items are contiguous. All variants are contiguous. A `match_range("variant:", "variant:\xFF")` returns every variant without touching inventory, reviews, or pricing. This is a prefix scan over sorted keys — no secondary index required.

Data is deterministic (seed 42) for reproducibility. See [Operations - Benchmarks](./operations#benchmarks) for how to run the benchmark.

### Benchmark Workload Mix

The `compare` workload runs a mixed read/write pattern against both flushdb and Cassandra:

| Operation | Ratio |
|---|---|
| Point read | 60% |
| Write (new product) | 15% |
| Update (price + inventory) | 15% |
| Delete | 5% |
| Scan (all items in a record) | 5% |

The benchmark scales from 1,000 to 100,000 seeded products across 10 steps, with product ID ranges 10x larger than the seed size to ensure reads hit both existing and missing records.

## The Pattern: Entity with Sorted Attributes

The e-commerce example illustrates a general pattern:

```
record_id  = entity identifier
item_key   = attribute name (often prefixed for grouping)
value      = attribute payload
```

The prefix convention (`inventory:`, `variant:`, `review:`) creates **implicit sub-groupings** within a record. Any prefix scan returns exactly one group. This is the same principle that makes the two-level map a natural fit for graph storage.

## Graph Storage: Adjacency Lists

An adjacency list maps each vertex to its sorted list of edges. The two-level sorted map **is** an adjacency list:

| Graph Concept | flushdb Mapping |
|---|---|
| Vertex | Record (`record_id = vertex_id`) |
| Edge | Item (`item_key` encodes edge type + target vertex) |
| Edge properties | Item value |
| Adjacency list | All items in a record |

### Example: Social Graph

```
Record: user:alice
  ├── follows:user:bob           → {since: "2024-01-15", weight: 0.8}
  ├── follows:user:charlie       → {since: "2024-03-20", weight: 0.6}
  ├── post:1710000001000         → {text: "Hello world", likes: 42}
  ├── post:1710000005000         → {text: "Second post", likes: 7}
  └── profile                    → {name: "Alice", bio: "..."}
```

On disk, the composite key encoding produces:

```
[user:alice][0x00][follows:user:bob]
[user:alice][0x00][follows:user:charlie]
[user:alice][0x00][post:1710000001000]
[user:alice][0x00][post:1710000005000]
[user:alice][0x00][profile]
```

### Why This Works

**Contiguous edges.** All edges for a vertex share the same record ID prefix. They are physically adjacent in the memtable skip list and SSTable data blocks. Reading an entire adjacency list is a sequential scan, not random I/O.

**Edge type filtering is a prefix scan.** `match_range("follows:", "follows:\xFF")` returns all outgoing follow edges without touching posts or profile data. This is O(log N + k) where k is the number of matching edges — the same complexity as a B-tree range scan.

**Temporal ordering for free.** Keys like `post:<timestamp>` and `review:<timestamp>` are naturally sorted by time. "Last 10 posts" is a reverse prefix scan. No secondary index needed.

**Bloom filters on vertex IDs.** A point lookup for "does `user:alice` exist?" checks one bloom filter per SSTable level, not one per edge. The bloom filter is built on record IDs, so a single check covers the entire adjacency list.

**Record ID deduplication in SSTables.** High-degree vertices (many edges) benefit from the SSTable block format's record ID deduplication — consecutive entries with the same record ID store only the item key on subsequent entries, saving 30-50% space for wide records.

**Range tombstones for bulk edge deletion.** Deleting all edges of a type (e.g., all `follows:*` for a vertex) is a single range tombstone, not one delete per edge.

## Comparison with Dedicated Graph Databases

The adjacency list pattern in flushdb is optimized for **single-hop queries**: neighbors of a vertex, edges of a type, fan-out reads. These are the building blocks for BFS/DFS, but traversal logic runs in application code.

Dedicated graph databases (Neo4j, TigerGraph) provide native multi-hop traversal, query languages (Cypher, GSQL), and index-free adjacency. If your workload is dominated by multi-hop path queries or subgraph matching, a dedicated graph database is likely a better fit.

The flushdb approach is advantageous when:

- You need the same store for both graph and non-graph workloads (e.g., product catalog + recommendation graph)
- You want S3-native durability and economics instead of provisioned storage
- Your graph queries are primarily single-hop fan-out reads or edge-type prefix scans
- You control traversal logic in application code and want predictable per-hop latency

This is the same tradeoff as storing adjacency lists in Cassandra or DynamoDB — you trade query-language expressiveness for storage economics and operational simplicity.

## Other Use Cases

The two-level sorted map generalizes beyond e-commerce and graphs:

| Domain | record_id | item_key pattern | Query pattern |
|---|---|---|---|
| E-commerce catalog | `product:000042` | `info`, `price`, `variant:*` | Point reads + prefix scans |
| Social graph | `user:alice` | `follows:*`, `post:*` | Fan-out reads, temporal scans |
| Time series | `sensor:temp-01` | `reading:<timestamp>` | Range scans by time window |
| Document store | `doc:invoice-789` | `field:*`, `attachment:*` | Full record reads |
| IoT device state | `device:thermostat-5` | `config`, `telemetry:<ts>` | Latest-N queries |

In every case, the record groups related data and the sorted item keys enable efficient sub-group access through prefix scans.
