# Task 4: Partition Key Router

**Crate:** `flushdb-server`
**File:** `src/partition_router.rs`
**Depends on:** Task 3 (NamespaceConfig, PartitionKeyStrategy)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §16.3 (User-Defined Partition Keys), Phase 7 §3

---

## Goal

Implement the partition routing layer that maps `(namespace, record_id) → partition_id` using the namespace's configured partition key strategy. This is the dispatcher that determines which engine instance handles each request. Designed as a pluggable trait so future cluster mode can swap in ring-based routing.

---

## What to Build

### 4.1 PartitionRouter Trait

```
#[async_trait]
trait PartitionRouter: Send + Sync {
    fn route(&self, record_id: &str) -> FlushResult<u32>;
}
```

Returns the `partition_id` (0-based index into the namespace's partition set).

**Why a trait:** Future cluster mode will implement this trait with a consistent hashing ring that maps partition IDs to physical nodes. The gRPC handlers call `route()` without knowing whether routing is local or ring-based.

### 4.2 LocalPartitionRouter Struct

```
LocalPartitionRouter {
    strategy:        PartitionKeyStrategy,
    partition_count: u32,
    partition_mask:  u32,    // partition_count - 1, for bitwise AND
}
```

The single-node implementation that routes all partitions locally.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: &NamespaceConfig) -> Self` | Extracts strategy and partition_count from config |
| `route` | `(&self, record_id: &str) -> FlushResult<u32>` | Dispatches to strategy-specific routing |

### 4.3 Strategy Implementations

All strategies follow the same pattern: extract a partition key from the record_id, hash it, apply `& partition_mask`.

#### Simple Strategy

```
partition_id = hash(record_id) & partition_mask
```

Direct hash of the full record_id. Default strategy.

#### Composite Strategy

```
fields = record_id.split(delimiter)
selected = fields[field_indices[0]], fields[field_indices[1]], ...
partition_key = selected.join(delimiter)
partition_id = hash(partition_key) & partition_mask
```

- If the record_id has fewer fields than the highest field index, return `FlushError::InvalidArgument` with details.
- Field indices are 0-based.

#### Prefix Strategy

```
prefix = record_id[..min(length, record_id.len())]
partition_id = hash(prefix) & partition_mask
```

- If record_id is shorter than `length`, use the full record_id as the prefix (no error).

#### Custom Hash Strategy

```
partition_id = named_hash(hash_name, record_id) & partition_mask
```

- For now, support `"fnv"` and `"crc32"` as named hash functions.
- Unknown hash names return `FlushError::InvalidArgument { field: "hash_name", reason: "unknown hash function: {name}" }`.

### 4.4 Hash Function

Use a fast, well-distributed hash function for partition routing. CRC32 (via `crc32fast`) is suitable — already a project dependency.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `partition_hash` | `(input: &[u8]) -> u32` | Returns CRC32 hash of input bytes |

### 4.5 Validation Rules

| Check | Error |
|-------|-------|
| Composite: record_id has fewer fields than highest field index | `FlushError::InvalidArgument { field: "record_id", reason: "expected at least N fields, got M" }` |
| CustomHash: unknown hash function name | `FlushError::InvalidArgument { field: "hash_name", reason: "unknown hash function: {name}" }` |

---

## Tests

**File:** `crates/flushdb-server/tests/partition_router_tests.rs`

### Simple Strategy Tests
| Test | What It Validates |
|------|-------------------|
| `test_simple_deterministic` | Same record_id always routes to same partition |
| `test_simple_distribution_uniform` | 10,000 random record_ids across 16 partitions — each gets at least 1% (no starvation) |
| `test_simple_single_partition` | With partition_count=1, all record_ids route to partition 0 |
| `test_simple_different_records_different_partitions` | Different record_ids can route to different partitions |

### Composite Strategy Tests
| Test | What It Validates |
|------|-------------------|
| `test_composite_extracts_correct_fields` | `"tenant:region:id"` with delimiter `":"` and indices `[0,1]` → hash of `"tenant:region"` |
| `test_composite_same_prefix_same_partition` | `"acme:us:123"` and `"acme:us:456"` route to same partition (locality) |
| `test_composite_different_prefix_can_differ` | `"acme:us:123"` and `"beta:eu:789"` can route to different partitions |
| `test_composite_insufficient_fields` | `"single"` with delimiter `":"` and indices `[0,1]` → `InvalidArgument` |

### Prefix Strategy Tests
| Test | What It Validates |
|------|-------------------|
| `test_prefix_extracts_correct_length` | `"us-east-user123"` with prefix_length=7 → hash of `"us-east"` |
| `test_prefix_same_prefix_same_partition` | `"us-east-a"` and `"us-east-b"` route to same partition |
| `test_prefix_short_record_id` | Record_id shorter than prefix length → uses full record_id (no error) |

### Custom Hash Strategy Tests
| Test | What It Validates |
|------|-------------------|
| `test_custom_hash_crc32` | `CustomHash { hash_name: "crc32" }` produces valid partition |
| `test_custom_hash_fnv` | `CustomHash { hash_name: "fnv" }` produces valid partition |
| `test_custom_hash_unknown_rejected` | `CustomHash { hash_name: "sha256" }` → `InvalidArgument` |

### Partition Boundary Tests
| Test | What It Validates |
|------|-------------------|
| `test_partition_id_within_bounds` | All routed partition_ids are `< partition_count` |
| `test_power_of_two_masking` | Bitwise AND with mask produces same result as modulo |
| `test_partition_count_1` | Single partition always returns 0 |
| `test_partition_count_256` | 256 partitions — IDs in range `[0, 255]` |

### Trait Object Tests
| Test | What It Validates |
|------|-------------------|
| `test_router_as_trait_object` | `LocalPartitionRouter` can be used as `dyn PartitionRouter` |

---

## Done When

- [ ] `PartitionRouter` trait defined with `route` method
- [ ] `LocalPartitionRouter` implements all 4 strategies
- [ ] Simple strategy distributes uniformly (no starvation for 10K random IDs)
- [ ] Composite strategy extracts and hashes correct fields
- [ ] Prefix strategy handles short record_ids gracefully
- [ ] Custom hash supports "fnv" and "crc32", rejects unknown
- [ ] All partition_ids are within `[0, partition_count)` bounds
- [ ] Router is usable as `dyn PartitionRouter` trait object
- [ ] All tests pass
