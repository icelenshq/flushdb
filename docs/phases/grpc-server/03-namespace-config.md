# Task 3: Namespace Configuration

**Crate:** `flushdb-server`
**File:** `src/namespace_config.rs`
**Depends on:** Nothing
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §20 (Multi-Tenancy), Phase 7 §2

---

## Goal

Define the namespace configuration types that provide tenant isolation. Each namespace has its own partitioning strategy, S3 path prefix, performance tuning, and storage layers. This is the foundation that the partition router, namespace manager, and server bootstrap all build on.

---

## What to Build

### 3.1 PartitionKeyStrategy Enum

```
PartitionKeyStrategy {
    Simple,
    Composite { delimiter: String, field_indices: Vec<usize> },
    Prefix { length: usize },
    CustomHash { hash_name: String },
}
```

**Derives:** `Clone, Debug, PartialEq, Eq, Serialize, Deserialize`

| Variant | Description | Use Case |
|---------|-------------|----------|
| `Simple` | `hash(record_id) % partition_count` | Default. Uniform distribution. |
| `Composite` | Split record_id by delimiter, hash on selected field indices | Multi-tenant with locality (e.g., `tenant:region`) |
| `Prefix` | First `length` bytes of record_id → partition key | Natural prefix grouping (e.g., geo codes) |
| `CustomHash` | Named hash function applied before partition assignment | Client-controlled distribution |

### 3.2 ConsistencyScope Enum

```
ConsistencyScope { Local, Global }
```

**Derives:** `Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default`

Default: `Local`

### 3.3 ConsistencyTarget Enum

```
ConsistencyTarget { ReadYourWrites, Eventual }
```

**Derives:** `Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default`

Default: `ReadYourWrites`

### 3.4 WriteConsistency Enum

```
WriteConsistency { One, Quorum, All }
```

**Derives:** `Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default`

Default: `Quorum`

### 3.5 StorageLayerType Enum

```
StorageLayerType { S3, Cache }
```

**Derives:** `Clone, Debug, PartialEq, Eq, Serialize, Deserialize`

### 3.6 StorageLayerConfig Struct

```
StorageLayerConfig {
    consistency_scope:  ConsistencyScope,
    consistency_target: ConsistencyTarget,
    default_ttl:        Option<Duration>,
}
```

**Derives:** `Clone, Debug, PartialEq, Eq, Serialize, Deserialize`

All fields use `#[serde(default)]` for forward-compatible deserialization.

### 3.7 StorageLayer Struct

```
StorageLayer {
    id:     String,
    layer_type: StorageLayerType,
    config: StorageLayerConfig,
}
```

**Derives:** `Clone, Debug, PartialEq, Eq, Serialize, Deserialize`

### 3.8 NamespaceConfig Struct

```
NamespaceConfig {
    // Identity (immutable after creation)
    name:                    String,
    partition_key_strategy:  PartitionKeyStrategy,
    partition_count:         u32,
    s3_path_prefix:          String,

    // Storage configuration
    persistence:             Vec<StorageLayer>,

    // Performance tuning (mutable at runtime)
    memtable_size_threshold: u64,        // bytes, default 64MB
    compaction_strategy:     String,      // "LEVELED" (only option for now)
    bloom_filter_fp_rate:    f64,         // default 0.01
    default_page_size_bytes: u32,        // default 2MB
    max_page_size_bytes:     u32,        // default 8MB
    target_latency_slo_ms:   u64,        // default 10
    max_latency_slo_ms:      u64,        // default 500

    // Replication (for future cluster mode)
    write_consistency:       WriteConsistency,
    replication_factor:      u32,        // default 3
}
```

**Derives:** `Clone, Debug, Serialize, Deserialize`

**Default values** via `#[serde(default)]` on all performance tuning and replication fields.

### 3.9 Construction and Defaults

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(name: String, partition_count: u32) -> FlushResult<Self>` | Creates config with Simple strategy and sensible defaults. Validates partition_count. |
| `with_strategy` | `(name: String, strategy: PartitionKeyStrategy, partition_count: u32) -> FlushResult<Self>` | Creates config with specified strategy. |
| `validate` | `(&self) -> FlushResult<()>` | Runs all validation rules. Called at construction and deserialization. |
| `s3_path_prefix` | `(&self) -> &str` | Returns the S3 path prefix (defaults to `flushdb/{name}/` if not set) |
| `engine_config` | `(&self) -> EngineConfig` | Converts namespace tuning fields to an `EngineConfig` for the engine crate |

### 3.10 Validation Rules

| Check | Error |
|-------|-------|
| `name` is empty | `FlushError::InvalidArgument { field: "name", reason: "must not be empty" }` |
| `name` contains `/` or `\0` | `FlushError::InvalidArgument { field: "name", reason: "must not contain / or null bytes" }` |
| `partition_count` is 0 | `FlushError::InvalidArgument { field: "partition_count", reason: "must be > 0" }` |
| `partition_count` is not power of 2 | `FlushError::InvalidArgument { field: "partition_count", reason: "must be a power of 2" }` |
| `bloom_filter_fp_rate` <= 0.0 or >= 1.0 | `FlushError::InvalidArgument { field: "bloom_filter_fp_rate", reason: "must be in (0.0, 1.0)" }` |
| `default_page_size_bytes` > `max_page_size_bytes` | `FlushError::InvalidArgument { field: "default_page_size_bytes", reason: "must be <= max_page_size_bytes" }` |
| `target_latency_slo_ms` > `max_latency_slo_ms` | `FlushError::InvalidArgument { field: "target_latency_slo_ms", reason: "must be <= max_latency_slo_ms" }` |
| `Composite` strategy with empty `field_indices` | `FlushError::InvalidArgument { field: "field_indices", reason: "must have at least one field index" }` |
| `Prefix` strategy with `length` = 0 | `FlushError::InvalidArgument { field: "prefix_length", reason: "must be > 0" }` |

### 3.11 Immutability Semantics

The following fields are immutable after namespace creation:
- `name`
- `partition_key_strategy`
- `partition_count`

A method `can_update_from(&self, other: &NamespaceConfig) -> FlushResult<()>` validates that an updated config only changes mutable fields. Returns `FlushError::InvalidArgument` with details if immutable fields differ.

### 3.12 Design Decisions

- **`#[serde(default)]` on optional/tuning fields** — future phases can add new fields (e.g., CDC sink configuration) without breaking deserialization of existing configs.
- **`partition_count` as `u32`** — supports up to 4 billion partitions. Power-of-2 constraint allows `hash % partition_count` to be implemented as `hash & (partition_count - 1)`.
- **`s3_path_prefix` defaults to `flushdb/{name}/`** — convention from STORAGE_DESIGN.md §23.
- **`compaction_strategy` as String** — extensible for future strategies without enum migration.

---

## Tests

**File:** `crates/flushdb-server/tests/namespace_config_tests.rs`

### Construction Tests
| Test | What It Validates |
|------|-------------------|
| `test_new_simple_defaults` | `new("test-ns", 4)` creates config with Simple strategy and correct defaults |
| `test_new_with_strategy` | `with_strategy` sets the correct strategy variant |
| `test_default_values` | All default values match spec (64MB memtable, 0.01 bloom FP rate, 2MB page size, etc.) |
| `test_s3_path_prefix_default` | Default prefix is `flushdb/{name}/` |

### Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_rejects_empty_name` | Empty name → `InvalidArgument` |
| `test_rejects_slash_in_name` | Name with `/` → `InvalidArgument` |
| `test_rejects_null_in_name` | Name with `\0` → `InvalidArgument` |
| `test_rejects_zero_partition_count` | `partition_count = 0` → `InvalidArgument` |
| `test_rejects_non_power_of_two` | `partition_count = 3` → `InvalidArgument` |
| `test_accepts_power_of_two` | 1, 2, 4, 8, 16, 32, 64, 128, 256 all accepted |
| `test_rejects_invalid_bloom_fp_rate` | 0.0 and 1.0 rejected, 0.01 accepted |
| `test_rejects_page_size_inversion` | `default_page_size > max_page_size` → error |
| `test_rejects_slo_inversion` | `target_slo > max_slo` → error |
| `test_rejects_composite_empty_fields` | Composite strategy with empty field_indices → error |
| `test_rejects_prefix_zero_length` | Prefix strategy with length 0 → error |

### Serialization Tests
| Test | What It Validates |
|------|-------------------|
| `test_serde_round_trip` | Serialize to JSON → deserialize → equals original |
| `test_serde_missing_optional_fields` | Deserialize JSON with only required fields uses defaults |
| `test_serde_forward_compatibility` | Deserialize JSON with unknown extra fields succeeds (serde default behavior) |

### Immutability Tests
| Test | What It Validates |
|------|-------------------|
| `test_can_update_mutable_fields` | Changing memtable_size, bloom_fp_rate, page sizes → ok |
| `test_rejects_name_change` | Changing name → error |
| `test_rejects_strategy_change` | Changing partition_key_strategy → error |
| `test_rejects_partition_count_change` | Changing partition_count → error |

### Engine Config Conversion Tests
| Test | What It Validates |
|------|-------------------|
| `test_engine_config_defaults` | `engine_config()` produces correct EngineConfig from defaults |
| `test_engine_config_custom_values` | Custom memtable size and bloom FP rate propagate to EngineConfig |

---

## Done When

- [ ] All types defined with correct derives and serde attributes
- [ ] Validation catches all invalid configurations at construction time
- [ ] Serde round-trip works with forward-compatible deserialization
- [ ] Immutable field changes are rejected with clear error messages
- [ ] Default values match the spec (64MB memtable, 0.01 bloom, 2MB/8MB page sizes, 10ms/500ms SLO)
- [ ] `engine_config()` correctly maps namespace tuning to EngineConfig
- [ ] All tests pass
