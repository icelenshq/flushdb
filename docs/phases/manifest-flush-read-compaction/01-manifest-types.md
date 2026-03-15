# Task 1: Manifest Types & Serialization

**Crate:** `flushdb-engine`
**File:** `src/manifest/types.rs`, `src/manifest/mod.rs`
**Depends on:** Nothing
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §8.1 (Manifest Contents), §8.2 (Manifest ID Scheme)

---

## Goal

Define the manifest data structures that are the single source of truth for which SSTables are live at each level. Every component in Phase 5 — flush, reads, compaction, recovery — depends on these types. The manifest is JSON-serialized for human readability and debuggability.

---

## What to Build

### 1.1 ManifestId

A newtype wrapping `u64` that formats as a zero-padded 20-digit string for S3 key construction and lexicographic ordering.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(id: u64) -> Self` | Wraps the raw ID |
| `next` | `(&self) -> Self` | Returns `ManifestId(self.0 + 1)` |
| `as_u64` | `(&self) -> u64` | Returns the raw value |
| `to_path_string` | `(&self) -> String` | Formats as `"00000000000000000042"` (20-digit zero-padded) |
| `from_path_string` | `(s: &str) -> FlushResult<Self>` | Parses a 20-digit string back to ManifestId |
| `ZERO` | const | `ManifestId(0)` — sentinel for "no previous manifest" |

Implements: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash`, `Serialize`, `Deserialize`

Custom `Serialize`/`Deserialize`: serializes as the 20-digit string, not as a raw integer. This matches the JSON format in STORAGE_DESIGN.md §8.1.

### 1.2 SSTableMeta

Metadata for a single SSTable file, stored in the manifest. This is NOT the in-memory SSTable reader — it's the serializable metadata record.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String` | ULID-based SSTable identifier (e.g., `"01JKQW3XYZ-L0-0001"`) |
| `size_bytes` | `u64` | Total file size on StorageBackend |
| `entry_count` | `u64` | Number of entries in the SSTable |
| `min_key` | `Vec<u8>` | First composite key (binary, base64-encoded in JSON) |
| `max_key` | `Vec<u8>` | Last composite key (binary, base64-encoded in JSON) |
| `bloom_filter_offset` | `u64` | Byte offset of bloom filter in file |
| `bloom_filter_size` | `u32` | Size of bloom filter in bytes |
| `index_offset` | `u64` | Byte offset of index block in file |
| `index_size` | `u32` | Size of index block in bytes |
| `created_at_ms` | `u64` | Creation timestamp (milliseconds since epoch) |
| `sequence_range` | `(u64, u64)` | `(min_sequence, max_sequence)` of entries |
| `record_id_count` | `u64` | Approximate number of distinct record IDs |
| `run_id` | `Option<String>` | Run identifier for L1+ fragments (None for L0) |
| `fragment_index` | `Option<u32>` | Fragment position within a run (None for L0) |
| `dedup_block_size` | `u32` | Size of dedup block in bytes |

Implements: `Clone`, `Debug`, `PartialEq`, `Eq`, `Serialize`, `Deserialize`

**Conversion from SstInfo:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `from_sst_info` | `(info: &SstInfo, sequence_range: (u64, u64), record_id_count: u64, created_at_ms: u64) -> Self` | Converts SSTableWriter output to manifest metadata. Extracts min_key/max_key as raw bytes from CompositeKey. |

**Key range methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `contains_key` | `(&self, key: &CompositeKey) -> bool` | Returns true if key falls within `[min_key, max_key]` range |
| `overlaps` | `(&self, other: &SSTableMeta) -> bool` | Returns true if key ranges overlap |
| `overlaps_range` | `(&self, start: &[u8], end: &[u8]) -> bool` | Returns true if key range overlaps with `[start, end]` |
| `sst_path` | `(&self, namespace: &str, level: Level) -> String` | Constructs the StorageBackend path: `flushdb/{namespace}/sstables/{level}/{id}.sst` or `flushdb/{namespace}/sstables/{level}/{run_id}/{frag}.sst` |

### 1.3 Level

An enum representing SSTable levels.

```
L0 = 0
L1 = 1
L2 = 2
L3 = 3
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `as_str` | `(&self) -> &'static str` | Returns `"L0"`, `"L1"`, `"L2"`, `"L3"` |
| `from_str` | `(s: &str) -> FlushResult<Self>` | Parses level string |
| `as_u8` | `(&self) -> u8` | Returns 0-3 |
| `next` | `(&self) -> Option<Self>` | Returns the next deeper level (L3 returns None) |
| `is_bottom` | `(&self) -> bool` | Returns true for L3 |
| `max_size_bytes` | `(&self) -> u64` | L0: N/A (count-based), L1: 256 MB, L2: 2.56 GB, L3: 25.6 GB |
| `is_overlapping` | `(&self) -> bool` | Returns true for L0 only |

Implements: `Clone`, `Copy`, `Debug`, `PartialEq`, `Eq`, `PartialOrd`, `Ord`, `Hash`, `Serialize`, `Deserialize`

Custom serialization: serializes as `"L0"`, `"L1"`, etc.

### 1.4 BlobFileMeta

Metadata for large value blob files (reserved for future use, but the manifest structure includes it).

| Field | Type | Description |
|-------|------|-------------|
| `id` | `String` | Blob file identifier |
| `size_bytes` | `u64` | Total blob file size |
| `live_bytes` | `u64` | Bytes still referenced by live SSTables |
| `entry_count` | `u64` | Number of entries in blob file |
| `referenced_by_ssts` | `Vec<String>` | SSTable IDs that reference this blob |

### 1.5 Manifest

The top-level manifest structure. Serialized to JSON.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `format_version` | `u32` | Always `1` for now |
| `manifest_id` | `ManifestId` | Current manifest version |
| `writer_epoch` | `u64` | Writer epoch for fencing |
| `compactor_epoch` | `u64` | Compactor epoch for fencing |
| `namespace` | `String` | Namespace this manifest belongs to |
| `created_at_ms` | `u64` | When this manifest version was created |
| `last_flushed_sequence` | `u64` | Highest sequence number that has been flushed to SSTable |
| `levels` | `BTreeMap<Level, Vec<SSTableMeta>>` | SSTables at each level |
| `blob_files` | `Vec<BlobFileMeta>` | Blob files (reserved for future) |
| `tombstone_compaction_watermarks` | `BTreeMap<Level, u64>` | Per-level watermark timestamps |
| `previous_manifest_id` | `ManifestId` | ID of the manifest this was derived from |
| `is_snapshot` | `bool` | Whether this is a full snapshot (for manifest compaction) |

**Methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new_empty` | `(namespace: &str) -> Self` | Creates an initial empty manifest with format_version=1, manifest_id=ZERO, all levels empty |
| `sstables_at_level` | `(&self, level: Level) -> &[SSTableMeta]` | Returns SSTables at the given level |
| `l0_count` | `(&self) -> usize` | Returns number of L0 SSTables |
| `level_size_bytes` | `(&self, level: Level) -> u64` | Sum of size_bytes for all SSTables at level |
| `total_sstable_count` | `(&self) -> usize` | Total SSTables across all levels |
| `all_sstable_ids` | `(&self) -> Vec<&str>` | All SSTable IDs across all levels |
| `find_overlapping` | `(&self, level: Level, start: &[u8], end: &[u8]) -> Vec<&SSTableMeta>` | Returns SSTables at level whose key range overlaps [start, end] |
| `find_sstable_for_key` | `(&self, level: Level, key: &CompositeKey) -> Option<&SSTableMeta>` | For non-overlapping levels (L1-L3): binary search to find the one SSTable whose range contains the key |
| `serialize` | `(&self) -> FlushResult<Bytes>` | JSON serialization with pretty-print |
| `deserialize` | `(data: &[u8]) -> FlushResult<Self>` | JSON deserialization with validation |

### 1.6 ManifestUpdate

Describes a change to apply to a manifest (used by flush, compaction, GC).

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `trigger` | `ManifestUpdateTrigger` | `Flush`, `Compaction`, or `GC` |
| `add_sstables` | `Vec<(Level, SSTableMeta)>` | SSTables to add |
| `remove_sstables` | `Vec<(Level, String)>` | SSTable IDs to remove (level, id) |
| `new_last_flushed_sequence` | `Option<u64>` | Updated last_flushed_sequence (flush only) |
| `writer_epoch` | `u64` | Writer's current epoch |
| `compactor_epoch` | `u64` | Compactor's current epoch |

**ManifestUpdateTrigger enum:** `Flush`, `Compaction`, `GC`

| Method | Signature | Behavior |
|--------|-----------|----------|
| `apply` | `(&self, current: &Manifest) -> FlushResult<Manifest>` | Creates a new manifest by applying this update to the current one. Increments manifest_id, sets previous_manifest_id, adds/removes SSTables, updates last_flushed_sequence if set. Returns error if any remove target doesn't exist. |

### 1.7 ManifestConfig

Configuration for manifest behavior.

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `snapshot_interval` | `u64` | `100` | Create snapshot every N manifest versions |
| `max_manifest_size` | `usize` | `16 * 1024 * 1024` | Force snapshot if manifest exceeds this size |
| `pruning_batch_size` | `usize` | `50` | Number of old manifests to delete per pruning pass |
| `base_path` | `String` | `"flushdb"` | Root path prefix in StorageBackend |

### 1.8 S3 Path Helpers

Free functions for constructing StorageBackend paths:

| Function | Signature | Output |
|----------|-----------|--------|
| `manifest_path` | `(base: &str, namespace: &str, id: &ManifestId) -> String` | `"{base}/{namespace}/manifests/{id}"` |
| `manifest_prefix` | `(base: &str, namespace: &str) -> String` | `"{base}/{namespace}/manifests/"` |
| `l0_sst_path` | `(base: &str, namespace: &str, sst_id: &str) -> String` | `"{base}/{namespace}/sstables/L0/{sst_id}.sst"` |
| `run_fragment_path` | `(base: &str, namespace: &str, level: Level, run_id: &str, fragment_index: u32) -> String` | `"{base}/{namespace}/sstables/{level}/{run_id}/frag-{fragment_index:04}.sst"` |

---

## Tests

**File:** `crates/flushdb-engine/tests/manifest_types_tests.rs`

### ManifestId Tests
| Test | What It Validates |
|------|-------------------|
| `test_manifest_id_to_path_string` | `ManifestId(42)` → `"00000000000000000042"` |
| `test_manifest_id_from_path_string` | `"00000000000000000042"` → `ManifestId(42)` |
| `test_manifest_id_from_path_string_invalid` | Non-numeric strings, wrong length → error |
| `test_manifest_id_next` | `ManifestId(5).next()` → `ManifestId(6)` |
| `test_manifest_id_ordering` | `ManifestId(1) < ManifestId(2)`, lexicographic string order matches numeric order |
| `test_manifest_id_serde_roundtrip` | JSON serialize/deserialize preserves 20-digit string format |
| `test_manifest_id_zero_sentinel` | `ManifestId::ZERO` is `ManifestId(0)` |

### SSTableMeta Tests
| Test | What It Validates |
|------|-------------------|
| `test_sstable_meta_serde_roundtrip` | JSON round-trip preserves all fields including binary min_key/max_key as base64 |
| `test_sstable_meta_contains_key` | Key within range → true, key outside → false, boundary keys → true |
| `test_sstable_meta_overlaps` | Overlapping ranges → true, disjoint → false, adjacent → false |
| `test_sstable_meta_sst_path_l0` | L0 path: `flushdb/ns/sstables/L0/id.sst` |
| `test_sstable_meta_sst_path_l1_fragment` | L1 path: `flushdb/ns/sstables/L1/run-id/frag-0000.sst` |
| `test_sstable_meta_from_sst_info` | Conversion from SstInfo populates all fields correctly |

### Level Tests
| Test | What It Validates |
|------|-------------------|
| `test_level_ordering` | `L0 < L1 < L2 < L3` |
| `test_level_next` | `L0.next()` → `Some(L1)`, `L3.next()` → `None` |
| `test_level_is_bottom` | Only L3 returns true |
| `test_level_is_overlapping` | Only L0 returns true |
| `test_level_max_size_bytes` | L1=256MB, L2=2.56GB, L3=25.6GB |
| `test_level_serde_roundtrip` | Serializes as string `"L0"`, not integer |

### Manifest Tests
| Test | What It Validates |
|------|-------------------|
| `test_manifest_new_empty` | All levels empty, manifest_id=ZERO, format_version=1 |
| `test_manifest_serde_roundtrip` | Full manifest with SSTables at multiple levels round-trips through JSON |
| `test_manifest_l0_count` | Correct count after adding SSTables |
| `test_manifest_level_size_bytes` | Sums size_bytes correctly |
| `test_manifest_find_overlapping` | Returns only SSTables whose key ranges overlap the query |
| `test_manifest_find_sstable_for_key_binary_search` | For L1+, finds the correct SSTable via binary search |
| `test_manifest_find_sstable_for_key_miss` | Key not covered by any SSTable → None |
| `test_manifest_all_sstable_ids` | Returns IDs from all levels |

### ManifestUpdate Tests
| Test | What It Validates |
|------|-------------------|
| `test_manifest_update_add_sstables` | Apply adds SSTables to correct levels, increments manifest_id |
| `test_manifest_update_remove_sstables` | Apply removes specified SSTables |
| `test_manifest_update_remove_nonexistent` | Removing an ID that doesn't exist → error |
| `test_manifest_update_sets_previous_id` | `previous_manifest_id` set to current manifest's ID |
| `test_manifest_update_flush_sequence` | `new_last_flushed_sequence` is applied when set |
| `test_manifest_update_add_and_remove` | Simultaneous add + remove (compaction pattern) works |

### Path Helper Tests
| Test | What It Validates |
|------|-------------------|
| `test_manifest_path_format` | Correct path construction |
| `test_l0_sst_path_format` | Correct L0 path |
| `test_run_fragment_path_format` | Correct run fragment path with zero-padded index |

---

## Done When

- [ ] ManifestId formats as 20-digit zero-padded string and round-trips through JSON
- [ ] SSTableMeta stores all fields from STORAGE_DESIGN.md §8.1 including binary keys
- [ ] Level enum has correct size limits and overlapping/bottom-level semantics
- [ ] Manifest serializes to human-readable JSON matching the design doc format
- [ ] ManifestUpdate.apply correctly adds/removes SSTables and increments version
- [ ] Path helpers produce correct StorageBackend paths per §23
- [ ] All tests pass
