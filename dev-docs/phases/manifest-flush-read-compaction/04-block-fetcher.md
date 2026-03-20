# Task 4: BlockFetcher Trait & SSTable Metadata Cache

**Crate:** `flushdb-engine`
**File:** `src/block_fetcher.rs`, `src/sstable_handle.rs`
**Depends on:** Nothing (uses existing SSTable types from Phase 4)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §11.4 (S3 GET Reduction), §11.5 (S3 Byte-Range Read Protocol), Phase 5 Future Work (BlockFetcher trait for P6 cache seam)

---

## Goal

Create the abstraction layer between the read path and block-level I/O. The read path should never call `StorageBackend::get_range` directly — instead, it calls through a `BlockFetcher` trait that Phase 6's cache layer can wrap. Also implement `SSTableHandle`, an in-memory handle to an open SSTable that keeps bloom filters and index blocks pinned in memory for the SSTable's lifetime.

---

## What to Build

### 4.1 BlockFetcher Trait

The pluggable interface for fetching data blocks from SSTables. The direct implementation calls `StorageBackend::get_range`. Phase 6 wraps this with a cache layer.

```
#[async_trait]
trait BlockFetcher: Send + Sync {
    async fn fetch_block(&self, sst_path: &str, offset: u64, size: u32, compression: CompressionType) -> FlushResult<Vec<BlockEntry>>;
    async fn fetch_raw_block(&self, sst_path: &str, offset: u64, size: u32) -> FlushResult<Bytes>;
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `fetch_block` | `async (&self, sst_path: &str, offset: u64, size: u32, compression: CompressionType) -> FlushResult<Vec<BlockEntry>>` | Fetches a data block by byte range, decompresses, decodes into `Vec<BlockEntry>` |
| `fetch_raw_block` | `async (&self, sst_path: &str, offset: u64, size: u32) -> FlushResult<Bytes>` | Fetches raw bytes without decoding (used for bloom/index loading) |

### 4.2 DirectBlockFetcher

The concrete implementation that reads directly from `StorageBackend`:

```
DirectBlockFetcher<B: StorageBackend>
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(backend: B) -> Self` | Wraps a StorageBackend |

Implements `BlockFetcher`:
- `fetch_block`: calls `backend.get_range(sst_path, offset, size as u64)`, then `decode_block(data, compression)`
- `fetch_raw_block`: calls `backend.get_range(sst_path, offset, size as u64)`, returns raw bytes

### 4.3 SSTableHandle

An in-memory handle to an open SSTable. Holds pre-loaded metadata (bloom filter, index block, footer) so that point reads and range scans don't need to re-fetch metadata on every access.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `meta` | `SSTableMeta` | Manifest metadata for this SSTable |
| `path` | `String` | Full StorageBackend path |
| `footer` | `SstFooter` | Parsed footer |
| `bloom_filter` | `FilterBlock` | In-memory bloom filter |
| `index_block` | `IndexBlock` | In-memory sparse index |

**Methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `async (meta: SSTableMeta, path: String, fetcher: &dyn BlockFetcher) -> FlushResult<Self>` | Fetches footer (last 80 bytes via `fetch_raw_block`), then bloom filter and index block. Parses all three. Stores them in memory. |
| `may_contain_record` | `(&self, record_id: &[u8]) -> bool` | Checks bloom filter for record_id |
| `find_block_for_key` | `(&self, key: &CompositeKey) -> Option<&IndexEntry>` | Binary search in index block |
| `block_count` | `(&self) -> usize` | Number of data blocks |
| `key_range` | `(&self) -> (&CompositeKey, &CompositeKey)` | Min/max key from index block |
| `overlaps` | `(&self, start: &CompositeKey, end: &CompositeKey) -> bool` | Key range overlap check |
| `get_block` | `async (&self, block_index: usize, fetcher: &dyn BlockFetcher) -> FlushResult<Vec<BlockEntry>>` | Fetches and decodes a specific data block by index using the index entry's offset/size |
| `get` | `async (&self, key: &CompositeKey, fetcher: &dyn BlockFetcher) -> FlushResult<Option<BlockEntry>>` | Point lookup: bloom check → index lookup → block fetch → linear scan within block |
| `scan` | `async (&self, start: &CompositeKey, end: Option<&CompositeKey>, fetcher: &dyn BlockFetcher) -> FlushResult<Vec<BlockEntry>>` | Range scan: find start block → fetch blocks → collect entries in range |

### 4.4 LevelState

In-memory representation of all open SSTables at a given level, built from manifest metadata.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `level` | `Level` | Which level |
| `handles` | `Vec<SSTableHandle>` | Open SSTable handles, sorted by min_key for L1+ |

**Methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open_all` | `async (level: Level, metas: &[SSTableMeta], namespace: &str, config: &ManifestConfig, fetcher: &dyn BlockFetcher) -> FlushResult<Self>` | Opens all SSTables at a level concurrently. For L1+, sorts handles by min_key. |
| `find_candidates_for_key` | `(&self, key: &CompositeKey) -> Vec<&SSTableHandle>` | For L0: returns all handles that pass bloom filter. For L1+: binary search on key ranges, returns at most one. |
| `find_candidates_for_range` | `(&self, start: &CompositeKey, end: &CompositeKey) -> Vec<&SSTableHandle>` | Returns all handles whose key range overlaps [start, end] |
| `handle_count` | `(&self) -> usize` | Number of open handles |

### 4.5 Trait Does NOT Include

- **No caching** — that's Phase 6. `DirectBlockFetcher` fetches every time.
- **No coalesced block fetches** — that's a Phase 6 optimization. Each `fetch_block` is a single `get_range`.
- **No prefetch** — no readahead logic. Each block is fetched on demand.

---

## Tests

**File:** `crates/flushdb-engine/tests/block_fetcher_tests.rs`

### DirectBlockFetcher Tests
| Test | What It Validates |
|------|-------------------|
| `test_fetch_block_decodes_correctly` | Write SSTable via SSTableWriter, fetch a block by offset/size, verify decoded entries match originals |
| `test_fetch_raw_block_returns_bytes` | Raw fetch returns exact bytes without decoding |
| `test_fetch_block_nonexistent_path` | Fetching from missing path → `NotFound` error |
| `test_fetch_block_invalid_offset` | Offset beyond file size → error |

### SSTableHandle Tests
| Test | What It Validates |
|------|-------------------|
| `test_handle_open_loads_metadata` | Open a handle → bloom filter, index block, footer all loaded |
| `test_handle_bloom_filter_check` | `may_contain_record` returns true for keys in SSTable, can return false for absent keys |
| `test_handle_point_lookup_hit` | `get` finds existing key |
| `test_handle_point_lookup_miss` | `get` returns None for absent key |
| `test_handle_point_lookup_bloom_miss` | Key not in bloom filter → returns None without fetching data block |
| `test_handle_scan_full_range` | `scan` with no end returns all entries |
| `test_handle_scan_bounded_range` | `scan` with start/end returns only entries in range |
| `test_handle_key_range` | `key_range` returns correct min/max |

### LevelState Tests
| Test | What It Validates |
|------|-------------------|
| `test_level_state_l0_returns_all_bloom_matches` | L0 returns all handles that pass bloom filter (overlapping level) |
| `test_level_state_l1_binary_search` | L1+ returns at most one handle via binary search |
| `test_level_state_l1_key_miss` | Key not covered by any L1 SSTable → empty result |
| `test_level_state_range_candidates` | `find_candidates_for_range` returns all overlapping handles |
| `test_level_state_open_all_concurrent` | Opening many SSTables concurrently succeeds |

---

## Done When

- [ ] `BlockFetcher` trait defined with `fetch_block` and `fetch_raw_block`
- [ ] `DirectBlockFetcher` implements `BlockFetcher` via `StorageBackend::get_range`
- [ ] `SSTableHandle` loads bloom filter, index block, and footer on open
- [ ] Point lookup on handle: bloom check → index lookup → block fetch → scan
- [ ] Range scan on handle fetches correct blocks and filters by key range
- [ ] `LevelState` manages sorted handles for efficient key/range lookup
- [ ] L0 returns all bloom-matching handles; L1+ uses binary search
- [ ] All tests pass
