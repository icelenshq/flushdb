# Task 3: PinnedMetadataCache — Index and Filter Pinning

**Crate:** `flushdb-engine`
**File:** `src/cache/pinned_metadata.rs`
**Depends on:** Task 1 (CacheConfig)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §12 (bloom/ribbon filters, sparse indexes pinned in DRAM), Phase 6 §4 (Index and Filter Pinning)

---

## Goal

Provide a shared, long-lived cache for bloom filters and index blocks that persists across SSTableHandle lifecycles. Currently, each `SSTableHandle::open()` fetches bloom filter, index block, and footer from StorageBackend — 3 GETs per SSTable. When levels are rebuilt after compaction (`Engine::rebuild_levels`), handles are recreated and metadata is re-fetched. The pinned metadata cache eliminates these redundant fetches: metadata is loaded once per SSTable and released only when the SSTable is removed from the manifest.

---

## What to Build

### 3.1 PinnedMetadata

Metadata for a single SSTable, loaded once and pinned in DRAM:

```
PinnedMetadata {
    bloom_filter: FilterBlock,
    index_block: IndexBlock,
    footer: SstFooter,
    estimated_size_bytes: u64,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(bloom: FilterBlock, index: IndexBlock, footer: SstFooter) -> Self` | Computes estimated_size_bytes from bloom serialized size + index entry count * ~50 bytes + 80 bytes footer |

### 3.2 PinnedMetadataCache

A `HashMap<String, PinnedMetadata>` keyed by SSTable ID:

```
PinnedMetadataCache {
    entries: HashMap<String, PinnedMetadata>,
    total_size_bytes: u64,
    max_entries: usize,
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: &CacheConfig) -> Self` | Creates cache with `max_entries` from `config.pinned_metadata_capacity` |
| `pin` | `(&mut self, sst_id: String, metadata: PinnedMetadata)` | Stores metadata for the SSTable. If an entry for this ID already exists, replaces it. If at capacity, does NOT evict — the caller must release first (metadata should never be silently evicted). |
| `get` | `(&self, sst_id: &str) -> Option<&PinnedMetadata>` | Returns pinned metadata for the SSTable, or None if not pinned |
| `release` | `(&mut self, sst_id: &str) -> bool` | Removes metadata for the SSTable. Returns true if it was present. Updates `total_size_bytes`. |
| `release_batch` | `(&mut self, sst_ids: &[String])` | Removes metadata for multiple SSTables. More efficient than calling `release` in a loop. |
| `contains` | `(&self, sst_id: &str) -> bool` | Returns true if the SSTable's metadata is pinned |
| `entry_count` | `(&self) -> usize` | Number of currently pinned SSTables |
| `total_size_bytes` | `(&self) -> u64` | Total estimated memory used by pinned metadata |
| `pinned_sst_ids` | `(&self) -> Vec<&str>` | Returns all currently pinned SSTable IDs |

### 3.3 SSTableHandle Integration Point

`SSTableHandle::open()` currently makes 3 `fetch_raw_block` calls to load footer, bloom filter, and index block. With `PinnedMetadataCache`, the flow becomes:

1. Check `PinnedMetadataCache::get(sst_id)`
2. **Hit:** Use pinned bloom, index, footer — skip all 3 fetches
3. **Miss:** Fetch footer, bloom, index via fetcher (current behavior), then call `PinnedMetadataCache::pin(sst_id, metadata)` to store for future use

This integration is wired in Task 9 (Engine Integration). This task only builds the data structure.

### 3.4 Design Decisions

- **Not using moka for metadata:** Bloom filters and index blocks should NEVER be evicted by W-TinyLFU. They are critical for every SSTable access (bloom filter eliminates SSTables, index locates blocks). Evicting them would cause 3 extra StorageBackend GETs before any data read. A simple `HashMap` with explicit lifecycle management is correct here.
- **No capacity-based eviction:** The `max_entries` field is a safety bound, not an LRU. If the engine has more open SSTables than `max_entries`, new pins fail silently (the SSTable falls back to fetching metadata on each open). This should be rare — default capacity of 1000 SSTables covers most deployments.
- **`release_batch` for compaction:** Compaction removes multiple SSTables at once. Batch release avoids multiple lock acquisitions in future concurrent designs.
- **Separate from BlockCache:** Data block cache uses W-TinyLFU for scan resistance. Metadata must never compete with data blocks for cache capacity. Keeping them separate ensures metadata is always available regardless of data cache pressure.

### 3.5 Prerequisite Changes

Before this task can compile, the following existing types need `Clone` derives added:

- `FilterBlock` in `src/sstable/bloom_filter.rs` — add `#[derive(Clone)]`
- `BloomFilter` (inner struct of `FilterBlock`) — add `Clone` to its derives
- `IndexBlock` in `src/sstable/index_block.rs` — add `#[derive(Clone)]` (`IndexEntry` already derives `Clone`)

These are safe additions — all inner fields are owned types that support `Clone`.

### 3.6 Future-Proofing

- **Ribbon filters (future):** The cache stores `FilterBlock` which is an enum. When ribbon filters are added as a new variant, the pinning mechanism works unchanged.
- **Per-partition caches (Phase 7):** Each partition will manage its own set of SSTables. The `PinnedMetadataCache` can be per-partition with no API changes.

---

## Tests

**File:** `crates/flushdb-engine/tests/pinned_metadata_tests.rs`

### Basic Pin/Get/Release
| Test | What It Validates |
|------|-------------------|
| `test_pin_and_get` | Pin metadata for SST "A", get returns the same bloom/index/footer |
| `test_get_not_pinned` | Get for SST that was never pinned returns None |
| `test_release` | Pin then release SST "A", get returns None |
| `test_release_nonexistent` | Release for unknown SST returns false, no panic |
| `test_pin_overwrites` | Pin SST "A" twice with different metadata, get returns the latest |
| `test_contains` | Pin SST "A", contains("A") returns true, contains("B") returns false |

### Batch Operations
| Test | What It Validates |
|------|-------------------|
| `test_release_batch` | Pin 5 SSTables, release_batch 3 of them, verify only 2 remain |
| `test_release_batch_with_nonexistent` | release_batch with some unknown IDs — known ones removed, no panic |
| `test_pinned_sst_ids` | Pin 3 SSTables, pinned_sst_ids returns all 3 |

### Size Tracking
| Test | What It Validates |
|------|-------------------|
| `test_total_size_increases_on_pin` | Pin 3 SSTables, total_size_bytes > 0 and grows with each pin |
| `test_total_size_decreases_on_release` | Pin then release, total_size_bytes decreases accordingly |
| `test_entry_count` | Pin 5, release 2, entry_count is 3 |

### Capacity Bounds
| Test | What It Validates |
|------|-------------------|
| `test_at_capacity_pin_replaces_existing` | At max_entries, pinning an existing SST ID succeeds (overwrite) |
| `test_beyond_capacity_new_pin_rejected` | At max_entries, pinning a new SST ID when at capacity does not panic (returns gracefully) |

### Metadata Content Correctness
| Test | What It Validates |
|------|-------------------|
| `test_bloom_filter_usable_after_pin` | Pin real bloom filter, get it back, `maybe_contains()` returns correct results |
| `test_index_block_usable_after_pin` | Pin real index block, get it back, `find_block()` returns correct index entries |

---

## Done When

- [ ] `PinnedMetadataCache` stores bloom filters, index blocks, and footers per SSTable ID
- [ ] Metadata is never evicted by W-TinyLFU — it uses explicit lifecycle management
- [ ] `release` and `release_batch` correctly free metadata and update size tracking
- [ ] Size tracking accurately reflects total pinned metadata memory
- [ ] Bloom filters and index blocks retrieved from cache are functionally identical to freshly deserialized ones
- [ ] All tests pass
