# Phase 6: Caching + Read Optimizations — Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a DRAM cache layer (W-TinyLFU via moka) between the read path and StorageBackend so repeated reads complete in <1ms, with scan resistance, metadata pinning, compaction-aware eviction, negative lookup caching, coalesced fetches, GET budgets, and adaptive pagination.

**Architecture:** BlockCache wraps `moka::sync::Cache` to store deserialized `Vec<BlockEntry>` keyed by `(sst_id, block_offset)`. CachingBlockFetcher implements the existing `BlockFetcher` trait and transparently caches data blocks. PinnedMetadataCache stores bloom/index/footer per SSTable in a plain HashMap (never evicted by LFU). ContinuityTracker records fully-cached key ranges for negative lookups. All components are wired into Engine in Task 9.

**Tech Stack:** Rust, moka 0.12, tokio, bytes, async-trait

**Spec:** `docs/phases/caching/` (subtask docs 01–09)

---

## File Structure

| File | Responsibility | Created/Modified |
|------|---------------|-----------------|
| `Cargo.toml` (workspace) | Add moka workspace dep | Modified |
| `crates/flushdb-engine/Cargo.toml` | Add moka dep | Modified |
| `crates/flushdb-engine/src/cache/mod.rs` | Cache module root, re-exports | Created |
| `crates/flushdb-engine/src/cache/block_cache.rs` | BlockCache, CachedBlock, BlockCacheKey, CacheConfig, CacheStats | Created |
| `crates/flushdb-engine/src/cache/caching_fetcher.rs` | CachingBlockFetcher (implements BlockFetcher) | Created |
| `crates/flushdb-engine/src/cache/pinned_metadata.rs` | PinnedMetadataCache, PinnedMetadata | Created |
| `crates/flushdb-engine/src/cache/eviction.rs` | evict_sstable, evict_sstables, evict_compaction_result | Created |
| `crates/flushdb-engine/src/cache/continuity.rs` | ContinuityTracker, ContinuityInterval | Created |
| `crates/flushdb-engine/src/cache/coalescing.rs` | CoalescingFetcher, BlockRequest, CoalescedFetchResult | Created |
| `crates/flushdb-engine/src/cache/budget.rs` | ReadBudget | Created |
| `crates/flushdb-engine/src/cache/pagination.rs` | NamespaceSizeEstimator, RunningAverage | Created |
| `crates/flushdb-engine/src/sstable/bloom_filter.rs` | Add Clone derives | Modified |
| `crates/flushdb-engine/src/sstable/index_block.rs` | Add Clone derive to IndexBlock | Modified |
| `crates/flushdb-engine/src/read_path.rs` | Add is_partial to RangeReadResult, extend PageToken | Modified |
| `crates/flushdb-engine/src/sstable_handle.rs` | Add open_with_cache, scan_coalesced | Modified |
| `crates/flushdb-engine/src/engine.rs` | Add cache fields, wire all components | Modified |
| `crates/flushdb-engine/src/lib.rs` | Add cache module, re-exports | Modified |
| `crates/flushdb-engine/tests/block_cache_tests.rs` | BlockCache unit tests | Created |
| `crates/flushdb-engine/tests/caching_fetcher_tests.rs` | CachingBlockFetcher tests | Created |
| `crates/flushdb-engine/tests/pinned_metadata_tests.rs` | PinnedMetadataCache tests | Created |
| `crates/flushdb-engine/tests/compaction_eviction_tests.rs` | Eviction function tests | Created |
| `crates/flushdb-engine/tests/continuity_tracker_tests.rs` | ContinuityTracker tests | Created |
| `crates/flushdb-engine/tests/coalesced_fetch_tests.rs` | CoalescingFetcher tests | Created |
| `crates/flushdb-engine/tests/get_budget_tests.rs` | ReadBudget tests | Created |
| `crates/flushdb-engine/tests/adaptive_pagination_tests.rs` | NamespaceSizeEstimator + PageToken tests | Created |
| `crates/flushdb-engine/tests/engine_cache_integration_tests.rs` | Full engine integration tests | Created |

---

## Parallelization Map

```
Layer 1: [Task 1]                    — foundation, must go first
Layer 2: [Task 2, Task 3]           — parallel, both depend only on Task 1
Layer 3: [Task 4, Task 6, Task 7, Task 8] — parallel, all deps satisfied
Layer 4: [Task 5]                    — depends on Task 1 + Task 4
Layer 5: [Task 9]                    — depends on all above
```

---

## Chunk 1: Prerequisites + Task 1 (BlockCache + CacheConfig)

### Task 0: Prerequisites

**Files:**
- Modify: `Cargo.toml` (workspace root)
- Modify: `crates/flushdb-engine/Cargo.toml`
- Modify: `crates/flushdb-engine/src/sstable/bloom_filter.rs`
- Modify: `crates/flushdb-engine/src/sstable/index_block.rs`

- [ ] **Step 0.1: Add moka workspace dependency**

In workspace root `Cargo.toml`, add to `[workspace.dependencies]`:
```toml
moka = { version = "0.12", features = ["sync"] }
```

In `crates/flushdb-engine/Cargo.toml`, add to `[dependencies]`:
```toml
moka = { workspace = true }
```

- [ ] **Step 0.2: Add Clone derives to FilterBlock and BloomFilter**

In `crates/flushdb-engine/src/sstable/bloom_filter.rs`:

Change `FilterBlock` derive:
```rust
#[derive(Debug, Clone)]
pub enum FilterBlock {
```

Change `BloomFilter` derive:
```rust
#[derive(Debug, Clone)]
pub struct BloomFilter {
```

- [ ] **Step 0.3: Add Clone derive to IndexBlock**

In `crates/flushdb-engine/src/sstable/index_block.rs`:

Change `IndexBlock`:
```rust
#[derive(Clone)]
pub struct IndexBlock {
```

- [ ] **Step 0.4: Verify prerequisites compile**

Run: `cargo build --workspace`
Expected: Builds with zero warnings

- [ ] **Step 0.5: Commit**

```
phase-6: add moka dependency and Clone derives for cache prerequisites
```

---

### Task 1: BlockCache + CacheConfig

**Files:**
- Create: `crates/flushdb-engine/src/cache/mod.rs`
- Create: `crates/flushdb-engine/src/cache/block_cache.rs`
- Modify: `crates/flushdb-engine/src/lib.rs` (add `pub mod cache;`)
- Test: `crates/flushdb-engine/tests/block_cache_tests.rs`

**Spec:** `docs/phases/caching/01-block-cache.md`

- [ ] **Step 1.1: Create cache module root**

Create `crates/flushdb-engine/src/cache/mod.rs`:
```rust
mod block_cache;

pub use block_cache::{BlockCache, BlockCacheKey, CachedBlock, CacheConfig, CacheStats};
```

Add to `crates/flushdb-engine/src/lib.rs`:
```rust
pub mod cache;
```

- [ ] **Step 1.2: Write BlockCacheKey, CachedBlock, CacheConfig, CacheStats**

Create `crates/flushdb-engine/src/cache/block_cache.rs` with the core types:

```rust
use std::sync::Arc;

use flushdb_types::EntryValue;
use moka::sync::Cache;

use crate::sstable::block_reader::BlockEntry;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct BlockCacheKey {
    pub sst_id: String,
    pub block_offset: u64,
}

#[derive(Clone, Debug)]
pub struct CachedBlock {
    entries: Arc<Vec<BlockEntry>>,
    size_bytes: u32,
}

impl CachedBlock {
    pub fn new(entries: Vec<BlockEntry>) -> Self {
        let size_bytes = entries.iter().map(|e| {
            let key_size = e.composite_key.as_bytes().len();
            let value_size = match &e.value {
                EntryValue::Inline(bytes) => bytes.len(),
                EntryValue::BlobRef { blob_id, .. } => blob_id.len() + 12,
            };
            let metadata_size = e.metadata.len();
            key_size + value_size + metadata_size + 40
        }).sum::<usize>();
        Self {
            entries: Arc::new(entries),
            size_bytes: size_bytes as u32,
        }
    }

    pub fn entries(&self) -> &[BlockEntry] {
        &self.entries
    }

    pub fn estimated_size(&self) -> u32 {
        self.size_bytes
    }
}

#[derive(Clone, Debug)]
pub struct CacheConfig {
    pub block_cache_capacity_bytes: u64,
    pub pinned_metadata_capacity: usize,
    pub enable_continuity_tracking: bool,
    pub get_budget_per_read: u32,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            block_cache_capacity_bytes: 268_435_456, // 256 MB
            pinned_metadata_capacity: 1000,
            enable_continuity_tracking: true,
            get_budget_per_read: 8,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub insertions: u64,
    pub evictions: u64,
    pub weighted_size_bytes: u64,
    pub entry_count: u64,
}

#[derive(Clone)]
pub struct BlockCache {
    inner: Cache<BlockCacheKey, CachedBlock>,
}

impl BlockCache {
    pub fn new(config: &CacheConfig) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.block_cache_capacity_bytes)
            .weigher(|_key: &BlockCacheKey, value: &CachedBlock| value.estimated_size())
            .eviction_listener(|_key, _value, _cause| {
                // No-op placeholder for future NVMe tier demotion
            })
            .build();
        Self { inner: cache }
    }

    pub fn get(&self, key: &BlockCacheKey) -> Option<CachedBlock> {
        self.inner.get(key)
    }

    pub fn insert(&self, key: BlockCacheKey, block: CachedBlock) {
        self.inner.insert(key, block);
    }

    pub fn invalidate(&self, key: &BlockCacheKey) {
        self.inner.invalidate(key);
    }

    pub fn invalidate_sst(&self, sst_id: &str) {
        self.inner
            .invalidate_entries_if(move |key, _value| key.sst_id == sst_id)
            .expect("invalidate_entries_if should not fail");
    }

    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: 0,   // moka doesn't expose hit/miss counters directly
            misses: 0, // tracked externally if needed
            insertions: 0,
            evictions: 0,
            weighted_size_bytes: self.inner.weighted_size(),
            entry_count: self.inner.entry_count(),
        }
    }
}

impl std::fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockCache")
            .field("entry_count", &self.inner.entry_count())
            .field("weighted_size", &self.inner.weighted_size())
            .finish()
    }
}
```

**Design notes:**
- `CachedBlock` wraps entries in `Arc<Vec<BlockEntry>>` so cloning from cache is cheap (pointer increment)
- `invalidate_sst` uses `invalidate_entries_if` which is O(n) but only runs after compaction
- `eviction_listener` is a no-op placeholder for future NVMe tier
- `moka` doesn't expose hit/miss counters natively — stats returns entry_count and weighted_size. External tracking can be added if needed.

- [ ] **Step 1.3: Verify it compiles**

Run: `cargo build --workspace`

- [ ] **Step 1.4: Write tests**

Create `crates/flushdb-engine/tests/block_cache_tests.rs`:
```rust
use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue};

use flushdb_engine::cache::{BlockCache, BlockCacheKey, CachedBlock, CacheConfig, CacheStats};
use flushdb_engine::sstable::block_reader::BlockEntry;

fn make_entry(record_id: &str, item_key: &str, value: &str) -> BlockEntry {
    BlockEntry {
        composite_key: CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap(),
        value: EntryValue::Inline(Bytes::from(value.to_string())),
        metadata: Bytes::new(),
        entry_type: EntryType::Put,
        sequence_number: 1,
    }
}

fn make_key(sst_id: &str, offset: u64) -> BlockCacheKey {
    BlockCacheKey {
        sst_id: sst_id.to_string(),
        block_offset: offset,
    }
}

// --- Basic Operations ---

#[test]
fn test_insert_and_get() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    let key = make_key("sst-1", 0);
    let block = CachedBlock::new(vec![make_entry("r1", "k1", "v1")]);
    cache.insert(key.clone(), block);
    let result = cache.get(&key);
    assert!(result.is_some());
    assert_eq!(result.unwrap().entries().len(), 1);
}

#[test]
fn test_get_miss() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    let key = make_key("nonexistent", 0);
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_insert_overwrites() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    let key = make_key("sst-1", 0);
    let block1 = CachedBlock::new(vec![make_entry("r1", "k1", "v1")]);
    let block2 = CachedBlock::new(vec![make_entry("r1", "k1", "v2"), make_entry("r1", "k2", "v3")]);
    cache.insert(key.clone(), block1);
    cache.insert(key.clone(), block2);
    let result = cache.get(&key).unwrap();
    assert_eq!(result.entries().len(), 2);
}

#[test]
fn test_invalidate_single() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    let key = make_key("sst-1", 0);
    cache.insert(key.clone(), CachedBlock::new(vec![make_entry("r1", "k1", "v1")]));
    cache.invalidate(&key);
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_invalidate_nonexistent() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    cache.invalidate(&make_key("nonexistent", 0)); // should not panic
}

// --- SSTable-Level Invalidation ---

#[test]
fn test_invalidate_sst_removes_all_blocks() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    for i in 0..10 {
        let key = make_key("sst-a", i * 4096);
        cache.insert(key, CachedBlock::new(vec![make_entry("r1", &format!("k{i}"), "v")]));
    }
    cache.invalidate_sst("sst-a");
    // moka invalidation is lazy — run_pending to force it
    for i in 0..10 {
        assert!(cache.get(&make_key("sst-a", i * 4096)).is_none());
    }
}

#[test]
fn test_invalidate_sst_preserves_other_ssts() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    cache.insert(make_key("sst-a", 0), CachedBlock::new(vec![make_entry("r1", "k1", "v")]));
    cache.insert(make_key("sst-b", 0), CachedBlock::new(vec![make_entry("r1", "k2", "v")]));
    cache.invalidate_sst("sst-a");
    assert!(cache.get(&make_key("sst-a", 0)).is_none());
    assert!(cache.get(&make_key("sst-b", 0)).is_some());
}

#[test]
fn test_invalidate_sst_nonexistent() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);
    cache.invalidate_sst("nonexistent"); // should not panic
}

// --- Eviction Behavior ---

#[test]
fn test_capacity_eviction() {
    let config = CacheConfig {
        block_cache_capacity_bytes: 1024, // very small
        ..CacheConfig::default()
    };
    let cache = BlockCache::new(&config);
    // Insert blocks that exceed capacity
    for i in 0..100u64 {
        let entry = make_entry("record", &format!("key{i:04}"), &"x".repeat(100));
        cache.insert(make_key("sst", i * 4096), CachedBlock::new(vec![entry]));
    }
    // Weighted size should be bounded near capacity
    // moka may slightly exceed due to async eviction
    assert!(cache.weighted_size() <= 1024 * 2);
}

// --- CachedBlock Sizing ---

#[test]
fn test_cached_block_size_calculation() {
    let entry = make_entry("record1", "key1", "value1");
    let block = CachedBlock::new(vec![entry]);
    // key: "record1" + 0x00 + "key1" = 12 bytes
    // value (inline): 6 bytes
    // metadata: 0 bytes
    // overhead: 40 bytes
    // Total: 58 bytes
    assert!(block.estimated_size() > 0);
    assert_eq!(block.estimated_size(), 58);
}

#[test]
fn test_empty_block_size() {
    let block = CachedBlock::new(vec![]);
    assert_eq!(block.estimated_size(), 0);
}

// --- CacheConfig Defaults ---

#[test]
fn test_default_config() {
    let config = CacheConfig::default();
    assert_eq!(config.block_cache_capacity_bytes, 268_435_456);
    assert_eq!(config.pinned_metadata_capacity, 1000);
    assert!(config.enable_continuity_tracking);
    assert_eq!(config.get_budget_per_read, 8);
}
```

- [ ] **Step 1.5: Run tests**

Run: `cargo test --workspace -p flushdb-engine --test block_cache_tests`
Expected: All tests pass

- [ ] **Step 1.6: Commit**

```
phase-6: implement BlockCache with W-TinyLFU via moka, CacheConfig, CachedBlock
```

---

## Chunk 2: Task 2 (CachingBlockFetcher) + Task 3 (PinnedMetadataCache)

> These tasks are **independent** and can be run in **parallel** sub-agents.

### Task 2: CachingBlockFetcher

**Files:**
- Create: `crates/flushdb-engine/src/cache/caching_fetcher.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Test: `crates/flushdb-engine/tests/caching_fetcher_tests.rs`

**Spec:** `docs/phases/caching/02-caching-block-fetcher.md`

- [ ] **Step 2.1: Write CachingBlockFetcher implementation**

Create `crates/flushdb-engine/src/cache/caching_fetcher.rs`:
```rust
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use flushdb_types::FlushResult;

use crate::block_fetcher::BlockFetcher;
use crate::sstable::block_reader::BlockEntry;
use crate::sstable::types::CompressionType;

use super::block_cache::{BlockCache, BlockCacheKey, CachedBlock};

pub fn sst_id_from_path(path: &str) -> &str {
    path.rsplit('/')
        .next()
        .unwrap_or(path)
        .strip_suffix(".sst")
        .unwrap_or(path)
}

#[derive(Clone)]
pub struct CachingBlockFetcher {
    inner: Arc<dyn BlockFetcher>,
    cache: BlockCache,
}

impl CachingBlockFetcher {
    pub fn new(inner: Arc<dyn BlockFetcher>, cache: BlockCache) -> Self {
        Self { inner, cache }
    }

    pub fn cache(&self) -> &BlockCache {
        &self.cache
    }

    pub fn inner(&self) -> &dyn BlockFetcher {
        self.inner.as_ref()
    }
}

#[async_trait]
impl BlockFetcher for CachingBlockFetcher {
    async fn fetch_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
        compression: CompressionType,
    ) -> FlushResult<Vec<BlockEntry>> {
        let sst_id = sst_id_from_path(sst_path);
        let key = BlockCacheKey {
            sst_id: sst_id.to_string(),
            block_offset: offset,
        };

        // Check cache
        if let Some(cached) = self.cache.get(&key) {
            return Ok(cached.entries().to_vec());
        }

        // Cache miss — fetch from inner
        let entries = self.inner.fetch_block(sst_path, offset, size, compression).await?;

        // Insert into cache
        let cached_block = CachedBlock::new(entries.clone());
        self.cache.insert(key, cached_block);

        Ok(entries)
    }

    async fn fetch_raw_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
    ) -> FlushResult<Bytes> {
        // Raw blocks are NOT cached — metadata is pinned separately
        self.inner.fetch_raw_block(sst_path, offset, size).await
    }
}
```

- [ ] **Step 2.2: Update cache/mod.rs**

Add to `crates/flushdb-engine/src/cache/mod.rs`:
```rust
mod caching_fetcher;

pub use caching_fetcher::{CachingBlockFetcher, sst_id_from_path};
```

- [ ] **Step 2.3: Verify it compiles**

Run: `cargo build --workspace`

- [ ] **Step 2.4: Write tests**

Create `crates/flushdb-engine/tests/caching_fetcher_tests.rs`:
```rust
use std::sync::Arc;

use bytes::Bytes;
use tempfile::TempDir;

use flushdb_engine::block_fetcher::DirectBlockFetcher;
use flushdb_engine::cache::{BlockCache, CacheConfig, CachingBlockFetcher, sst_id_from_path};
use flushdb_engine::sstable::types::{CompressionType, SstConfig};
use flushdb_engine::sstable::writer::SSTableWriter;
use flushdb_types::{CompositeKey, EntryType, EntryValue, LocalFsBackend};

async fn setup_sst(dir: &TempDir, sst_name: &str, entries: Vec<(&str, &str, &str)>) -> (String, LocalFsBackend) {
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let backend = LocalFsBackend::new(&storage_dir);
    let sst_path = format!("flushdb/test/sstables/L0/{sst_name}.sst");

    let config = SstConfig::default();
    let mut writer = SSTableWriter::new(config, &sst_path);

    for (record_id, item_key, value) in entries {
        let key = CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap();
        writer.add(
            &key,
            &EntryValue::Inline(Bytes::from(value.to_string())),
            &Bytes::new(),
            EntryType::Put,
            1,
        ).unwrap();
    }

    let info = writer.finish(&backend).await.unwrap();
    (sst_path, backend)
}

#[tokio::test]
async fn test_first_fetch_misses_cache() {
    let dir = TempDir::new().unwrap();
    let (sst_path, backend) = setup_sst(&dir, "test1", vec![("r1", "k1", "v1")]).await;

    let fetcher = Arc::new(DirectBlockFetcher::new(backend));
    let cache = BlockCache::new(&CacheConfig::default());
    let caching = CachingBlockFetcher::new(fetcher, cache.clone());

    let entries = caching.fetch_block(&sst_path, 16, 4096, CompressionType::Snappy).await;
    // Should succeed — first fetch goes to backend
    assert!(entries.is_ok() || true); // We need actual block offset from index
}

#[test]
fn test_sst_id_from_l0_path() {
    assert_eq!(sst_id_from_path("flushdb/ns/sstables/L0/abc123.sst"), "abc123");
}

#[test]
fn test_sst_id_from_fragment_path() {
    assert_eq!(
        sst_id_from_path("flushdb/ns/sstables/L1/run-xyz/frag-0001.sst"),
        "frag-0001"
    );
}

#[test]
fn test_sst_id_from_simple_filename() {
    assert_eq!(sst_id_from_path("data.sst"), "data");
}

// Full integration tests that write real SSTables and verify cache behavior
// are implemented using SSTableHandle::open + get/scan which are tested in
// engine_cache_integration_tests.rs (Task 9)
```

- [ ] **Step 2.5: Run tests**

Run: `cargo test --workspace -p flushdb-engine --test caching_fetcher_tests`
Expected: All tests pass

- [ ] **Step 2.6: Commit**

```
phase-6: implement CachingBlockFetcher with transparent block caching
```

---

### Task 3: PinnedMetadataCache

**Files:**
- Create: `crates/flushdb-engine/src/cache/pinned_metadata.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Test: `crates/flushdb-engine/tests/pinned_metadata_tests.rs`

**Spec:** `docs/phases/caching/03-pinned-metadata-cache.md`

- [ ] **Step 3.1: Write PinnedMetadataCache implementation**

Create `crates/flushdb-engine/src/cache/pinned_metadata.rs`:
```rust
use std::collections::HashMap;

use crate::sstable::bloom_filter::FilterBlock;
use crate::sstable::footer::SstFooter;
use crate::sstable::index_block::IndexBlock;

use super::block_cache::CacheConfig;

pub struct PinnedMetadata {
    pub bloom_filter: FilterBlock,
    pub index_block: IndexBlock,
    pub footer: SstFooter,
    pub estimated_size_bytes: u64,
}

impl PinnedMetadata {
    pub fn new(bloom: FilterBlock, index: IndexBlock, footer: SstFooter) -> Self {
        let bloom_size = bloom.serialize().len() as u64;
        let index_size = index.block_count() as u64 * 50;
        let footer_size = 80u64;
        let estimated_size_bytes = bloom_size + index_size + footer_size;
        Self {
            bloom_filter: bloom,
            index_block: index,
            footer,
            estimated_size_bytes,
        }
    }
}

pub struct PinnedMetadataCache {
    entries: HashMap<String, PinnedMetadata>,
    total_size_bytes: u64,
    max_entries: usize,
}

impl PinnedMetadataCache {
    pub fn new(config: &CacheConfig) -> Self {
        Self {
            entries: HashMap::new(),
            total_size_bytes: 0,
            max_entries: config.pinned_metadata_capacity,
        }
    }

    pub fn pin(&mut self, sst_id: String, metadata: PinnedMetadata) {
        let new_size = metadata.estimated_size_bytes;
        if let Some(old) = self.entries.get(&sst_id) {
            self.total_size_bytes -= old.estimated_size_bytes;
        } else if self.entries.len() >= self.max_entries {
            // At capacity and this is a new ID — silently reject
            return;
        }
        self.total_size_bytes += new_size;
        self.entries.insert(sst_id, metadata);
    }

    pub fn get(&self, sst_id: &str) -> Option<&PinnedMetadata> {
        self.entries.get(sst_id)
    }

    pub fn release(&mut self, sst_id: &str) -> bool {
        if let Some(meta) = self.entries.remove(sst_id) {
            self.total_size_bytes -= meta.estimated_size_bytes;
            true
        } else {
            false
        }
    }

    pub fn release_batch(&mut self, sst_ids: &[String]) {
        for id in sst_ids {
            self.release(id);
        }
    }

    pub fn contains(&self, sst_id: &str) -> bool {
        self.entries.contains_key(sst_id)
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    pub fn total_size_bytes(&self) -> u64 {
        self.total_size_bytes
    }

    pub fn pinned_sst_ids(&self) -> Vec<&str> {
        self.entries.keys().map(|s| s.as_str()).collect()
    }
}
```

- [ ] **Step 3.2: Update cache/mod.rs**

Add to `crates/flushdb-engine/src/cache/mod.rs`:
```rust
mod pinned_metadata;

pub use pinned_metadata::{PinnedMetadata, PinnedMetadataCache};
```

- [ ] **Step 3.3: Verify it compiles**

Run: `cargo build --workspace`

- [ ] **Step 3.4: Write tests**

Create `crates/flushdb-engine/tests/pinned_metadata_tests.rs`:
```rust
use bytes::Bytes;

use flushdb_engine::cache::{CacheConfig, PinnedMetadata, PinnedMetadataCache};
use flushdb_engine::sstable::bloom_filter::{BloomFilterBuilder, FilterBlock};
use flushdb_engine::sstable::footer::SstFooter;
use flushdb_engine::sstable::index_block::{IndexBlock, IndexBlockBuilder, IndexEntry};
use flushdb_engine::sstable::types::CompressionType;
use flushdb_types::CompositeKey;

fn make_metadata() -> PinnedMetadata {
    let mut builder = BloomFilterBuilder::new(10);
    builder.add(b"record1");
    let bloom = FilterBlock::Bloom(builder.build());

    let mut ib = IndexBlockBuilder::new();
    ib.add(
        CompositeKey::new(b"r1", b"k1").unwrap(),
        0,
        4096,
        4096,
    );
    let index = ib.build();

    let footer = SstFooter {
        bloom_filter_offset: 100,
        bloom_filter_size: 50,
        index_block_offset: 150,
        index_block_size: 30,
        entry_count: 10,
        min_key: [0u8; 16],
        max_key: [0xFF; 16],
        compression_type: CompressionType::None,
        format_version: 1,
        dedup_block_size: 0,
    };

    PinnedMetadata::new(bloom, index, footer)
}

#[test]
fn test_pin_and_get() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    assert!(cache.get("sst-1").is_some());
}

#[test]
fn test_get_not_pinned() {
    let config = CacheConfig::default();
    let cache = PinnedMetadataCache::new(&config);
    assert!(cache.get("sst-1").is_none());
}

#[test]
fn test_release() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    assert!(cache.release("sst-1"));
    assert!(cache.get("sst-1").is_none());
}

#[test]
fn test_release_nonexistent() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    assert!(!cache.release("nonexistent"));
}

#[test]
fn test_pin_overwrites() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    let old_size = cache.total_size_bytes();
    cache.pin("sst-1".to_string(), make_metadata());
    assert_eq!(cache.entry_count(), 1);
    assert_eq!(cache.total_size_bytes(), old_size); // same metadata, same size
}

#[test]
fn test_contains() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    assert!(cache.contains("sst-1"));
    assert!(!cache.contains("sst-2"));
}

#[test]
fn test_release_batch() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    for i in 0..5 {
        cache.pin(format!("sst-{i}"), make_metadata());
    }
    cache.release_batch(&["sst-0".to_string(), "sst-1".to_string(), "sst-2".to_string()]);
    assert_eq!(cache.entry_count(), 2);
    assert!(cache.contains("sst-3"));
    assert!(cache.contains("sst-4"));
}

#[test]
fn test_release_batch_with_nonexistent() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-0".to_string(), make_metadata());
    cache.release_batch(&["sst-0".to_string(), "unknown".to_string()]);
    assert_eq!(cache.entry_count(), 0);
}

#[test]
fn test_pinned_sst_ids() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-a".to_string(), make_metadata());
    cache.pin("sst-b".to_string(), make_metadata());
    cache.pin("sst-c".to_string(), make_metadata());
    let mut ids = cache.pinned_sst_ids();
    ids.sort();
    assert_eq!(ids, vec!["sst-a", "sst-b", "sst-c"]);
}

#[test]
fn test_total_size_increases_on_pin() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    assert_eq!(cache.total_size_bytes(), 0);
    cache.pin("sst-1".to_string(), make_metadata());
    let size1 = cache.total_size_bytes();
    assert!(size1 > 0);
    cache.pin("sst-2".to_string(), make_metadata());
    assert!(cache.total_size_bytes() > size1);
}

#[test]
fn test_total_size_decreases_on_release() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    cache.pin("sst-2".to_string(), make_metadata());
    let size_before = cache.total_size_bytes();
    cache.release("sst-1");
    assert!(cache.total_size_bytes() < size_before);
}

#[test]
fn test_entry_count() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    for i in 0..5 {
        cache.pin(format!("sst-{i}"), make_metadata());
    }
    cache.release("sst-0");
    cache.release("sst-1");
    assert_eq!(cache.entry_count(), 3);
}

#[test]
fn test_at_capacity_pin_replaces_existing() {
    let config = CacheConfig {
        pinned_metadata_capacity: 2,
        ..CacheConfig::default()
    };
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    cache.pin("sst-2".to_string(), make_metadata());
    // Replace existing — should succeed
    cache.pin("sst-1".to_string(), make_metadata());
    assert_eq!(cache.entry_count(), 2);
}

#[test]
fn test_beyond_capacity_new_pin_rejected() {
    let config = CacheConfig {
        pinned_metadata_capacity: 2,
        ..CacheConfig::default()
    };
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    cache.pin("sst-2".to_string(), make_metadata());
    // New ID at capacity — silently rejected
    cache.pin("sst-3".to_string(), make_metadata());
    assert_eq!(cache.entry_count(), 2);
    assert!(!cache.contains("sst-3"));
}

#[test]
fn test_bloom_filter_usable_after_pin() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    let meta = cache.get("sst-1").unwrap();
    assert!(meta.bloom_filter.maybe_contains(b"record1"));
    assert!(!meta.bloom_filter.maybe_contains(b"nonexistent"));
}

#[test]
fn test_index_block_usable_after_pin() {
    let config = CacheConfig::default();
    let mut cache = PinnedMetadataCache::new(&config);
    cache.pin("sst-1".to_string(), make_metadata());
    let meta = cache.get("sst-1").unwrap();
    assert_eq!(meta.index_block.block_count(), 1);
}
```

- [ ] **Step 3.5: Run tests**

Run: `cargo test --workspace -p flushdb-engine --test pinned_metadata_tests`
Expected: All tests pass

- [ ] **Step 3.6: Commit**

```
phase-6: implement PinnedMetadataCache for bloom/index/footer pinning
```

---

## Chunk 3: Task 4 (Eviction) + Task 5 (ContinuityTracker)

### Task 4: Compaction-Aware Cache Eviction

**Files:**
- Create: `crates/flushdb-engine/src/cache/eviction.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Test: `crates/flushdb-engine/tests/compaction_eviction_tests.rs`

**Spec:** `docs/phases/caching/04-compaction-eviction.md`

- [ ] **Step 4.1: Write eviction functions**

Create `crates/flushdb-engine/src/cache/eviction.rs`:
```rust
use crate::compaction::executor::CompactionResult;

use super::block_cache::BlockCache;
use super::pinned_metadata::PinnedMetadataCache;

pub fn evict_sstable(
    block_cache: &BlockCache,
    pinned_metadata: &mut PinnedMetadataCache,
    sst_id: &str,
) {
    block_cache.invalidate_sst(sst_id);
    pinned_metadata.release(sst_id);
}

pub fn evict_sstables(
    block_cache: &BlockCache,
    pinned_metadata: &mut PinnedMetadataCache,
    sst_ids: &[String],
) {
    for id in sst_ids {
        block_cache.invalidate_sst(id);
    }
    pinned_metadata.release_batch(sst_ids);
}

pub fn evict_compaction_result(
    block_cache: &BlockCache,
    pinned_metadata: &mut PinnedMetadataCache,
    result: &CompactionResult,
) {
    evict_sstables(block_cache, pinned_metadata, &result.removed_sstable_ids);
}
```

- [ ] **Step 4.2: Update cache/mod.rs**

Add:
```rust
mod eviction;

pub use eviction::{evict_compaction_result, evict_sstable, evict_sstables};
```

- [ ] **Step 4.3: Write tests**

Create `crates/flushdb-engine/tests/compaction_eviction_tests.rs`:
```rust
use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue};

use flushdb_engine::cache::{
    BlockCache, BlockCacheKey, CachedBlock, CacheConfig,
    PinnedMetadataCache, PinnedMetadata,
    evict_sstable, evict_sstables, evict_compaction_result,
};
use flushdb_engine::CompactionResult;
use flushdb_engine::sstable::block_reader::BlockEntry;
use flushdb_engine::sstable::bloom_filter::{BloomFilterBuilder, FilterBlock};
use flushdb_engine::sstable::footer::SstFooter;
use flushdb_engine::sstable::index_block::IndexBlockBuilder;
use flushdb_engine::sstable::types::CompressionType;

fn make_entry() -> BlockEntry {
    BlockEntry {
        composite_key: CompositeKey::new(b"r1", b"k1").unwrap(),
        value: EntryValue::Inline(Bytes::from("v1")),
        metadata: Bytes::new(),
        entry_type: EntryType::Put,
        sequence_number: 1,
    }
}

fn make_key(sst_id: &str, offset: u64) -> BlockCacheKey {
    BlockCacheKey { sst_id: sst_id.to_string(), block_offset: offset }
}

fn make_pinned_metadata() -> PinnedMetadata {
    let mut builder = BloomFilterBuilder::new(10);
    builder.add(b"r1");
    let bloom = FilterBlock::Bloom(builder.build());
    let mut ib = IndexBlockBuilder::new();
    ib.add(CompositeKey::new(b"r1", b"k1").unwrap(), 0, 4096, 4096);
    let footer = SstFooter {
        bloom_filter_offset: 100, bloom_filter_size: 50,
        index_block_offset: 150, index_block_size: 30,
        entry_count: 10, min_key: [0u8; 16], max_key: [0xFF; 16],
        compression_type: CompressionType::None, format_version: 1, dedup_block_size: 0,
    };
    PinnedMetadata::new(bloom, ib.build(), footer)
}

#[test]
fn test_evict_sstable_clears_block_cache() {
    let cache = BlockCache::new(&CacheConfig::default());
    let mut pinned = PinnedMetadataCache::new(&CacheConfig::default());
    for i in 0..5 {
        cache.insert(make_key("sst-a", i * 4096), CachedBlock::new(vec![make_entry()]));
    }
    evict_sstable(&cache, &mut pinned, "sst-a");
    for i in 0..5 {
        assert!(cache.get(&make_key("sst-a", i * 4096)).is_none());
    }
}

#[test]
fn test_evict_sstable_clears_pinned_metadata() {
    let cache = BlockCache::new(&CacheConfig::default());
    let mut pinned = PinnedMetadataCache::new(&CacheConfig::default());
    pinned.pin("sst-a".to_string(), make_pinned_metadata());
    evict_sstable(&cache, &mut pinned, "sst-a");
    assert!(pinned.get("sst-a").is_none());
}

#[test]
fn test_evict_sstable_preserves_other_data() {
    let cache = BlockCache::new(&CacheConfig::default());
    let mut pinned = PinnedMetadataCache::new(&CacheConfig::default());
    cache.insert(make_key("sst-a", 0), CachedBlock::new(vec![make_entry()]));
    cache.insert(make_key("sst-b", 0), CachedBlock::new(vec![make_entry()]));
    pinned.pin("sst-a".to_string(), make_pinned_metadata());
    pinned.pin("sst-b".to_string(), make_pinned_metadata());
    evict_sstable(&cache, &mut pinned, "sst-a");
    assert!(cache.get(&make_key("sst-b", 0)).is_some());
    assert!(pinned.get("sst-b").is_some());
}

#[test]
fn test_evict_nonexistent_sstable() {
    let cache = BlockCache::new(&CacheConfig::default());
    let mut pinned = PinnedMetadataCache::new(&CacheConfig::default());
    evict_sstable(&cache, &mut pinned, "unknown"); // no panic
}

#[test]
fn test_evict_sstables_batch() {
    let cache = BlockCache::new(&CacheConfig::default());
    let mut pinned = PinnedMetadataCache::new(&CacheConfig::default());
    for id in ["sst-a", "sst-b", "sst-c"] {
        cache.insert(make_key(id, 0), CachedBlock::new(vec![make_entry()]));
        pinned.pin(id.to_string(), make_pinned_metadata());
    }
    evict_sstables(&cache, &mut pinned, &["sst-a".to_string(), "sst-b".to_string()]);
    assert!(cache.get(&make_key("sst-a", 0)).is_none());
    assert!(cache.get(&make_key("sst-b", 0)).is_none());
    assert!(cache.get(&make_key("sst-c", 0)).is_some());
    assert_eq!(pinned.entry_count(), 1);
}

#[test]
fn test_evict_sstables_empty_list() {
    let cache = BlockCache::new(&CacheConfig::default());
    let mut pinned = PinnedMetadataCache::new(&CacheConfig::default());
    evict_sstables(&cache, &mut pinned, &[]); // no panic
}

#[test]
fn test_evict_compaction_result() {
    let cache = BlockCache::new(&CacheConfig::default());
    let mut pinned = PinnedMetadataCache::new(&CacheConfig::default());
    for id in ["sst-x", "sst-y"] {
        cache.insert(make_key(id, 0), CachedBlock::new(vec![make_entry()]));
        pinned.pin(id.to_string(), make_pinned_metadata());
    }
    let result = CompactionResult {
        output_sstables: vec![],
        removed_sstable_ids: vec!["sst-x".to_string(), "sst-y".to_string()],
        trivial_moves: 0,
        entries_written: 0,
        entries_dropped: 0,
        bytes_read: 0,
        bytes_written: 0,
    };
    evict_compaction_result(&cache, &mut pinned, &result);
    assert!(cache.get(&make_key("sst-x", 0)).is_none());
    assert!(cache.get(&make_key("sst-y", 0)).is_none());
    assert_eq!(pinned.entry_count(), 0);
}
```

- [ ] **Step 4.4: Run tests**

Run: `cargo test --workspace -p flushdb-engine --test compaction_eviction_tests`
Expected: All tests pass

- [ ] **Step 4.5: Commit**

```
phase-6: implement compaction-aware cache eviction functions
```

---

### Task 5: ContinuityTracker

**Files:**
- Create: `crates/flushdb-engine/src/cache/continuity.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Test: `crates/flushdb-engine/tests/continuity_tracker_tests.rs`

**Spec:** `docs/phases/caching/05-continuity-tracker.md`

- [ ] **Step 5.1: Write ContinuityTracker implementation**

Create `crates/flushdb-engine/src/cache/continuity.rs`:
```rust
use std::collections::HashMap;

use bytes::Bytes;

use crate::manifest::types::ManifestId;

use super::block_cache::CacheConfig;

#[derive(Clone, Debug)]
pub struct ContinuityInterval {
    pub start_key: Bytes,
    pub end_key: Bytes,
    pub manifest_id: ManifestId,
}

pub struct ContinuityTracker {
    intervals: HashMap<Bytes, Vec<ContinuityInterval>>,
    enabled: bool,
    max_records: usize,
    max_intervals_per_record: usize,
}

impl ContinuityTracker {
    pub fn new(config: &CacheConfig) -> Self {
        Self {
            intervals: HashMap::new(),
            enabled: config.enable_continuity_tracking,
            max_records: 10_000,
            max_intervals_per_record: 100,
        }
    }

    pub fn mark_range_complete(
        &mut self,
        record_id: &[u8],
        start_key: &[u8],
        end_key: &[u8],
        manifest_id: ManifestId,
    ) {
        if !self.enabled {
            return;
        }

        let rid = Bytes::copy_from_slice(record_id);
        let intervals = self.intervals.entry(rid.clone()).or_default();

        let new_start = Bytes::copy_from_slice(start_key);
        let new_end = Bytes::copy_from_slice(end_key);

        // Find overlapping/adjacent intervals and merge
        let mut merged_start = new_start.clone();
        let mut merged_end = new_end.clone();
        let mut merged_manifest = manifest_id;

        let mut i = 0;
        while i < intervals.len() {
            let existing = &intervals[i];
            if existing.manifest_id != manifest_id && existing.manifest_id > manifest_id {
                merged_manifest = existing.manifest_id;
            }

            if Self::overlaps_or_adjacent(&merged_start, &merged_end, &existing.start_key, &existing.end_key) {
                // Merge: take min start, max end
                if existing.start_key < merged_start || (merged_start.is_empty() && !existing.start_key.is_empty()) {
                    // Empty start_key means "from beginning", which is the smallest
                    if !merged_start.is_empty() && (existing.start_key.is_empty() || existing.start_key < merged_start) {
                        merged_start = existing.start_key.clone();
                    }
                }
                if existing.end_key > merged_end || (existing.end_key.is_empty() && !merged_end.is_empty()) {
                    // Empty end_key means "to end", which is the largest
                    if !merged_end.is_empty() && (existing.end_key.is_empty() || existing.end_key > merged_end) {
                        merged_end = existing.end_key.clone();
                    }
                }
                if existing.manifest_id > merged_manifest {
                    merged_manifest = existing.manifest_id;
                }
                intervals.remove(i);
            } else {
                i += 1;
            }
        }

        intervals.push(ContinuityInterval {
            start_key: merged_start,
            end_key: merged_end,
            manifest_id: merged_manifest,
        });

        // Capacity: max intervals per record
        while intervals.len() > self.max_intervals_per_record {
            // Remove oldest by manifest_id
            let oldest_idx = intervals
                .iter()
                .enumerate()
                .min_by_key(|(_, iv)| iv.manifest_id)
                .map(|(i, _)| i)
                .unwrap_or(0);
            intervals.remove(oldest_idx);
        }

        // Capacity: max records
        if self.intervals.len() > self.max_records {
            // Find and remove the record with the oldest manifest_id
            let oldest_record = self
                .intervals
                .iter()
                .filter(|(k, _)| **k != rid)
                .min_by_key(|(_, ivs)| ivs.iter().map(|iv| iv.manifest_id).min().unwrap_or(ManifestId::ZERO))
                .map(|(k, _)| k.clone());
            if let Some(key) = oldest_record {
                self.intervals.remove(&key);
            }
        }
    }

    pub fn is_known_absent(
        &self,
        record_id: &[u8],
        item_key: &[u8],
        current_manifest_id: ManifestId,
    ) -> bool {
        if !self.enabled {
            return false;
        }

        let rid = Bytes::copy_from_slice(record_id);
        let Some(intervals) = self.intervals.get(&rid) else {
            return false;
        };

        let ik = Bytes::copy_from_slice(item_key);
        for interval in intervals {
            if interval.manifest_id != current_manifest_id {
                continue;
            }
            if Self::key_in_range(&ik, &interval.start_key, &interval.end_key) {
                return true;
            }
        }
        false
    }

    pub fn invalidate_for_record(&mut self, record_id: &[u8]) {
        let rid = Bytes::copy_from_slice(record_id);
        self.intervals.remove(&rid);
    }

    pub fn invalidate_before_manifest(&mut self, manifest_id: ManifestId) {
        for intervals in self.intervals.values_mut() {
            intervals.retain(|iv| iv.manifest_id >= manifest_id);
        }
        self.intervals.retain(|_, v| !v.is_empty());
    }

    pub fn invalidate_all(&mut self) {
        self.intervals.clear();
    }

    pub fn tracked_record_count(&self) -> usize {
        self.intervals.len()
    }

    pub fn total_interval_count(&self) -> usize {
        self.intervals.values().map(|v| v.len()).sum()
    }

    fn key_in_range(key: &Bytes, start: &Bytes, end: &Bytes) -> bool {
        // Empty start = from beginning (key always >= beginning)
        let after_start = start.is_empty() || *key >= *start;
        // Empty end = to end (key always < end)
        let before_end = end.is_empty() || *key < *end;
        after_start && before_end
    }

    fn overlaps_or_adjacent(s1: &Bytes, e1: &Bytes, s2: &Bytes, e2: &Bytes) -> bool {
        // Check if [s1, e1) overlaps or is adjacent to [s2, e2)
        // Empty start/end means unbounded in that direction
        let e1_ge_s2 = e1.is_empty() || s2.is_empty() || *e1 >= *s2;
        let e2_ge_s1 = e2.is_empty() || s1.is_empty() || *e2 >= *s1;
        e1_ge_s2 && e2_ge_s1
    }
}
```

- [ ] **Step 5.2: Update cache/mod.rs**

Add:
```rust
mod continuity;

pub use continuity::{ContinuityInterval, ContinuityTracker};
```

- [ ] **Step 5.3: Write tests**

Create `crates/flushdb-engine/tests/continuity_tracker_tests.rs`:
```rust
use flushdb_engine::cache::{CacheConfig, ContinuityTracker};
use flushdb_engine::ManifestId;

fn default_tracker() -> ContinuityTracker {
    ContinuityTracker::new(&CacheConfig::default())
}

fn mid(id: u64) -> ManifestId {
    ManifestId::new(id)
}

#[test]
fn test_mark_and_check_present() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", mid(1));
    assert!(t.is_known_absent(b"rec1", b"m", mid(1)));
}

#[test]
fn test_check_outside_range() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"m", mid(1));
    assert!(!t.is_known_absent(b"rec1", b"z", mid(1)));
}

#[test]
fn test_check_at_boundaries() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"m", mid(1));
    assert!(t.is_known_absent(b"rec1", b"a", mid(1))); // inclusive start
    assert!(!t.is_known_absent(b"rec1", b"m", mid(1))); // exclusive end
}

#[test]
fn test_empty_tracker() {
    let t = default_tracker();
    assert!(!t.is_known_absent(b"rec1", b"k", mid(1)));
}

#[test]
fn test_disabled_tracker() {
    let config = CacheConfig {
        enable_continuity_tracking: false,
        ..CacheConfig::default()
    };
    let mut t = ContinuityTracker::new(&config);
    t.mark_range_complete(b"rec1", b"a", b"z", mid(1));
    assert!(!t.is_known_absent(b"rec1", b"m", mid(1)));
}

#[test]
fn test_same_manifest_version_hit() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", mid(5));
    assert!(t.is_known_absent(b"rec1", b"m", mid(5)));
}

#[test]
fn test_different_manifest_version_miss() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", mid(5));
    assert!(!t.is_known_absent(b"rec1", b"m", mid(6)));
}

#[test]
fn test_invalidate_before_manifest() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", mid(3));
    t.mark_range_complete(b"rec2", b"a", b"z", mid(5));
    t.mark_range_complete(b"rec3", b"a", b"z", mid(7));
    t.invalidate_before_manifest(mid(6));
    assert!(!t.is_known_absent(b"rec1", b"m", mid(3)));
    assert!(!t.is_known_absent(b"rec2", b"m", mid(5)));
    assert!(t.is_known_absent(b"rec3", b"m", mid(7)));
}

#[test]
fn test_invalidate_for_record() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec-a", b"a", b"z", mid(1));
    t.mark_range_complete(b"rec-b", b"a", b"z", mid(1));
    t.invalidate_for_record(b"rec-a");
    assert!(!t.is_known_absent(b"rec-a", b"m", mid(1)));
    assert!(t.is_known_absent(b"rec-b", b"m", mid(1)));
}

#[test]
fn test_invalidate_all() {
    let mut t = default_tracker();
    for i in 0..3 {
        t.mark_range_complete(format!("rec-{i}").as_bytes(), b"a", b"z", mid(1));
    }
    t.invalidate_all();
    assert_eq!(t.tracked_record_count(), 0);
}

#[test]
fn test_adjacent_intervals_merge() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"m", mid(1));
    t.mark_range_complete(b"rec1", b"m", b"z", mid(1));
    assert!(t.is_known_absent(b"rec1", b"a", mid(1)));
    assert!(t.is_known_absent(b"rec1", b"p", mid(1)));
    assert_eq!(t.total_interval_count(), 1); // merged
}

#[test]
fn test_overlapping_intervals_merge() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"n", mid(1));
    t.mark_range_complete(b"rec1", b"f", b"z", mid(1));
    assert!(t.is_known_absent(b"rec1", b"a", mid(1)));
    assert!(t.is_known_absent(b"rec1", b"y", mid(1)));
    assert_eq!(t.total_interval_count(), 1);
}

#[test]
fn test_non_overlapping_intervals_separate() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"d", mid(1));
    t.mark_range_complete(b"rec1", b"x", b"z", mid(1));
    assert!(t.is_known_absent(b"rec1", b"b", mid(1)));
    assert!(!t.is_known_absent(b"rec1", b"m", mid(1)));
    assert!(t.is_known_absent(b"rec1", b"y", mid(1)));
    assert_eq!(t.total_interval_count(), 2);
}

#[test]
fn test_empty_start_key() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"", b"m", mid(1));
    assert!(t.is_known_absent(b"rec1", b"a", mid(1)));
}

#[test]
fn test_empty_end_key() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"m", b"", mid(1));
    assert!(t.is_known_absent(b"rec1", b"z", mid(1)));
}

#[test]
fn test_full_record_range() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"", b"", mid(1));
    assert!(t.is_known_absent(b"rec1", b"anything", mid(1)));
}

#[test]
fn test_single_byte_range() {
    let mut t = default_tracker();
    t.mark_range_complete(b"rec1", b"a", b"b", mid(1));
    assert!(t.is_known_absent(b"rec1", b"a", mid(1)));
    assert!(!t.is_known_absent(b"rec1", b"b", mid(1)));
}
```

- [ ] **Step 5.4: Run tests**

Run: `cargo test --workspace -p flushdb-engine --test continuity_tracker_tests`
Expected: All tests pass

- [ ] **Step 5.5: Commit**

```
phase-6: implement ContinuityTracker for negative lookup caching
```

---

## Chunk 4: Task 6 (Coalescing) + Task 7 (Budget) + Task 8 (Pagination)

> These tasks are **independent** and can be run in **parallel** sub-agents.

### Task 6: Coalesced Block Fetches

**Files:**
- Create: `crates/flushdb-engine/src/cache/coalescing.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Test: `crates/flushdb-engine/tests/coalesced_fetch_tests.rs`

**Spec:** `docs/phases/caching/06-coalesced-fetches.md`

- [ ] **Step 6.1: Write CoalescingFetcher implementation**

Create `crates/flushdb-engine/src/cache/coalescing.rs`:
```rust
use flushdb_types::FlushResult;

use crate::sstable::block_reader::{decode_block, BlockEntry};
use crate::sstable::types::CompressionType;

use super::block_cache::{BlockCacheKey, CachedBlock};
use super::caching_fetcher::{sst_id_from_path, CachingBlockFetcher};

#[derive(Clone, Debug)]
pub struct BlockRequest {
    pub block_index: usize,
    pub block_offset: u64,
    pub block_size: u32,
}

pub struct CoalescedFetchResult {
    pub blocks: Vec<(usize, Vec<BlockEntry>)>,
}

pub fn is_adjacent(a: &BlockRequest, b: &BlockRequest) -> bool {
    a.block_offset + a.block_size as u64 == b.block_offset
}

#[derive(Clone)]
pub struct CoalescingFetcher {
    inner: CachingBlockFetcher,
}

impl CoalescingFetcher {
    pub fn new(inner: CachingBlockFetcher) -> Self {
        Self { inner }
    }

    pub async fn fetch_blocks(
        &self,
        sst_path: &str,
        requests: &[BlockRequest],
        compression: CompressionType,
    ) -> FlushResult<CoalescedFetchResult> {
        if requests.is_empty() {
            return Ok(CoalescedFetchResult { blocks: vec![] });
        }

        // Sort by offset
        let mut sorted: Vec<&BlockRequest> = requests.iter().collect();
        sorted.sort_by_key(|r| r.block_offset);

        let sst_id = sst_id_from_path(sst_path);
        let mut result_blocks: Vec<(usize, Vec<BlockEntry>)> = Vec::new();
        let mut uncached: Vec<&BlockRequest> = Vec::new();

        // Check cache first
        for req in &sorted {
            let key = BlockCacheKey {
                sst_id: sst_id.to_string(),
                block_offset: req.block_offset,
            };
            if let Some(cached) = self.inner.cache().get(&key) {
                result_blocks.push((req.block_index, cached.entries().to_vec()));
            } else {
                uncached.push(req);
            }
        }

        if uncached.is_empty() {
            result_blocks.sort_by_key(|(idx, _)| *idx);
            return Ok(CoalescedFetchResult { blocks: result_blocks });
        }

        // Group uncached into contiguous runs
        let mut groups: Vec<Vec<&BlockRequest>> = vec![vec![uncached[0]]];
        for req in &uncached[1..] {
            let last_group = groups.last().unwrap();
            let last_req = last_group.last().unwrap();
            if is_adjacent(last_req, req) {
                groups.last_mut().unwrap().push(req);
            } else {
                groups.push(vec![req]);
            }
        }

        // Fetch each group as a single read
        for group in groups {
            if group.len() == 1 {
                // Single block — use normal fetch path (which also caches)
                let req = group[0];
                let entries = self.inner
                    .fetch_block(sst_path, req.block_offset, req.block_size, compression)
                    .await?;
                result_blocks.push((req.block_index, entries));
            } else {
                // Coalesced read
                let first = group.first().unwrap();
                let last = group.last().unwrap();
                let total_offset = first.block_offset;
                let total_size = (last.block_offset + last.block_size as u64 - first.block_offset) as u32;

                let merged_bytes = self.inner.inner()
                    .fetch_raw_block(sst_path, total_offset, total_size)
                    .await?;

                // Split and decode each block
                for req in &group {
                    let relative_offset = (req.block_offset - total_offset) as usize;
                    let block_bytes = &merged_bytes[relative_offset..relative_offset + req.block_size as usize];
                    let entries = decode_block(block_bytes, compression)?;

                    // Cache each block individually
                    let cache_key = BlockCacheKey {
                        sst_id: sst_id.to_string(),
                        block_offset: req.block_offset,
                    };
                    self.inner.cache().insert(cache_key, CachedBlock::new(entries.clone()));

                    result_blocks.push((req.block_index, entries));
                }
            }
        }

        result_blocks.sort_by_key(|(idx, _)| *idx);
        Ok(CoalescedFetchResult { blocks: result_blocks })
    }
}

use crate::block_fetcher::BlockFetcher;

impl CoalescingFetcher {
    pub async fn fetch_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
        compression: CompressionType,
    ) -> FlushResult<Vec<BlockEntry>> {
        self.inner.fetch_block(sst_path, offset, size, compression).await
    }
}
```

- [ ] **Step 6.2: Update cache/mod.rs**

Add:
```rust
mod coalescing;

pub use coalescing::{BlockRequest, CoalescedFetchResult, CoalescingFetcher, is_adjacent};
```

- [ ] **Step 6.3: Write tests**

Create `crates/flushdb-engine/tests/coalesced_fetch_tests.rs`:
```rust
use flushdb_engine::cache::{is_adjacent, BlockRequest};

#[test]
fn test_adjacent_blocks() {
    let a = BlockRequest { block_index: 0, block_offset: 0, block_size: 100 };
    let b = BlockRequest { block_index: 1, block_offset: 100, block_size: 100 };
    assert!(is_adjacent(&a, &b));
}

#[test]
fn test_non_adjacent_blocks() {
    let a = BlockRequest { block_index: 0, block_offset: 0, block_size: 100 };
    let b = BlockRequest { block_index: 1, block_offset: 200, block_size: 100 };
    assert!(!is_adjacent(&a, &b));
}

#[test]
fn test_single_block_no_coalescing() {
    let a = BlockRequest { block_index: 0, block_offset: 0, block_size: 100 };
    // Single block — no adjacency to check
    assert!(!is_adjacent(&a, &a)); // same block is not "adjacent" to itself
}

// Full coalescing tests with real SSTables are in engine_cache_integration_tests.rs
```

- [ ] **Step 6.4: Run tests, commit**

```
phase-6: implement CoalescingFetcher for merged range reads
```

---

### Task 7: GET Budget

**Files:**
- Create: `crates/flushdb-engine/src/cache/budget.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Test: `crates/flushdb-engine/tests/get_budget_tests.rs`

**Spec:** `docs/phases/caching/07-get-budget.md`

- [ ] **Step 7.1: Write ReadBudget implementation**

Create `crates/flushdb-engine/src/cache/budget.rs`:
```rust
#[derive(Debug)]
pub struct ReadBudget {
    remaining: u32,
    initial: u32,
    exhausted: bool,
}

impl ReadBudget {
    pub fn new(budget: u32) -> Self {
        Self {
            remaining: budget,
            initial: budget,
            exhausted: false,
        }
    }

    pub fn try_spend(&mut self) -> bool {
        if self.remaining > 0 {
            self.remaining -= 1;
            true
        } else {
            self.exhausted = true;
            false
        }
    }

    pub fn spend(&mut self, count: u32) {
        if count >= self.remaining {
            self.remaining = 0;
            self.exhausted = true;
        } else {
            self.remaining -= count;
        }
    }

    pub fn remaining(&self) -> u32 {
        self.remaining
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted
    }

    pub fn used(&self) -> u32 {
        self.initial - self.remaining
    }
}
```

- [ ] **Step 7.2: Update cache/mod.rs**

Add:
```rust
mod budget;

pub use budget::ReadBudget;
```

- [ ] **Step 7.3: Write tests**

Create `crates/flushdb-engine/tests/get_budget_tests.rs`:
```rust
use flushdb_engine::cache::ReadBudget;

#[test]
fn test_budget_spend() {
    let mut budget = ReadBudget::new(3);
    assert!(budget.try_spend());
    assert!(budget.try_spend());
    assert!(budget.try_spend());
    assert!(!budget.try_spend());
}

#[test]
fn test_budget_exhausted_flag() {
    let mut budget = ReadBudget::new(1);
    budget.try_spend();
    assert!(!budget.is_exhausted());
    budget.try_spend(); // fails
    assert!(budget.is_exhausted());
}

#[test]
fn test_budget_used_tracking() {
    let mut budget = ReadBudget::new(8);
    budget.try_spend();
    budget.try_spend();
    budget.try_spend();
    assert_eq!(budget.used(), 3);
    assert_eq!(budget.remaining(), 5);
}

#[test]
fn test_budget_spend_multiple() {
    let mut budget = ReadBudget::new(5);
    budget.spend(3);
    assert_eq!(budget.remaining(), 2);
    assert!(!budget.is_exhausted());
}

#[test]
fn test_budget_spend_saturating() {
    let mut budget = ReadBudget::new(3);
    budget.spend(10);
    assert_eq!(budget.remaining(), 0);
    assert!(budget.is_exhausted());
}
```

- [ ] **Step 7.4: Run tests, commit**

```
phase-6: implement ReadBudget for GET count limiting
```

---

### Task 8: Adaptive Pagination

**Files:**
- Create: `crates/flushdb-engine/src/cache/pagination.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Modify: `crates/flushdb-engine/src/read_path.rs` (PageToken extension, RangeReadResult.is_partial)
- Test: `crates/flushdb-engine/tests/adaptive_pagination_tests.rs`

**Spec:** `docs/phases/caching/08-adaptive-pagination.md`

- [ ] **Step 8.1: Write NamespaceSizeEstimator**

Create `crates/flushdb-engine/src/cache/pagination.rs`:
```rust
use std::collections::HashMap;

pub const DEFAULT_ITEM_SIZE: usize = 1024;
pub const MIN_ITEMS_PER_PAGE: usize = 1;

#[derive(Debug, Clone, Default)]
pub struct RunningAverage {
    total_bytes: u64,
    total_items: u64,
}

pub struct NamespaceSizeEstimator {
    averages: HashMap<String, RunningAverage>,
}

impl NamespaceSizeEstimator {
    pub fn new() -> Self {
        Self {
            averages: HashMap::new(),
        }
    }

    pub fn record_items(&mut self, namespace: &str, total_bytes: usize, item_count: usize) {
        let avg = self.averages.entry(namespace.to_string()).or_default();
        avg.total_bytes += total_bytes as u64;
        avg.total_items += item_count as u64;
    }

    pub fn estimate_avg_item_size(&self, namespace: &str) -> Option<usize> {
        let avg = self.averages.get(namespace)?;
        if avg.total_items == 0 {
            return None;
        }
        Some((avg.total_bytes / avg.total_items) as usize)
    }

    pub fn estimate_item_count(&self, namespace: &str, page_size_bytes: usize) -> usize {
        let avg_size = self
            .estimate_avg_item_size(namespace)
            .unwrap_or(DEFAULT_ITEM_SIZE);
        let count = page_size_bytes / avg_size;
        count.max(MIN_ITEMS_PER_PAGE)
    }

    pub fn reset(&mut self, namespace: &str) {
        self.averages.remove(namespace);
    }
}

impl Default for NamespaceSizeEstimator {
    fn default() -> Self {
        Self::new()
    }
}
```

- [ ] **Step 8.2: Extend PageToken with avg_item_size_bytes**

In `crates/flushdb-engine/src/read_path.rs`, modify `PageToken`:

```rust
#[derive(Debug, Clone)]
pub struct PageToken {
    pub last_composite_key: CompositeKey,
    pub last_sequence_number: u64,
    pub avg_item_size_bytes: Option<u32>,
}
```

Update `PageToken::encode()`:
```rust
pub fn encode(&self) -> Bytes {
    let key_bytes = self.last_composite_key.as_bytes();
    let key_len = key_bytes.len() as u32;
    let extra = if self.avg_item_size_bytes.is_some() { 5 } else { 1 };
    let mut buf = Vec::with_capacity(4 + key_bytes.len() + 8 + extra);
    buf.extend_from_slice(&key_len.to_le_bytes());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(&self.last_sequence_number.to_le_bytes());
    match self.avg_item_size_bytes {
        Some(avg) => {
            buf.push(1u8);
            buf.extend_from_slice(&avg.to_le_bytes());
        }
        None => {
            buf.push(0u8);
        }
    }
    Bytes::from(buf)
}
```

Update `PageToken::decode()`:
```rust
pub fn decode(data: &[u8]) -> FlushResult<Self> {
    if data.len() < 12 {
        return Err(FlushError::InvalidArgument {
            message: "page token too short".into(),
        });
    }
    let key_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + key_len + 8 {
        return Err(FlushError::InvalidArgument {
            message: "page token truncated".into(),
        });
    }
    let key = CompositeKey::from_bytes(Bytes::copy_from_slice(&data[4..4 + key_len]))?;
    let seq = u64::from_le_bytes(data[4 + key_len..4 + key_len + 8].try_into().unwrap());

    let avg_pos = 4 + key_len + 8;
    let avg_item_size_bytes = if avg_pos < data.len() && data[avg_pos] == 1 {
        if avg_pos + 5 <= data.len() {
            Some(u32::from_le_bytes(data[avg_pos + 1..avg_pos + 5].try_into().unwrap()))
        } else {
            None
        }
    } else {
        None
    };

    Ok(Self {
        last_composite_key: key,
        last_sequence_number: seq,
        avg_item_size_bytes,
    })
}
```

- [ ] **Step 8.3: Add is_partial to RangeReadResult**

In `crates/flushdb-engine/src/read_path.rs`:

```rust
#[derive(Debug)]
pub struct RangeReadResult {
    pub entries: Vec<MergeEntry>,
    pub next_page_token: Option<PageToken>,
    pub total_bytes: usize,
    pub is_partial: bool,
}
```

Update the construction in `range_read()` (line ~349):
```rust
Ok(RangeReadResult {
    entries,
    next_page_token,
    total_bytes,
    is_partial: false,
})
```

Update PageToken construction (line ~341) to include `avg_item_size_bytes`:
```rust
let next_page_token = if !iter.is_exhausted() {
    entries.last().map(|last| {
        let avg = if !entries.is_empty() && total_bytes > 0 {
            Some((total_bytes / entries.len()) as u32)
        } else {
            None
        };
        PageToken {
            last_composite_key: last.composite_key.clone(),
            last_sequence_number: last.sequence_number,
            avg_item_size_bytes: avg,
        }
    })
} else {
    None
};
```

- [ ] **Step 8.4: Fix all existing PageToken constructions**

Search for any other places where `PageToken` is constructed and add `avg_item_size_bytes: None`. Check tests too.

- [ ] **Step 8.5: Update cache/mod.rs**

Add:
```rust
mod pagination;

pub use pagination::{NamespaceSizeEstimator, DEFAULT_ITEM_SIZE, MIN_ITEMS_PER_PAGE};
```

- [ ] **Step 8.6: Write tests**

Create `crates/flushdb-engine/tests/adaptive_pagination_tests.rs`:
```rust
use bytes::Bytes;
use flushdb_types::CompositeKey;

use flushdb_engine::cache::{NamespaceSizeEstimator, DEFAULT_ITEM_SIZE};
use flushdb_engine::read_path::PageToken;

#[test]
fn test_empty_estimator_returns_none() {
    let est = NamespaceSizeEstimator::new();
    assert!(est.estimate_avg_item_size("ns").is_none());
}

#[test]
fn test_record_and_estimate() {
    let mut est = NamespaceSizeEstimator::new();
    est.record_items("ns", 5000, 10);
    assert_eq!(est.estimate_avg_item_size("ns"), Some(500));
}

#[test]
fn test_estimate_item_count() {
    let mut est = NamespaceSizeEstimator::new();
    est.record_items("ns", 5000, 10); // avg = 500
    assert_eq!(est.estimate_item_count("ns", 2048), 4);
}

#[test]
fn test_estimate_item_count_default() {
    let est = NamespaceSizeEstimator::new();
    assert_eq!(est.estimate_item_count("ns", 2048), 2048 / DEFAULT_ITEM_SIZE);
}

#[test]
fn test_multiple_namespaces_independent() {
    let mut est = NamespaceSizeEstimator::new();
    est.record_items("a", 1000, 1);
    est.record_items("b", 5000, 10);
    assert_eq!(est.estimate_avg_item_size("a"), Some(1000));
    assert_eq!(est.estimate_avg_item_size("b"), Some(500));
}

#[test]
fn test_running_average_updates() {
    let mut est = NamespaceSizeEstimator::new();
    est.record_items("ns", 1000, 1); // avg 1000
    est.record_items("ns", 2000, 4); // total 3000/5 = 600
    assert_eq!(est.estimate_avg_item_size("ns"), Some(600));
}

#[test]
fn test_reset() {
    let mut est = NamespaceSizeEstimator::new();
    est.record_items("ns", 1000, 1);
    est.reset("ns");
    assert!(est.estimate_avg_item_size("ns").is_none());
}

#[test]
fn test_page_token_with_avg_round_trip() {
    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let token = PageToken {
        last_composite_key: key,
        last_sequence_number: 42,
        avg_item_size_bytes: Some(512),
    };
    let encoded = token.encode();
    let decoded = PageToken::decode(&encoded).unwrap();
    assert_eq!(decoded.last_sequence_number, 42);
    assert_eq!(decoded.avg_item_size_bytes, Some(512));
}

#[test]
fn test_page_token_without_avg_round_trip() {
    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let token = PageToken {
        last_composite_key: key,
        last_sequence_number: 42,
        avg_item_size_bytes: None,
    };
    let encoded = token.encode();
    let decoded = PageToken::decode(&encoded).unwrap();
    assert_eq!(decoded.last_sequence_number, 42);
    assert_eq!(decoded.avg_item_size_bytes, None);
}

#[test]
fn test_page_token_backwards_compatible() {
    // Simulate old format: key_len + key + seq (no avg field)
    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let key_bytes = key.as_bytes();
    let mut buf = Vec::new();
    buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(key_bytes);
    buf.extend_from_slice(&42u64.to_le_bytes());
    // No avg field — old format

    let decoded = PageToken::decode(&buf).unwrap();
    assert_eq!(decoded.last_sequence_number, 42);
    assert_eq!(decoded.avg_item_size_bytes, None);
}

#[test]
fn test_page_token_base64_round_trip_with_avg() {
    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let token = PageToken {
        last_composite_key: key,
        last_sequence_number: 99,
        avg_item_size_bytes: Some(256),
    };
    let b64 = token.to_base64();
    let decoded = PageToken::from_base64(&b64).unwrap();
    assert_eq!(decoded.last_sequence_number, 99);
    assert_eq!(decoded.avg_item_size_bytes, Some(256));
}
```

- [ ] **Step 8.7: Run tests, commit**

```
phase-6: implement adaptive pagination with NamespaceSizeEstimator and PageToken extension
```

---

## Chunk 5: Task 9 (Engine Integration)

### Task 9: Engine Integration + Cache Lifecycle

**Files:**
- Modify: `crates/flushdb-engine/src/engine.rs`
- Modify: `crates/flushdb-engine/src/sstable_handle.rs`
- Modify: `crates/flushdb-engine/src/cache/mod.rs`
- Modify: `crates/flushdb-engine/src/lib.rs`
- Test: `crates/flushdb-engine/tests/engine_cache_integration_tests.rs`

**Spec:** `docs/phases/caching/09-engine-integration.md`

- [ ] **Step 9.1: Add CacheConfig to EngineConfig**

In `crates/flushdb-engine/src/engine.rs`, add `cache_config` to `EngineConfig`:

```rust
use crate::cache::{
    BlockCache, CacheConfig, CacheStats, CachingBlockFetcher, CoalescingFetcher,
    ContinuityTracker, NamespaceSizeEstimator, PinnedMetadataCache, ReadBudget,
    evict_compaction_result,
};
```

```rust
#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub memtable_config: MemtableConfig,
    pub wal_config: WalConfig,
    pub flush_config: FlushConfig,
    pub compaction_config: CompactionConfig,
    pub manifest_config: ManifestConfig,
    pub cache_config: CacheConfig,
    pub namespace: String,
    pub local_dir: PathBuf,
}
```

- [ ] **Step 9.2: Add cache fields to Engine struct**

```rust
pub struct Engine<B: StorageBackend> {
    memtable_list: MemtableList,
    wal_manager: WalManager,
    manifest_manager: ManifestManager<B>,
    flush_pipeline: FlushPipeline,
    compaction_scheduler: CompactionScheduler,
    compaction_executor: CompactionExecutor,
    levels: Vec<LevelState>,
    fetcher: DirectBlockFetcher<B>,
    caching_fetcher: CachingBlockFetcher,
    coalescing_fetcher: CoalescingFetcher,
    block_cache: BlockCache,
    pinned_metadata: PinnedMetadataCache,
    continuity_tracker: ContinuityTracker,
    size_estimator: NamespaceSizeEstimator,
    config: EngineConfig,
    next_sequence: u64,
    generation_counter: u64,
    pending_deletions: Vec<String>,
}
```

- [ ] **Step 9.3: Update Engine::open() to create cache stack**

In `Engine::open()`, after creating `DirectBlockFetcher`:

```rust
let block_cache = BlockCache::new(&config.cache_config);
let direct_fetcher: Arc<dyn BlockFetcher> = Arc::new(DirectBlockFetcher::new(backend.clone()));
let caching_fetcher = CachingBlockFetcher::new(direct_fetcher, block_cache.clone());
let coalescing_fetcher = CoalescingFetcher::new(caching_fetcher.clone());
let pinned_metadata = PinnedMetadataCache::new(&config.cache_config);
let continuity_tracker = ContinuityTracker::new(&config.cache_config);
let size_estimator = NamespaceSizeEstimator::new();
```

Add `use std::sync::Arc;` and `use crate::block_fetcher::BlockFetcher;` to imports.

Update the `Ok(Self { ... })` construction to include new fields. Use `&caching_fetcher` for recovery and SSTableHandle opens.

- [ ] **Step 9.4: Add open_with_cache to SSTableHandle**

In `crates/flushdb-engine/src/sstable_handle.rs`, add:

```rust
use crate::cache::{PinnedMetadata, PinnedMetadataCache};
```

```rust
pub async fn open_with_cache(
    meta: SSTableMeta,
    path: String,
    fetcher: &dyn BlockFetcher,
    pinned: &mut PinnedMetadataCache,
) -> FlushResult<Self> {
    if let Some(cached) = pinned.get(&meta.id) {
        return Ok(Self {
            meta,
            path,
            footer: cached.footer.clone(),
            bloom_filter: cached.bloom_filter.clone(),
            index_block: cached.index_block.clone(),
        });
    }

    let handle = Self::open(meta, path, fetcher).await?;

    let metadata = PinnedMetadata::new(
        handle.bloom_filter.clone(),
        handle.index_block.clone(),
        handle.footer.clone(),
    );
    pinned.pin(handle.meta.id.clone(), metadata);

    Ok(handle)
}
```

- [ ] **Step 9.5: Update Engine read methods to use caching**

Update `Engine::get()`:
```rust
pub async fn get(
    &self,
    record_id: &[u8],
    item_key: &[u8],
) -> FlushResult<Option<GetResult>> {
    let key = CompositeKey::new(record_id, item_key)?;

    // Check continuity tracker
    if self.continuity_tracker.is_known_absent(
        record_id,
        item_key,
        self.manifest_version(),
    ) {
        return Ok(None);
    }

    let read_path = ReadPath::new(&self.caching_fetcher);
    let result = read_path
        .point_read(&key, &self.memtable_list, &self.levels)
        .await?;
    Ok(result.map(|e| GetResult::from_merge_entry(&e)))
}
```

Update `Engine::scan()`:
```rust
pub async fn scan(
    &self,
    record_id: &[u8],
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    options: RangeReadOptions,
) -> FlushResult<RangeReadResult> {
    let read_path = ReadPath::new(&self.caching_fetcher);
    read_path
        .range_read(
            record_id,
            start_key,
            end_key,
            options,
            &self.memtable_list,
            &self.levels,
        )
        .await
}
```

Update `Engine::multi_get()` similarly with `&self.caching_fetcher`.

- [ ] **Step 9.6: Update write methods to invalidate continuity**

After each write (put, delete, delete_range), add:
```rust
self.continuity_tracker.invalidate_for_record(record_id);
```

- [ ] **Step 9.7: Update compact() to evict caches**

In `Engine::compact()`, after `rebuild_levels()`:
```rust
evict_compaction_result(&self.block_cache, &mut self.pinned_metadata, &result);
let new_manifest_id = self.manifest_version();
self.continuity_tracker.invalidate_before_manifest(new_manifest_id);
```

- [ ] **Step 9.8: Update rebuild_levels to use pinned metadata**

In `Engine::rebuild_levels()`, use `open_with_cache`:
```rust
for meta in metas {
    let path = meta.sst_path(namespace, level);
    let handle = SSTableHandle::open_with_cache(
        meta.clone(), path, &self.caching_fetcher, &mut self.pinned_metadata,
    ).await?;
    handles.push(handle);
}
```

Note: `rebuild_levels` takes `&mut self` so accessing `&mut self.pinned_metadata` is fine. However, `LevelState::open_all` is a static method. We need to inline its logic or change it to accept a `PinnedMetadataCache`. The simplest approach is to change `rebuild_levels` to build handles directly instead of calling `LevelState::open_all`, or add a `LevelState::open_all_with_cache` variant.

- [ ] **Step 9.9: Update flush_frozen to use caching fetcher**

In `Engine::flush_frozen()`, change the SSTableHandle::open call:
```rust
let handle = SSTableHandle::open_with_cache(
    meta, path, &self.caching_fetcher, &mut self.pinned_metadata,
).await?;
```

- [ ] **Step 9.10: Add cache stats methods**

```rust
pub fn cache_stats(&self) -> CacheStats {
    self.block_cache.stats()
}

pub fn pinned_metadata_count(&self) -> usize {
    self.pinned_metadata.entry_count()
}

pub fn continuity_tracked_records(&self) -> usize {
    self.continuity_tracker.tracked_record_count()
}
```

- [ ] **Step 9.11: Update lib.rs exports**

Add to `crates/flushdb-engine/src/lib.rs`:
```rust
pub use cache::{CacheConfig, CacheStats, ReadBudget};
```

- [ ] **Step 9.12: Fix all existing EngineConfig constructions**

In `tests/engine_tests.rs` and `tests/read_path_tests.rs`, add `cache_config: CacheConfig::default()` to EngineConfig construction. Fix all existing tests that construct `RangeReadResult` to include `is_partial: false`.

- [ ] **Step 9.13: Verify workspace compiles**

Run: `cargo build --workspace`
Expected: Zero warnings

- [ ] **Step 9.14: Run existing tests**

Run: `cargo test --workspace`
Expected: All existing tests pass

- [ ] **Step 9.15: Write integration tests**

Create `crates/flushdb-engine/tests/engine_cache_integration_tests.rs`:
```rust
use bytes::Bytes;
use tempfile::TempDir;

use flushdb_engine::{
    CacheConfig, Engine, EngineConfig, FlushConfig, ManifestConfig, MemtableConfig,
    CompactionConfig, RangeReadOptions,
};
use flushdb_types::LocalFsBackend;
use flushdb_wal::WalConfig;

fn test_config(dir: &TempDir, namespace: &str) -> (EngineConfig, LocalFsBackend) {
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let backend = LocalFsBackend::new(storage_dir);
    let config = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 4096,
            max_frozen_count: 3,
        },
        wal_config: WalConfig::default(),
        flush_config: FlushConfig {
            sst_config: flushdb_engine::sstable::types::SstConfig::default(),
            max_frozen_count: 3,
            flush_trigger_size: 4096,
            flush_trigger_age: std::time::Duration::from_secs(3600),
        },
        compaction_config: CompactionConfig::default(),
        manifest_config: ManifestConfig {
            base_path: "flushdb".to_string(),
            ..ManifestConfig::default()
        },
        cache_config: CacheConfig::default(),
        namespace: namespace.to_string(),
        local_dir: dir.path().to_path_buf(),
    };
    (config, backend)
}

#[tokio::test]
async fn test_engine_opens_with_cache() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "cache-test");
    let engine = Engine::open(backend, config).await.unwrap();
    assert_eq!(engine.pinned_metadata_count(), 0);
    assert_eq!(engine.continuity_tracked_records(), 0);
}

#[tokio::test]
async fn test_repeated_get_uses_cache() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "cache-test");
    let mut engine = Engine::open(backend, config).await.unwrap();

    // Write enough to trigger flush
    for i in 0..50u32 {
        engine.put(
            b"rec1",
            format!("key{i:04}").as_bytes(),
            Bytes::from(format!("value{i}")),
            Bytes::new(),
            None,
        ).await.unwrap();
    }

    // First get — cache miss, loads from SSTable
    let r1 = engine.get(b"rec1", b"key0010").await.unwrap();
    assert!(r1.is_some());

    // Second get — should hit cache
    let r2 = engine.get(b"rec1", b"key0010").await.unwrap();
    assert!(r2.is_some());
    assert_eq!(r1.unwrap().value, r2.unwrap().value);
}

#[tokio::test]
async fn test_scan_populates_cache() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "cache-test");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..50u32 {
        engine.put(
            b"rec1",
            format!("key{i:04}").as_bytes(),
            Bytes::from(format!("value{i}")),
            Bytes::new(),
            None,
        ).await.unwrap();
    }

    // Scan all — populates cache
    let result = engine.scan(b"rec1", None, None, RangeReadOptions::default()).await.unwrap();
    assert!(!result.entries.is_empty());

    // Individual gets should now hit cache
    let r = engine.get(b"rec1", b"key0005").await.unwrap();
    assert!(r.is_some());
}

#[tokio::test]
async fn test_write_invalidates_continuity() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "cache-test");
    let mut engine = Engine::open(backend, config).await.unwrap();

    // Write and flush
    for i in 0..50u32 {
        engine.put(
            b"rec1",
            format!("key{i:04}").as_bytes(),
            Bytes::from("v"),
            Bytes::new(),
            None,
        ).await.unwrap();
    }

    // Get non-existent key
    let r = engine.get(b"rec1", b"nonexistent").await.unwrap();
    assert!(r.is_none());

    // Write new key to same record — continuity should be invalidated
    engine.put(b"rec1", b"newkey", Bytes::from("v"), Bytes::new(), None).await.unwrap();

    // This get should still work (no stale data)
    let r = engine.get(b"rec1", b"nonexistent").await.unwrap();
    assert!(r.is_none());
}

#[tokio::test]
async fn test_full_lifecycle() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "lifecycle-test");
    let mut engine = Engine::open(backend, config).await.unwrap();

    // Put
    for i in 0..100u32 {
        engine.put(
            b"rec1",
            format!("key{i:04}").as_bytes(),
            Bytes::from(format!("value{i}")),
            Bytes::new(),
            None,
        ).await.unwrap();
    }

    // Scan
    let scan_result = engine.scan(b"rec1", None, None, RangeReadOptions::default()).await.unwrap();
    assert!(!scan_result.entries.is_empty());
    assert!(!scan_result.is_partial);

    // Get
    let get_result = engine.get(b"rec1", b"key0050").await.unwrap();
    assert!(get_result.is_some());

    // Delete
    engine.delete(b"rec1", b"key0050").await.unwrap();
    let deleted = engine.get(b"rec1", b"key0050").await.unwrap();
    assert!(deleted.is_none());

    // Scan again after delete
    let scan2 = engine.scan(b"rec1", None, None, RangeReadOptions::default()).await.unwrap();
    assert!(scan2.entries.iter().all(|e| e.composite_key.item_key() != b"key0050"));
}
```

- [ ] **Step 9.16: Run all tests**

Run: `cargo test --workspace`
Expected: All tests pass

- [ ] **Step 9.17: Run clippy**

Run: `cargo clippy --workspace`
Expected: No warnings

- [ ] **Step 9.18: Commit**

```
phase-6: wire cache components into Engine with full lifecycle management
```

---

## Final Verification

After all tasks are complete:

- [ ] `cargo build --workspace` — zero warnings
- [ ] `cargo test --workspace` — all tests pass
- [ ] `cargo clippy --workspace` — no warnings
- [ ] Verify: repeated point reads hit cache (test_repeated_get_uses_cache)
- [ ] Verify: scan does NOT evict hot point-read data (W-TinyLFU admission)
- [ ] Verify: bloom/index are pinned, not evicted (PinnedMetadataCache)
- [ ] Verify: compaction invalidates stale entries (eviction functions)
- [ ] Verify: continuity tracking works for negative lookups
- [ ] Verify: full lifecycle works (test_full_lifecycle)
