use std::fmt;
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
        let size_bytes: usize = entries
            .iter()
            .map(|entry| {
                let key_size = entry.composite_key.as_bytes().len();
                let value_size = match &entry.value {
                    EntryValue::Inline(bytes) => bytes.len(),
                    EntryValue::BlobRef {
                        blob_id, ..
                    } => blob_id.len() + 12,
                };
                let metadata_size = entry.metadata.len();
                key_size + value_size + metadata_size + 40
            })
            .sum();
        let size_bytes = u32::try_from(size_bytes).unwrap_or(u32::MAX);

        Self {
            entries: Arc::new(entries),
            size_bytes,
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
            block_cache_capacity_bytes: 268_435_456,
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
        let inner = Cache::builder()
            .max_capacity(config.block_cache_capacity_bytes)
            .weigher(|_key: &BlockCacheKey, value: &CachedBlock| value.estimated_size())
            .eviction_listener(|_key, _value, _cause| {
                // Placeholder for future NVMe tier integration
            })
            .support_invalidation_closures()
            .build();

        Self { inner }
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
        let sst_id_owned = sst_id.to_string();
        self.inner
            .invalidate_entries_if(move |key: &BlockCacheKey, _value| key.sst_id == sst_id_owned)
            .expect("invalidation closures must be enabled");
    }

    pub fn entry_count(&self) -> u64 {
        self.inner.entry_count()
    }

    pub fn weighted_size(&self) -> u64 {
        self.inner.weighted_size()
    }

    pub fn run_pending_tasks(&self) {
        self.inner.run_pending_tasks();
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: 0,
            misses: 0,
            insertions: 0,
            evictions: 0,
            weighted_size_bytes: self.inner.weighted_size(),
            entry_count: self.inner.entry_count(),
        }
    }
}

impl fmt::Debug for BlockCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BlockCache")
            .field("entry_count", &self.inner.entry_count())
            .field("weighted_size", &self.inner.weighted_size())
            .finish()
    }
}
