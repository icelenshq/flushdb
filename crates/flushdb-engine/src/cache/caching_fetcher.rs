use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use flushdb_types::FlushResult;

use crate::block_fetcher::BlockFetcher;
use crate::cache::block_cache::{BlockCache, BlockCacheKey, CachedBlock};
use crate::sstable::block_reader::BlockEntry;
use crate::sstable::types::CompressionType;

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

        if let Some(cached) = self.cache.get(&key) {
            return Ok(cached.entries().to_vec());
        }

        let entries = self.inner.fetch_block(sst_path, offset, size, compression).await?;
        let cached = CachedBlock::new(entries.clone());
        self.cache.insert(key, cached);
        Ok(entries)
    }

    async fn fetch_raw_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
    ) -> FlushResult<Bytes> {
        self.inner.fetch_raw_block(sst_path, offset, size).await
    }
}
