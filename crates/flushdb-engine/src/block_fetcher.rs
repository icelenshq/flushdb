use async_trait::async_trait;
use bytes::Bytes;
use flushdb_types::{FlushResult, StorageBackend};

use crate::sstable::block_reader::{decode_block, BlockEntry};
use crate::sstable::types::CompressionType;

#[async_trait]
pub trait BlockFetcher: Send + Sync {
    async fn fetch_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
        compression: CompressionType,
    ) -> FlushResult<Vec<BlockEntry>>;

    async fn fetch_raw_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
    ) -> FlushResult<Bytes>;
}

pub struct DirectBlockFetcher<B: StorageBackend> {
    backend: B,
}

impl<B: StorageBackend> DirectBlockFetcher<B> {
    pub fn new(backend: B) -> Self {
        Self { backend }
    }

    pub fn backend(&self) -> &B {
        &self.backend
    }
}

#[async_trait]
impl<B: StorageBackend> BlockFetcher for DirectBlockFetcher<B> {
    async fn fetch_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
        compression: CompressionType,
    ) -> FlushResult<Vec<BlockEntry>> {
        let raw = self
            .backend
            .get_range(sst_path, offset, size as u64)
            .await?;
        decode_block(&raw, compression)
    }

    async fn fetch_raw_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
    ) -> FlushResult<Bytes> {
        self.backend
            .get_range(sst_path, offset, size as u64)
            .await
    }
}
