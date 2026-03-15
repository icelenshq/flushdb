use std::fmt;

use bytes::Bytes;
use flushdb_types::FlushResult;

use crate::block_fetcher::BlockFetcher;
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

impl fmt::Debug for CoalescingFetcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoalescingFetcher")
            .field("inner", &"CachingBlockFetcher")
            .finish()
    }
}

impl CoalescingFetcher {
    pub fn new(inner: CachingBlockFetcher) -> Self {
        Self { inner }
    }

    pub async fn fetch_block(
        &self,
        sst_path: &str,
        offset: u64,
        size: u32,
        compression: CompressionType,
    ) -> FlushResult<Vec<BlockEntry>> {
        self.inner
            .fetch_block(sst_path, offset, size, compression)
            .await
    }

    pub async fn fetch_blocks(
        &self,
        sst_path: &str,
        requests: &[BlockRequest],
        compression: CompressionType,
    ) -> FlushResult<CoalescedFetchResult> {
        if requests.is_empty() {
            return Ok(CoalescedFetchResult { blocks: Vec::new() });
        }

        let mut sorted: Vec<&BlockRequest> = requests.iter().collect();
        sorted.sort_by_key(|r| r.block_offset);

        let sst_id = sst_id_from_path(sst_path);
        let cache = self.inner.cache();

        let mut cached_results: Vec<(usize, Vec<BlockEntry>)> = Vec::new();
        let mut uncached: Vec<&BlockRequest> = Vec::new();

        for req in &sorted {
            let key = BlockCacheKey {
                sst_id: sst_id.to_string(),
                block_offset: req.block_offset,
            };
            if let Some(cached) = cache.get(&key) {
                cached_results.push((req.block_index, cached.entries().to_vec()));
            } else {
                uncached.push(req);
            }
        }

        if uncached.is_empty() {
            cached_results.sort_by_key(|(idx, _)| *idx);
            return Ok(CoalescedFetchResult {
                blocks: cached_results,
            });
        }

        // Group uncached misses into contiguous runs
        let mut groups: Vec<Vec<&BlockRequest>> = Vec::new();
        let mut current_group: Vec<&BlockRequest> = vec![uncached[0]];

        for window in uncached.windows(2) {
            let prev = window[0];
            let curr = window[1];
            if prev.block_offset + prev.block_size as u64 == curr.block_offset {
                current_group.push(curr);
            } else {
                groups.push(current_group);
                current_group = vec![curr];
            }
        }
        groups.push(current_group);

        let mut all_results = cached_results;

        for group in &groups {
            if group.len() == 1 {
                let req = group[0];
                let entries = self
                    .inner
                    .fetch_block(sst_path, req.block_offset, req.block_size, compression)
                    .await?;
                all_results.push((req.block_index, entries));
            } else {
                let merged_offset = group[0].block_offset;
                let last = group[group.len() - 1];
                let merged_end = last.block_offset + last.block_size as u64;
                let total_size = merged_end - merged_offset;
                let total_size_u32 = u32::try_from(total_size).map_err(|_| {
                    flushdb_types::FlushError::CorruptedData {
                        message: format!(
                            "coalesced range too large: {total_size} bytes exceeds u32::MAX"
                        ),
                    }
                })?;

                let raw_data: Bytes = self
                    .inner
                    .inner()
                    .fetch_raw_block(sst_path, merged_offset, total_size_u32)
                    .await?;

                for req in group {
                    let local_offset = (req.block_offset - merged_offset) as usize;
                    let local_end = local_offset + req.block_size as usize;

                    if local_end > raw_data.len() {
                        return Err(flushdb_types::FlushError::CorruptedData {
                            message: format!(
                                "block at offset {} extends beyond fetched data (local_end={}, data_len={})",
                                req.block_offset, local_end, raw_data.len()
                            ),
                        });
                    }

                    let block_bytes = &raw_data[local_offset..local_end];
                    let entries = decode_block(block_bytes, compression)?;

                    let cache_key = BlockCacheKey {
                        sst_id: sst_id.to_string(),
                        block_offset: req.block_offset,
                    };
                    cache.insert(cache_key, CachedBlock::new(entries.clone()));

                    all_results.push((req.block_index, entries));
                }
            }
        }

        all_results.sort_by_key(|(idx, _)| *idx);
        Ok(CoalescedFetchResult {
            blocks: all_results,
        })
    }
}
