use flushdb_types::{CompositeKey, FlushResult};

use crate::block_fetcher::BlockFetcher;
use crate::manifest::types::{Level, ManifestConfig, SSTableMeta};
use crate::sstable::block_reader::BlockEntry;
use crate::sstable::bloom_filter::FilterBlock;
use crate::sstable::footer::SstFooter;
use crate::sstable::index_block::{IndexBlock, IndexEntry};
use crate::sstable::types::FOOTER_SIZE;

pub struct SSTableHandle {
    pub meta: SSTableMeta,
    pub path: String,
    footer: SstFooter,
    bloom_filter: FilterBlock,
    index_block: IndexBlock,
}

impl SSTableHandle {
    pub async fn open(
        meta: SSTableMeta,
        path: String,
        fetcher: &dyn BlockFetcher,
    ) -> FlushResult<Self> {
        // Fetch footer (last 80 bytes)
        let file_size = meta.size_bytes;
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let footer_bytes = fetcher
            .fetch_raw_block(&path, footer_offset, FOOTER_SIZE as u32)
            .await?;
        let footer = SstFooter::decode(&footer_bytes)?;

        // Fetch bloom filter
        let bloom_bytes = fetcher
            .fetch_raw_block(&path, footer.bloom_filter_offset, footer.bloom_filter_size)
            .await?;
        let bloom_filter = FilterBlock::deserialize(&bloom_bytes)?;

        // Fetch index block
        let index_bytes = fetcher
            .fetch_raw_block(&path, footer.index_block_offset, footer.index_block_size)
            .await?;
        let index_block = IndexBlock::deserialize(&index_bytes)?;

        Ok(Self {
            meta,
            path,
            footer,
            bloom_filter,
            index_block,
        })
    }

    pub fn may_contain_record(&self, record_id: &[u8]) -> bool {
        self.bloom_filter.maybe_contains(record_id)
    }

    pub fn find_block_for_key(&self, key: &CompositeKey) -> Option<&IndexEntry> {
        self.index_block.find_block(key)
    }

    pub fn block_count(&self) -> usize {
        self.index_block.block_count()
    }

    pub fn key_range(&self) -> Option<(&CompositeKey, &CompositeKey)> {
        self.index_block.key_range()
    }

    pub fn overlaps(&self, start: &CompositeKey, end: &CompositeKey) -> bool {
        self.index_block.overlaps(start, end)
    }

    pub fn compression(&self) -> crate::sstable::types::CompressionType {
        self.footer.compression_type
    }

    pub async fn get_block(
        &self,
        block_index: usize,
        fetcher: &dyn BlockFetcher,
    ) -> FlushResult<Vec<BlockEntry>> {
        let entry = self.index_block.get(block_index).ok_or_else(|| {
            flushdb_types::FlushError::InvalidArgument {
                message: format!(
                    "block index {} out of range (0..{})",
                    block_index,
                    self.index_block.block_count()
                ),
            }
        })?;

        fetcher
            .fetch_block(
                &self.path,
                entry.block_offset,
                entry.block_size,
                self.footer.compression_type,
            )
            .await
    }

    pub async fn get(
        &self,
        key: &CompositeKey,
        fetcher: &dyn BlockFetcher,
    ) -> FlushResult<Option<BlockEntry>> {
        // Bloom filter check
        if !self.may_contain_record(key.record_id()) {
            return Ok(None);
        }

        // Index lookup
        let Some(index_entry) = self.find_block_for_key(key) else {
            return Ok(None);
        };

        // Fetch and decode block
        let entries = fetcher
            .fetch_block(
                &self.path,
                index_entry.block_offset,
                index_entry.block_size,
                self.footer.compression_type,
            )
            .await?;

        // Linear scan within block for exact match
        // Return the entry with the highest sequence number for this key
        let mut best: Option<BlockEntry> = None;
        for entry in entries {
            if entry.composite_key == *key {
                match &best {
                    Some(b) if b.sequence_number >= entry.sequence_number => {}
                    _ => best = Some(entry),
                }
            }
        }

        Ok(best)
    }

    pub async fn scan(
        &self,
        start: &CompositeKey,
        end: Option<&CompositeKey>,
        fetcher: &dyn BlockFetcher,
    ) -> FlushResult<Vec<BlockEntry>> {
        let start_idx = self
            .index_block
            .find_block_index(start)
            .unwrap_or(0);

        let mut result = Vec::new();

        for block_idx in start_idx..self.index_block.block_count() {
            // Early termination: if block's first key is past our end
            if let Some(end_key) = end {
                if block_idx > start_idx {
                    if let Some(entry) = self.index_block.get(block_idx) {
                        if entry.first_key.as_bytes() >= end_key.as_bytes() {
                            break;
                        }
                    }
                }
            }

            let entries = self.get_block(block_idx, fetcher).await?;

            for entry in entries {
                if entry.composite_key.as_bytes() < start.as_bytes() {
                    continue;
                }
                if let Some(end_key) = end {
                    if entry.composite_key.as_bytes() >= end_key.as_bytes() {
                        return Ok(result);
                    }
                }
                result.push(entry);
            }
        }

        Ok(result)
    }
}

// --- LevelState ---

pub struct LevelState {
    pub level: Level,
    pub handles: Vec<SSTableHandle>,
}

impl LevelState {
    pub async fn open_all(
        level: Level,
        metas: &[SSTableMeta],
        namespace: &str,
        _config: &ManifestConfig,
        fetcher: &dyn BlockFetcher,
    ) -> FlushResult<Self> {
        let mut handles = Vec::with_capacity(metas.len());

        for meta in metas {
            let path = meta.sst_path(namespace, level);
            let handle = SSTableHandle::open(meta.clone(), path, fetcher).await?;
            handles.push(handle);
        }

        // L1+ handles sorted by min_key for binary search
        if !level.is_overlapping() {
            handles.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));
        }

        Ok(Self { level, handles })
    }

    pub fn empty(level: Level) -> Self {
        Self {
            level,
            handles: Vec::new(),
        }
    }

    pub fn find_candidates_for_key<'a>(
        &'a self,
        key: &CompositeKey,
    ) -> Vec<&'a SSTableHandle> {
        if self.level.is_overlapping() {
            // L0: return all handles that pass bloom filter
            self.handles
                .iter()
                .filter(|h| h.may_contain_record(key.record_id()))
                .collect()
        } else {
            // L1+: binary search on key ranges
            let kb = key.as_bytes();
            let idx = self
                .handles
                .partition_point(|h| h.meta.max_key.as_slice() < kb);
            if idx < self.handles.len() && self.handles[idx].meta.contains_key(key) {
                vec![&self.handles[idx]]
            } else {
                vec![]
            }
        }
    }

    pub fn find_candidates_for_range<'a>(
        &'a self,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Vec<&'a SSTableHandle> {
        self.handles
            .iter()
            .filter(|h| h.meta.overlaps_range(start.as_bytes(), end.as_bytes()))
            .collect()
    }

    pub fn handle_count(&self) -> usize {
        self.handles.len()
    }
}
