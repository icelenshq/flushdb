use flushdb_types::{CompositeKey, FlushError, FlushResult, IdempotencyToken, StorageBackend};

use super::block_reader::{decode_block, BlockEntry};
use super::bloom_filter::FilterBlock;
use super::dedup_block::DedupBlock;
use super::footer::SstFooter;
use super::index_block::IndexBlock;
use super::types::{CompressionType, FOOTER_SIZE};

pub struct SSTableReader<B: StorageBackend> {
    backend: B,
    path: String,
    file_size: u64,
    footer: SstFooter,
    filter: Option<FilterBlock>,
    index: Option<IndexBlock>,
    dedup: Option<DedupBlock>,
}

impl<B: StorageBackend> SSTableReader<B> {
    pub async fn open(backend: B, path: String, file_size: u64) -> FlushResult<Self> {
        let footer_offset = file_size - FOOTER_SIZE as u64;
        let footer_bytes = backend
            .get_range(&path, footer_offset, FOOTER_SIZE as u64)
            .await?;
        let footer = SstFooter::decode(&footer_bytes)?;

        Ok(Self {
            backend,
            path,
            file_size,
            footer,
            filter: None,
            index: None,
            dedup: None,
        })
    }

    pub async fn load_metadata(&mut self) -> FlushResult<()> {
        // Load bloom filter
        let filter_bytes = self
            .backend
            .get_range(
                &self.path,
                self.footer.bloom_filter_offset,
                self.footer.bloom_filter_size as u64,
            )
            .await?;
        self.filter = Some(FilterBlock::deserialize(&filter_bytes)?);

        // Load index block
        let index_bytes = self
            .backend
            .get_range(
                &self.path,
                self.footer.index_block_offset,
                self.footer.index_block_size as u64,
            )
            .await?;
        self.index = Some(IndexBlock::deserialize(&index_bytes)?);

        // Load dedup block
        let dedup_offset = self.footer.dedup_block_offset();
        let dedup_bytes = self
            .backend
            .get_range(
                &self.path,
                dedup_offset,
                self.footer.dedup_block_size as u64,
            )
            .await?;
        self.dedup = Some(DedupBlock::deserialize(&dedup_bytes)?);

        Ok(())
    }

    pub async fn get(&self, key: &CompositeKey) -> FlushResult<Option<BlockEntry>> {
        let (filter, index) = self.require_metadata()?;

        if !self.footer.may_contain_key(key) {
            return Ok(None);
        }

        if !filter.maybe_contains(key.record_id()) {
            return Ok(None);
        }

        let Some(block_entry) = index.find_block(key) else {
            return Ok(None);
        };

        let raw = self
            .backend
            .get_range(
                &self.path,
                block_entry.block_offset,
                block_entry.block_size as u64,
            )
            .await?;

        let entries = decode_block(&raw, self.footer.compression_type)?;

        Ok(entries.into_iter().find(|e| e.composite_key == *key))
    }

    pub async fn get_block(&self, block_index: usize) -> FlushResult<Vec<BlockEntry>> {
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "SSTable metadata not loaded — call load_metadata() first".into(),
            })?;

        let entry = index
            .get(block_index)
            .ok_or_else(|| FlushError::InvalidArgument {
                message: format!(
                    "block index {} out of range (0..{})",
                    block_index,
                    index.block_count()
                ),
            })?;

        let raw = self
            .backend
            .get_range(&self.path, entry.block_offset, entry.block_size as u64)
            .await?;

        decode_block(&raw, self.footer.compression_type)
    }

    pub async fn scan(
        &self,
        start: &CompositeKey,
        end: Option<&CompositeKey>,
    ) -> FlushResult<Vec<BlockEntry>> {
        let (_filter, index) = self.require_metadata()?;

        let start_idx = index.find_block_index(start).unwrap_or(0);

        let mut result = Vec::new();

        for block_idx in start_idx..index.block_count() {
            // Optimization: check if we can stop early
            if let Some(end_key) = end {
                if let Some(next_entry) = index.get(block_idx) {
                    if block_idx > start_idx
                        && next_entry.first_key.as_bytes() >= end_key.as_bytes()
                    {
                        break;
                    }
                }
            }

            let entries = self.get_block(block_idx).await?;

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

    pub fn contains_record(&self, record_id: &[u8]) -> FlushResult<bool> {
        let filter = self
            .filter
            .as_ref()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "SSTable metadata not loaded — call load_metadata() first".into(),
            })?;
        Ok(filter.maybe_contains(record_id))
    }

    pub fn check_dedup(&self, token: &IdempotencyToken) -> FlushResult<bool> {
        let dedup = self
            .dedup
            .as_ref()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "SSTable metadata not loaded — call load_metadata() first".into(),
            })?;
        Ok(dedup.contains(token))
    }

    pub fn footer(&self) -> &SstFooter {
        &self.footer
    }

    pub fn key_range(&self) -> FlushResult<Option<(&CompositeKey, &CompositeKey)>> {
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "SSTable metadata not loaded — call load_metadata() first".into(),
            })?;
        Ok(index.key_range())
    }

    pub fn entry_count(&self) -> u64 {
        self.footer.entry_count
    }

    pub fn file_size(&self) -> u64 {
        self.file_size
    }

    pub fn compression(&self) -> CompressionType {
        self.footer.compression_type
    }

    pub fn block_count(&self) -> FlushResult<usize> {
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "SSTable metadata not loaded — call load_metadata() first".into(),
            })?;
        Ok(index.block_count())
    }

    fn require_metadata(&self) -> FlushResult<(&FilterBlock, &IndexBlock)> {
        let filter = self
            .filter
            .as_ref()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "SSTable metadata not loaded — call load_metadata() first".into(),
            })?;
        let index = self
            .index
            .as_ref()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "SSTable metadata not loaded — call load_metadata() first".into(),
            })?;
        Ok((filter, index))
    }
}

pub struct SstableIterator<'a, B: StorageBackend> {
    reader: &'a SSTableReader<B>,
    current_block_idx: usize,
    current_entries: Vec<BlockEntry>,
    current_entry_idx: usize,
}

impl<'a, B: StorageBackend> SstableIterator<'a, B> {
    pub fn new(reader: &'a SSTableReader<B>) -> Self {
        Self {
            reader,
            current_block_idx: 0,
            current_entries: Vec::new(),
            current_entry_idx: 0,
        }
    }

    pub async fn next(&mut self) -> FlushResult<Option<BlockEntry>> {
        loop {
            if self.current_entry_idx < self.current_entries.len() {
                let entry = self.current_entries[self.current_entry_idx].clone();
                self.current_entry_idx += 1;
                return Ok(Some(entry));
            }

            let block_count = self.reader.block_count()?;
            if self.current_block_idx >= block_count {
                return Ok(None);
            }

            self.current_entries = self.reader.get_block(self.current_block_idx).await?;
            self.current_entry_idx = 0;
            self.current_block_idx += 1;
        }
    }
}
