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
    pub fn new(bloom_filter: FilterBlock, index_block: IndexBlock, footer: SstFooter) -> Self {
        let bloom_size = bloom_filter.serialize().len();
        let index_size = index_block.block_count() * 50;
        let footer_size = 80;
        let estimated_size_bytes = (bloom_size + index_size + footer_size) as u64;

        Self {
            bloom_filter,
            index_block,
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
        if let Some(existing) = self.entries.get(&sst_id) {
            self.total_size_bytes -= existing.estimated_size_bytes;
            self.total_size_bytes += metadata.estimated_size_bytes;
            self.entries.insert(sst_id, metadata);
            return;
        }

        if self.entries.len() >= self.max_entries {
            return;
        }

        self.total_size_bytes += metadata.estimated_size_bytes;
        self.entries.insert(sst_id, metadata);
    }

    pub fn get(&self, sst_id: &str) -> Option<&PinnedMetadata> {
        self.entries.get(sst_id)
    }

    pub fn release(&mut self, sst_id: &str) -> bool {
        if let Some(removed) = self.entries.remove(sst_id) {
            self.total_size_bytes -= removed.estimated_size_bytes;
            true
        } else {
            false
        }
    }

    pub fn release_batch(&mut self, sst_ids: &[String]) {
        for sst_id in sst_ids {
            self.release(sst_id);
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
