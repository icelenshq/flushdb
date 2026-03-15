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
