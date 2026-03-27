use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue};

use flushdb_engine::cache::{
    evict_compaction_result, evict_sstable, evict_sstables, BlockCache, BlockCacheKey, CacheConfig,
    CachedBlock, PinnedMetadata, PinnedMetadataCache,
};
use flushdb_engine::compaction::CompactionResult;
use flushdb_engine::sstable::block_reader::BlockEntry;
use flushdb_engine::sstable::bloom_filter::{BloomFilterBuilder, FilterBlock};
use flushdb_engine::sstable::footer::SstFooter;
use flushdb_engine::sstable::index_block::IndexBlockBuilder;
use flushdb_engine::sstable::types::CompressionType;

fn make_entry() -> BlockEntry {
    BlockEntry {
        composite_key: CompositeKey::new(b"rec", b"key").expect("valid composite key"),
        value: EntryValue::Inline(Bytes::from_static(b"val")),
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

fn make_pinned_metadata() -> PinnedMetadata {
    let mut bloom_builder = BloomFilterBuilder::new(10);
    bloom_builder.add(b"record1");
    let filter = FilterBlock::Bloom(bloom_builder.build());

    let mut index_builder = IndexBlockBuilder::new();
    index_builder.add(
        CompositeKey::new(b"r1", b"k1").expect("valid key"),
        0,
        4096,
        4096,
    );
    let index_block = index_builder.build();

    let min_key = SstFooter::truncate_key(&CompositeKey::new(b"aaa", b"000").expect("valid key"));
    let max_key = SstFooter::truncate_key(&CompositeKey::new(b"zzz", b"999").expect("valid key"));
    let footer = SstFooter {
        bloom_filter_offset: 8192,
        bloom_filter_size: 512,
        index_block_offset: 8704,
        index_block_size: 256,
        entry_count: 100,
        min_key,
        max_key,
        compression_type: CompressionType::None,
        format_version: 1,
        dedup_block_size: 0,
    };

    PinnedMetadata::new(filter, index_block, footer)
}

fn make_default_caches() -> (BlockCache, PinnedMetadataCache) {
    let config = CacheConfig::default();
    (BlockCache::new(&config), PinnedMetadataCache::new(&config))
}

#[test]
fn test_evict_sstable_clears_block_cache() {
    let (block_cache, mut pinned) = make_default_caches();

    for i in 0..5u64 {
        block_cache.insert(
            make_key("sst-a", i * 4096),
            CachedBlock::new(vec![make_entry()]),
        );
    }

    evict_sstable(&block_cache, &mut pinned, "sst-a");
    block_cache.run_pending_tasks();

    for i in 0..5u64 {
        assert!(
            block_cache.get(&make_key("sst-a", i * 4096)).is_none(),
            "block at offset {} should be evicted",
            i * 4096
        );
    }
}

#[test]
fn test_evict_sstable_clears_pinned_metadata() {
    let (block_cache, mut pinned) = make_default_caches();

    pinned.pin("sst-a".to_string(), make_pinned_metadata());
    assert!(pinned.get("sst-a").is_some());

    evict_sstable(&block_cache, &mut pinned, "sst-a");

    assert!(pinned.get("sst-a").is_none());
}

#[test]
fn test_evict_sstable_preserves_other_data() {
    let (block_cache, mut pinned) = make_default_caches();

    block_cache.insert(make_key("sst-a", 0), CachedBlock::new(vec![make_entry()]));
    block_cache.insert(make_key("sst-b", 0), CachedBlock::new(vec![make_entry()]));
    pinned.pin("sst-a".to_string(), make_pinned_metadata());
    pinned.pin("sst-b".to_string(), make_pinned_metadata());

    evict_sstable(&block_cache, &mut pinned, "sst-a");
    block_cache.run_pending_tasks();

    assert!(block_cache.get(&make_key("sst-a", 0)).is_none());
    assert!(block_cache.get(&make_key("sst-b", 0)).is_some());
    assert!(pinned.get("sst-a").is_none());
    assert!(pinned.get("sst-b").is_some());
}

#[test]
fn test_evict_nonexistent_sstable() {
    let (block_cache, mut pinned) = make_default_caches();

    evict_sstable(&block_cache, &mut pinned, "does-not-exist");
    // No panic — test passes if we reach here
}

#[test]
fn test_evict_sstables_batch() {
    let (block_cache, mut pinned) = make_default_caches();

    for id in ["a", "b", "c"] {
        block_cache.insert(make_key(id, 0), CachedBlock::new(vec![make_entry()]));
        pinned.pin(id.to_string(), make_pinned_metadata());
    }

    evict_sstables(
        &block_cache,
        &mut pinned,
        &["a".to_string(), "b".to_string()],
    );
    block_cache.run_pending_tasks();

    assert!(block_cache.get(&make_key("a", 0)).is_none());
    assert!(block_cache.get(&make_key("b", 0)).is_none());
    assert!(block_cache.get(&make_key("c", 0)).is_some());
    assert!(pinned.get("a").is_none());
    assert!(pinned.get("b").is_none());
    assert!(pinned.get("c").is_some());
}

#[test]
fn test_evict_sstables_empty_list() {
    let (block_cache, mut pinned) = make_default_caches();

    block_cache.insert(make_key("sst-x", 0), CachedBlock::new(vec![make_entry()]));
    pinned.pin("sst-x".to_string(), make_pinned_metadata());

    evict_sstables(&block_cache, &mut pinned, &[]);

    assert!(block_cache.get(&make_key("sst-x", 0)).is_some());
    assert!(pinned.get("sst-x").is_some());
}

#[test]
fn test_evict_compaction_result() {
    let (block_cache, mut pinned) = make_default_caches();

    for id in ["old-1", "old-2", "keep"] {
        block_cache.insert(make_key(id, 0), CachedBlock::new(vec![make_entry()]));
        pinned.pin(id.to_string(), make_pinned_metadata());
    }

    let result = CompactionResult {
        output_sstables: vec![],
        removed_sstable_ids: vec!["old-1".to_string(), "old-2".to_string()],
        trivial_moves: 0,
        entries_written: 0,
        entries_dropped: 0,
        bytes_read: 0,
        bytes_written: 0,
    };

    evict_compaction_result(&block_cache, &mut pinned, &result);
    block_cache.run_pending_tasks();

    assert!(block_cache.get(&make_key("old-1", 0)).is_none());
    assert!(block_cache.get(&make_key("old-2", 0)).is_none());
    assert!(block_cache.get(&make_key("keep", 0)).is_some());
    assert!(pinned.get("old-1").is_none());
    assert!(pinned.get("old-2").is_none());
    assert!(pinned.get("keep").is_some());
}
