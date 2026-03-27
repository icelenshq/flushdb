use flushdb_engine::cache::{CacheConfig, PinnedMetadata, PinnedMetadataCache};
use flushdb_engine::sstable::bloom_filter::{BloomFilterBuilder, FilterBlock};
use flushdb_engine::sstable::footer::SstFooter;
use flushdb_engine::sstable::index_block::IndexBlockBuilder;
use flushdb_engine::sstable::types::CompressionType;
use flushdb_types::CompositeKey;

fn make_metadata() -> PinnedMetadata {
    let mut bloom_builder = BloomFilterBuilder::new(10);
    bloom_builder.add(b"record1");
    let bloom = bloom_builder.build();
    let filter = FilterBlock::Bloom(bloom);

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

fn make_config(capacity: usize) -> CacheConfig {
    CacheConfig {
        pinned_metadata_capacity: capacity,
        ..CacheConfig::default()
    }
}

#[test]
fn test_pin_and_get() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());

    let result = cache.get("sst-1");
    assert!(result.is_some());
}

#[test]
fn test_get_not_pinned() {
    let cache = PinnedMetadataCache::new(&make_config(10));

    assert!(cache.get("never-pinned").is_none());
}

#[test]
fn test_release() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());

    let released = cache.release("sst-1");
    assert!(released);
    assert!(cache.get("sst-1").is_none());
}

#[test]
fn test_release_nonexistent() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));

    let released = cache.release("unknown-sst");
    assert!(!released);
}

#[test]
fn test_pin_overwrites() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());
    cache.pin("sst-1".to_string(), make_metadata());

    assert_eq!(cache.entry_count(), 1);
}

#[test]
fn test_contains() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());

    assert!(cache.contains("sst-1"));
    assert!(!cache.contains("sst-2"));
}

#[test]
fn test_release_batch() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    for i in 0..5 {
        cache.pin(format!("sst-{i}"), make_metadata());
    }
    assert_eq!(cache.entry_count(), 5);

    cache.release_batch(&[
        "sst-0".to_string(),
        "sst-2".to_string(),
        "sst-4".to_string(),
    ]);

    assert_eq!(cache.entry_count(), 2);
    assert!(cache.contains("sst-1"));
    assert!(cache.contains("sst-3"));
    assert!(!cache.contains("sst-0"));
    assert!(!cache.contains("sst-2"));
    assert!(!cache.contains("sst-4"));
}

#[test]
fn test_release_batch_with_nonexistent() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());

    cache.release_batch(&[
        "sst-1".to_string(),
        "nonexistent-a".to_string(),
        "nonexistent-b".to_string(),
    ]);

    assert_eq!(cache.entry_count(), 0);
}

#[test]
fn test_pinned_sst_ids() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-c".to_string(), make_metadata());
    cache.pin("sst-a".to_string(), make_metadata());
    cache.pin("sst-b".to_string(), make_metadata());

    let mut ids = cache.pinned_sst_ids();
    ids.sort();

    assert_eq!(ids, vec!["sst-a", "sst-b", "sst-c"]);
}

#[test]
fn test_total_size_increases_on_pin() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    assert_eq!(cache.total_size_bytes(), 0);

    cache.pin("sst-1".to_string(), make_metadata());
    let size_after_one = cache.total_size_bytes();
    assert!(size_after_one > 0);

    cache.pin("sst-2".to_string(), make_metadata());
    let size_after_two = cache.total_size_bytes();
    assert!(size_after_two > size_after_one);
}

#[test]
fn test_total_size_decreases_on_release() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());
    cache.pin("sst-2".to_string(), make_metadata());
    let size_before = cache.total_size_bytes();

    cache.release("sst-1");
    let size_after = cache.total_size_bytes();
    assert!(size_after < size_before);
}

#[test]
fn test_entry_count() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    for i in 0..5 {
        cache.pin(format!("sst-{i}"), make_metadata());
    }
    assert_eq!(cache.entry_count(), 5);

    cache.release("sst-0");
    cache.release("sst-3");
    assert_eq!(cache.entry_count(), 3);
}

#[test]
fn test_at_capacity_pin_replaces_existing() {
    let mut cache = PinnedMetadataCache::new(&make_config(2));
    cache.pin("sst-1".to_string(), make_metadata());
    cache.pin("sst-2".to_string(), make_metadata());
    assert_eq!(cache.entry_count(), 2);

    cache.pin("sst-1".to_string(), make_metadata());
    assert_eq!(cache.entry_count(), 2);
    assert!(cache.contains("sst-1"));
    assert!(cache.contains("sst-2"));
}

#[test]
fn test_beyond_capacity_new_pin_rejected() {
    let mut cache = PinnedMetadataCache::new(&make_config(2));
    cache.pin("sst-1".to_string(), make_metadata());
    cache.pin("sst-2".to_string(), make_metadata());

    cache.pin("sst-3".to_string(), make_metadata());

    assert_eq!(cache.entry_count(), 2);
    assert!(cache.contains("sst-1"));
    assert!(cache.contains("sst-2"));
    assert!(!cache.contains("sst-3"));
}

#[test]
fn test_bloom_filter_usable_after_pin() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());

    let pinned = cache.get("sst-1").expect("should be pinned");
    assert!(pinned.bloom_filter.maybe_contains(b"record1"));
    assert!(!pinned.bloom_filter.maybe_contains(b"nonexistent"));
}

#[test]
fn test_index_block_usable_after_pin() {
    let mut cache = PinnedMetadataCache::new(&make_config(10));
    cache.pin("sst-1".to_string(), make_metadata());

    let pinned = cache.get("sst-1").expect("should be pinned");
    assert_eq!(pinned.index_block.block_count(), 1);
}
