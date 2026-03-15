use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue};

use flushdb_engine::cache::{BlockCache, BlockCacheKey, CacheConfig, CacheStats, CachedBlock};
use flushdb_engine::sstable::BlockEntry;

fn make_entry(record_id: &str, item_key: &str, value: &[u8]) -> BlockEntry {
    BlockEntry {
        composite_key: CompositeKey::new(record_id.as_bytes(), item_key.as_bytes())
            .expect("valid composite key"),
        value: EntryValue::Inline(Bytes::copy_from_slice(value)),
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

#[test]
fn test_insert_and_get() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    let key = make_key("sst-1", 0);
    let entries = vec![make_entry("rec1", "item1", b"hello")];
    let block = CachedBlock::new(entries);

    cache.insert(key.clone(), block);
    let result = cache.get(&key);

    assert!(result.is_some());
    let cached = result.unwrap();
    assert_eq!(cached.entries().len(), 1);
    assert_eq!(cached.entries()[0].composite_key.record_id(), b"rec1");
}

#[test]
fn test_get_miss() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    let key = make_key("nonexistent", 999);
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_insert_overwrites() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    let key = make_key("sst-1", 0);
    let block1 = CachedBlock::new(vec![make_entry("rec1", "item1", b"first")]);
    let block2 = CachedBlock::new(vec![
        make_entry("rec2", "item2", b"second"),
        make_entry("rec3", "item3", b"third"),
    ]);

    cache.insert(key.clone(), block1);
    cache.insert(key.clone(), block2);

    let result = cache.get(&key).unwrap();
    assert_eq!(result.entries().len(), 2);
    assert_eq!(result.entries()[0].composite_key.record_id(), b"rec2");
}

#[test]
fn test_invalidate_single() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    let key = make_key("sst-1", 0);
    let block = CachedBlock::new(vec![make_entry("rec1", "item1", b"data")]);
    cache.insert(key.clone(), block);
    assert!(cache.get(&key).is_some());

    cache.invalidate(&key);
    assert!(cache.get(&key).is_none());
}

#[test]
fn test_invalidate_nonexistent() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    let key = make_key("never-inserted", 42);
    cache.invalidate(&key);
    // No panic — test passes if we reach here
}

#[test]
fn test_invalidate_sst_removes_all_blocks() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    for i in 0..10u64 {
        let key = make_key("sst-A", i * 4096);
        let block = CachedBlock::new(vec![make_entry(
            &format!("rec{i}"),
            "item",
            format!("data-{i}").as_bytes(),
        )]);
        cache.insert(key, block);
    }

    cache.invalidate_sst("sst-A");
    cache.run_pending_tasks();

    for i in 0..10u64 {
        let key = make_key("sst-A", i * 4096);
        assert!(
            cache.get(&key).is_none(),
            "block at offset {} should have been invalidated",
            i * 4096
        );
    }
}

#[test]
fn test_invalidate_sst_preserves_other_ssts() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    let key_a = make_key("sst-A", 0);
    let key_b = make_key("sst-B", 0);
    cache.insert(
        key_a.clone(),
        CachedBlock::new(vec![make_entry("recA", "item", b"A")]),
    );
    cache.insert(
        key_b.clone(),
        CachedBlock::new(vec![make_entry("recB", "item", b"B")]),
    );

    cache.invalidate_sst("sst-A");
    cache.run_pending_tasks();

    assert!(cache.get(&key_a).is_none());
    assert!(cache.get(&key_b).is_some());
}

#[test]
fn test_invalidate_sst_nonexistent() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    cache.invalidate_sst("does-not-exist");
    // No panic — test passes if we reach here
}

#[test]
fn test_capacity_eviction() {
    let config = CacheConfig {
        block_cache_capacity_bytes: 1024,
        ..CacheConfig::default()
    };
    let cache = BlockCache::new(&config);

    // Insert blocks that collectively exceed 1024 bytes
    for i in 0..50u64 {
        let key = make_key("sst-big", i * 4096);
        // Each entry has: key (~10+ bytes) + value (100 bytes) + metadata (0) + 40 overhead
        let block = CachedBlock::new(vec![make_entry(
            &format!("r{i:03}"),
            "k",
            &[0xAB; 100],
        )]);
        cache.insert(key, block);
    }

    // Force moka to process pending tasks
    cache.run_pending_tasks();

    // weighted_size should be bounded near capacity
    assert!(
        cache.weighted_size() <= 1024 + 512,
        "weighted_size {} should be roughly bounded by capacity 1024",
        cache.weighted_size()
    );
}

#[test]
fn test_cached_block_size_calculation() {
    let entries = vec![
        make_entry("rec1", "item1", b"hello"),
        make_entry("rec2", "item2", b"world!"),
    ];

    // Entry 1: key = "rec1" + 0x00 + "item1" = 10 bytes, value = 5 bytes, metadata = 0, overhead = 40 => 55
    // Entry 2: key = "rec2" + 0x00 + "item2" = 10 bytes, value = 6 bytes, metadata = 0, overhead = 40 => 56
    // Total: 111
    let block = CachedBlock::new(entries);
    assert_eq!(block.estimated_size(), 111);
}

#[test]
fn test_empty_block_size() {
    let block = CachedBlock::new(vec![]);
    assert_eq!(block.estimated_size(), 0);
}

#[test]
fn test_default_config() {
    let config = CacheConfig::default();
    assert_eq!(config.block_cache_capacity_bytes, 268_435_456);
    assert_eq!(config.pinned_metadata_capacity, 1000);
    assert!(config.enable_continuity_tracking);
    assert_eq!(config.get_budget_per_read, 8);
}

#[test]
fn test_stats_reflects_inserted_blocks() {
    let config = CacheConfig::default();
    let cache = BlockCache::new(&config);

    let block1 = CachedBlock::new(vec![make_entry("rec1", "item1", b"hello")]);
    let block2 = CachedBlock::new(vec![
        make_entry("rec2", "item2", b"world!"),
        make_entry("rec3", "item3", b"data"),
    ]);
    let expected_weight = (block1.estimated_size() + block2.estimated_size()) as u64;

    cache.insert(make_key("sst-1", 0), block1);
    cache.insert(make_key("sst-1", 4096), block2);
    cache.run_pending_tasks();

    let stats: CacheStats = cache.stats();
    assert_eq!(stats.entry_count, 2);
    assert_eq!(stats.weighted_size_bytes, expected_weight);
    assert!(stats.weighted_size_bytes > 0);
}
