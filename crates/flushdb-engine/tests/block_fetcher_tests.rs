use bytes::Bytes;
use flushdb_engine::sstable::{CompressionType, SstConfig, SSTableWriter, SstInfo};
use flushdb_engine::{
    BlockFetcher, DirectBlockFetcher, Level, ManifestConfig, SSTableHandle, SSTableMeta, LevelState,
};
use flushdb_types::{
    CompositeKey, EntryType, EntryValue, IdempotencyToken, LocalFsBackend, MemtableEntry,
    StorageBackend,
};
use tempfile::TempDir;

fn make_entry(record_id: &str, item_key: &str, value: &str, seq: u64) -> MemtableEntry {
    let key = CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap();
    MemtableEntry::with_sequence(
        key,
        Bytes::from(value.to_string()),
        Bytes::new(),
        IdempotencyToken::new(seq),
        seq,
        EntryType::Put,
    )
}

async fn write_sst(
    backend: &LocalFsBackend,
    path: &str,
    entries: Vec<MemtableEntry>,
) -> SstInfo {
    let config = SstConfig::default().with_compression(CompressionType::None);
    let writer = SSTableWriter::new(config);
    writer
        .write(backend, path, entries.into_iter())
        .await
        .unwrap()
}

fn meta_from_info(info: &SstInfo, created_at_ms: u64) -> SSTableMeta {
    let seq_min = 1;
    let seq_max = info.entry_count;
    SSTableMeta::from_sst_info(info, (seq_min, seq_max), info.entry_count, created_at_ms)
}

// ---- DirectBlockFetcher Tests ----

#[tokio::test]
async fn test_fetch_block_decodes_correctly() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..10)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "test.sst", entries.clone()).await;
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    // The index block tells us where the first data block is.
    // For a small SSTable with no compression, all entries likely fit in one block.
    // The first data block starts right after the header (16 bytes).
    // We fetch the block using the info from the SSTable to get the footer,
    // then parse the index block to find offsets.

    // Open the handle which loads footer, bloom, and index
    let meta = meta_from_info(&info, 1000);
    let handle = SSTableHandle::open(meta, "test.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Fetch the first block using the handle's knowledge
    let block_entries = handle.get_block(0, &fetcher).await.unwrap();
    assert!(!block_entries.is_empty(), "first block should have entries");

    // Verify decoded entries have correct keys
    for entry in &block_entries {
        assert!(
            entry.composite_key.record_id().starts_with(b"rec_"),
            "decoded entry key should match"
        );
        assert!(entry.value.is_inline(), "value should be inline");
    }
}

#[tokio::test]
async fn test_fetch_raw_block_returns_bytes() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..5)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "raw.sst", entries).await;
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    // Fetch some raw bytes from the bloom filter region
    let raw = fetcher
        .fetch_raw_block("raw.sst", info.bloom_filter_offset, info.bloom_filter_size)
        .await
        .unwrap();

    assert_eq!(
        raw.len(),
        info.bloom_filter_size as usize,
        "raw block should return exactly the requested size"
    );
}

#[tokio::test]
async fn test_fetch_block_nonexistent_path() {
    let tmp = TempDir::new().unwrap();
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let result = fetcher
        .fetch_raw_block("nonexistent.sst", 0, 100)
        .await;

    assert!(result.is_err(), "fetching from nonexistent file should fail");
}

#[tokio::test]
async fn test_fetch_block_invalid_offset() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries = vec![make_entry("rec_0000", "item_0000", "val_0", 1)];
    let _info = write_sst(&backend, "small.sst", entries).await;
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let result = fetcher
        .fetch_raw_block("small.sst", 999_999, 100)
        .await;

    assert!(result.is_err(), "fetching at offset beyond file size should fail");
}

// ---- SSTableHandle Tests ----

#[tokio::test]
async fn test_handle_open_loads_metadata() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..20)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "meta.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta.clone(), "meta.sst".to_string(), &fetcher)
        .await
        .unwrap();

    assert!(handle.block_count() > 0, "should have at least one block");
    assert_eq!(handle.meta.entry_count, 20);
    assert!(handle.key_range().is_some(), "index block should expose key range");
}

#[tokio::test]
async fn test_handle_bloom_filter_check() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..50)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "bloom.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "bloom.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Keys that are in the SSTable should pass bloom filter
    for i in 0..50 {
        let record_id = format!("rec_{i:04}");
        assert!(
            handle.may_contain_record(record_id.as_bytes()),
            "bloom filter should return true for existing record_id: {record_id}"
        );
    }

    // A key that is definitely NOT in the SSTable may return false (probabilistic)
    // We test many random keys to show bloom filter rejects at least some
    let mut false_count = 0;
    for i in 1000..1100 {
        let record_id = format!("absent_{i:06}");
        if !handle.may_contain_record(record_id.as_bytes()) {
            false_count += 1;
        }
    }
    assert!(
        false_count > 0,
        "bloom filter should reject at least some absent keys (got 0 rejections out of 100)"
    );
}

#[tokio::test]
async fn test_handle_point_lookup_hit() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..20)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "lookup.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "lookup.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Look up key at index 10
    let key = CompositeKey::new(b"rec_0010", b"item_0010").unwrap();
    let result = handle.get(&key, &fetcher).await.unwrap();

    assert!(result.is_some(), "point lookup should find existing key");
    let entry = result.unwrap();
    assert_eq!(entry.composite_key, key);
    assert_eq!(entry.value, EntryValue::Inline(Bytes::from("val_10")));
}

#[tokio::test]
async fn test_handle_point_lookup_miss() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..10)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "miss.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "miss.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Look up a key that doesn't exist
    let key = CompositeKey::new(b"rec_9999", b"item_9999").unwrap();
    let result = handle.get(&key, &fetcher).await.unwrap();
    assert!(result.is_none(), "point lookup should return None for absent key");
}

#[tokio::test]
async fn test_handle_point_lookup_bloom_miss() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    // Write entries with a specific prefix
    let entries: Vec<MemtableEntry> = (0..50)
        .map(|i| make_entry(&format!("alpha_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "bloommiss.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "bloommiss.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Try a key with a completely different record_id prefix
    // The bloom filter should reject it without fetching a data block.
    // We verify by checking that the lookup returns None.
    // (We can't directly assert "no data block fetch" without mocking, but we
    // verify the bloom filter rejects at least some of these keys.)
    let mut bloom_rejected = 0;
    for i in 0..100 {
        let record_id = format!("zzzzz_nonexist_{i:06}");
        if !handle.may_contain_record(record_id.as_bytes()) {
            bloom_rejected += 1;
        }
    }
    assert!(
        bloom_rejected > 50,
        "bloom filter should reject most absent record_ids, only rejected {bloom_rejected}/100"
    );

    // Full point lookup still returns None
    let key = CompositeKey::new(b"zzzzz_nonexist_000000", b"item_0000").unwrap();
    let result = handle.get(&key, &fetcher).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_handle_scan_full_range() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..30)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "scanfull.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "scanfull.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Scan with start before first key and no end
    let start = CompositeKey::new(b"rec_0000", b"item_0000").unwrap();
    let result = handle.scan(&start, None, &fetcher).await.unwrap();

    assert_eq!(result.len(), 30, "full scan should return all 30 entries");

    // Verify sorted order
    for i in 1..result.len() {
        assert!(
            result[i].composite_key.as_bytes() >= result[i - 1].composite_key.as_bytes(),
            "scan results should be in sorted order"
        );
    }
}

#[tokio::test]
async fn test_handle_scan_bounded_range() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..30)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "scanbounded.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "scanbounded.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Scan from rec_0010 to rec_0020 (exclusive)
    let start = CompositeKey::new(b"rec_0010", b"item_0010").unwrap();
    let end = CompositeKey::new(b"rec_0020", b"item_0020").unwrap();
    let result = handle.scan(&start, Some(&end), &fetcher).await.unwrap();

    // Should include entries from rec_0010 through rec_0019
    assert!(
        !result.is_empty(),
        "bounded scan should return entries in range"
    );

    for entry in &result {
        let key_bytes = entry.composite_key.as_bytes();
        assert!(
            key_bytes >= start.as_bytes(),
            "entry should be >= start"
        );
        assert!(
            key_bytes < end.as_bytes(),
            "entry should be < end"
        );
    }

    assert_eq!(result.len(), 10, "should return exactly 10 entries (rec_0010 through rec_0019)");
}

#[tokio::test]
async fn test_handle_key_range() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries: Vec<MemtableEntry> = (0..10)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "keyrange.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "keyrange.sst".to_string(), &fetcher)
        .await
        .unwrap();

    let (min_key, _max_key) = handle.key_range().unwrap();
    let expected_min = CompositeKey::new(b"rec_0000", b"item_0000").unwrap();

    // key_range returns first keys of first and last blocks (index block behavior)
    assert_eq!(*min_key, expected_min, "min key should be the first entry's key");
    // max_key is the first key of the last block, which depends on block boundaries
}

// ---- LevelState Tests ----

async fn write_sst_for_range(
    backend: &LocalFsBackend,
    path: &str,
    start: usize,
    end: usize,
) -> SstInfo {
    let entries: Vec<MemtableEntry> = (start..end)
        .map(|i| {
            make_entry(
                &format!("rec_{i:04}"),
                &format!("item_{i:04}"),
                &format!("val_{i}"),
                i as u64 + 1,
            )
        })
        .collect();
    write_sst(backend, path, entries).await
}

#[tokio::test]
async fn test_level_state_l0_returns_all_bloom_matches() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    // Write 3 L0 SSTables with overlapping key ranges
    let info_a = write_sst_for_range(&backend, "l0_a.sst", 0, 20).await;
    let info_b = write_sst_for_range(&backend, "l0_b.sst", 10, 30).await;
    let info_c = write_sst_for_range(&backend, "l0_c.sst", 25, 40).await;

    let mut meta_a = meta_from_info(&info_a, 1000);
    meta_a.id = "l0_a".to_string();
    let mut meta_b = meta_from_info(&info_b, 1001);
    meta_b.id = "l0_b".to_string();
    let mut meta_c = meta_from_info(&info_c, 1002);
    meta_c.id = "l0_c".to_string();

    // Override the sst_path to return our custom path.
    // Since sst_path uses the id field, we need metas whose sst_path matches our file paths.
    // Instead, we'll directly construct LevelState by opening handles with our known paths.
    let handle_a = SSTableHandle::open(meta_a, "l0_a.sst".to_string(), &fetcher)
        .await
        .unwrap();
    let handle_b = SSTableHandle::open(meta_b, "l0_b.sst".to_string(), &fetcher)
        .await
        .unwrap();
    let handle_c = SSTableHandle::open(meta_c, "l0_c.sst".to_string(), &fetcher)
        .await
        .unwrap();

    let level_state = LevelState {
        level: Level::L0,
        handles: vec![handle_a, handle_b, handle_c],
    };

    // rec_0015 exists in both SSTable A (0..20) and B (10..30)
    let key = CompositeKey::new(b"rec_0015", b"item_0015").unwrap();
    let candidates = level_state.find_candidates_for_key(&key);

    // L0 returns all bloom-matching handles
    assert!(
        candidates.len() >= 2,
        "L0 should return at least 2 candidates for a key present in 2 SSTables, got {}",
        candidates.len()
    );
}

#[tokio::test]
async fn test_level_state_l1_binary_search() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    // Write 3 L1 SSTables with non-overlapping key ranges
    let info_a = write_sst_for_range(&backend, "l1_a.sst", 0, 10).await;
    let info_b = write_sst_for_range(&backend, "l1_b.sst", 10, 20).await;
    let info_c = write_sst_for_range(&backend, "l1_c.sst", 20, 30).await;

    let mut meta_a = meta_from_info(&info_a, 1000);
    meta_a.id = "l1_a".to_string();
    let mut meta_b = meta_from_info(&info_b, 1001);
    meta_b.id = "l1_b".to_string();
    let mut meta_c = meta_from_info(&info_c, 1002);
    meta_c.id = "l1_c".to_string();

    let handle_a = SSTableHandle::open(meta_a, "l1_a.sst".to_string(), &fetcher)
        .await
        .unwrap();
    let handle_b = SSTableHandle::open(meta_b, "l1_b.sst".to_string(), &fetcher)
        .await
        .unwrap();
    let handle_c = SSTableHandle::open(meta_c, "l1_c.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // L1 handles should be sorted by min_key
    let mut handles = vec![handle_a, handle_b, handle_c];
    handles.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));

    let level_state = LevelState {
        level: Level::L1,
        handles,
    };

    // Key in the middle SSTable
    let key = CompositeKey::new(b"rec_0015", b"item_0015").unwrap();
    let candidates = level_state.find_candidates_for_key(&key);
    assert_eq!(
        candidates.len(),
        1,
        "L1 binary search should return exactly 1 candidate"
    );
    assert_eq!(candidates[0].meta.id, "l1_b");
}

#[tokio::test]
async fn test_level_state_l1_key_miss() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let info_a = write_sst_for_range(&backend, "l1_miss_a.sst", 0, 10).await;
    let info_b = write_sst_for_range(&backend, "l1_miss_b.sst", 20, 30).await;

    let mut meta_a = meta_from_info(&info_a, 1000);
    meta_a.id = "l1_miss_a".to_string();
    let mut meta_b = meta_from_info(&info_b, 1001);
    meta_b.id = "l1_miss_b".to_string();

    let handle_a = SSTableHandle::open(meta_a, "l1_miss_a.sst".to_string(), &fetcher)
        .await
        .unwrap();
    let handle_b = SSTableHandle::open(meta_b, "l1_miss_b.sst".to_string(), &fetcher)
        .await
        .unwrap();

    let mut handles = vec![handle_a, handle_b];
    handles.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));

    let level_state = LevelState {
        level: Level::L1,
        handles,
    };

    // Key in the gap between the two SSTables (rec_0010 to rec_0019)
    let key = CompositeKey::new(b"rec_0015", b"item_0015").unwrap();
    let candidates = level_state.find_candidates_for_key(&key);
    assert!(
        candidates.is_empty(),
        "key in gap between L1 SSTables should yield no candidates"
    );
}

#[tokio::test]
async fn test_level_state_range_candidates() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let info_a = write_sst_for_range(&backend, "range_a.sst", 0, 10).await;
    let info_b = write_sst_for_range(&backend, "range_b.sst", 10, 20).await;
    let info_c = write_sst_for_range(&backend, "range_c.sst", 20, 30).await;

    let mut meta_a = meta_from_info(&info_a, 1000);
    meta_a.id = "range_a".to_string();
    let mut meta_b = meta_from_info(&info_b, 1001);
    meta_b.id = "range_b".to_string();
    let mut meta_c = meta_from_info(&info_c, 1002);
    meta_c.id = "range_c".to_string();

    let handle_a = SSTableHandle::open(meta_a, "range_a.sst".to_string(), &fetcher)
        .await
        .unwrap();
    let handle_b = SSTableHandle::open(meta_b, "range_b.sst".to_string(), &fetcher)
        .await
        .unwrap();
    let handle_c = SSTableHandle::open(meta_c, "range_c.sst".to_string(), &fetcher)
        .await
        .unwrap();

    let mut handles = vec![handle_a, handle_b, handle_c];
    handles.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));

    let level_state = LevelState {
        level: Level::L1,
        handles,
    };

    // Range that spans SSTable B and C
    let start = CompositeKey::new(b"rec_0015", b"item_0015").unwrap();
    let end = CompositeKey::new(b"rec_0025", b"item_0025").unwrap();
    let candidates = level_state.find_candidates_for_range(&start, &end);

    let ids: Vec<&str> = candidates.iter().map(|h| h.meta.id.as_str()).collect();
    assert!(
        ids.contains(&"range_b"),
        "should include SSTable B which overlaps the range"
    );
    assert!(
        ids.contains(&"range_c"),
        "should include SSTable C which overlaps the range"
    );
    assert!(
        !ids.contains(&"range_a"),
        "should not include SSTable A which is entirely before the range"
    );
}

#[tokio::test]
async fn test_level_state_open_all_concurrent() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));
    let config = ManifestConfig::default();

    // Write multiple SSTables
    let mut metas = Vec::new();
    for i in 0..5 {
        let start = i * 10;
        let end = start + 10;
        let path = format!("flushdb/test/sstables/L1/open_all_{i}.sst");
        let info = write_sst_for_range(&backend, &path, start, end).await;
        let mut meta = meta_from_info(&info, 1000 + i as u64);
        meta.id = format!("open_all_{i}");
        metas.push(meta);
    }

    let level_state = LevelState::open_all(Level::L1, &metas, "test", &config, &fetcher)
        .await
        .unwrap();

    assert_eq!(level_state.handle_count(), 5);
    assert_eq!(level_state.level, Level::L1);

    // Verify handles are sorted by min_key for L1
    for i in 1..level_state.handles.len() {
        assert!(
            level_state.handles[i].meta.min_key >= level_state.handles[i - 1].meta.min_key,
            "L1 handles should be sorted by min_key"
        );
    }
}

#[tokio::test]
async fn test_level_state_empty() {
    let level_state = LevelState::empty(Level::L0);
    assert_eq!(level_state.handle_count(), 0);
    assert_eq!(level_state.level, Level::L0);

    let key = CompositeKey::new(b"any", b"key").unwrap();
    let candidates = level_state.find_candidates_for_key(&key);
    assert!(candidates.is_empty());
}

#[tokio::test]
async fn test_handle_overlaps() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let entries: Vec<MemtableEntry> = (10..20)
        .map(|i| make_entry(&format!("rec_{i:04}"), &format!("item_{i:04}"), &format!("val_{i}"), i + 1))
        .collect();

    let info = write_sst(&backend, "overlap.sst", entries).await;
    let meta = meta_from_info(&info, 1000);

    let handle = SSTableHandle::open(meta, "overlap.sst".to_string(), &fetcher)
        .await
        .unwrap();

    // Range that overlaps with the SSTable's index range
    // SSTable has keys rec_0010..rec_0019, so rec_0010 is in the index
    let start_overlap = CompositeKey::new(b"rec_0010", b"").unwrap();
    let end_overlap = CompositeKey::new(b"rec_0025", b"item_0000").unwrap();
    assert!(handle.overlaps(&start_overlap, &end_overlap));

    // Range entirely before the SSTable's key range
    let start_before = CompositeKey::new(b"rec_0000", b"item_0000").unwrap();
    let end_before = CompositeKey::new(b"rec_0005", b"item_0000").unwrap();
    assert!(!handle.overlaps(&start_before, &end_before));
}

#[tokio::test]
async fn test_handle_compression_accessor() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries = vec![make_entry("rec_0000", "item_0000", "val_0", 1)];
    let config = SstConfig::default().with_compression(CompressionType::None);
    let writer = SSTableWriter::new(config);
    let info = writer
        .write(&backend, "comp.sst", entries.into_iter())
        .await
        .unwrap();

    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "comp.sst".to_string(), &fetcher)
        .await
        .unwrap();

    assert_eq!(handle.compression(), CompressionType::None);
}

#[tokio::test]
async fn test_handle_get_block_out_of_range() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    let entries = vec![make_entry("rec_0000", "item_0000", "val_0", 1)];
    let info = write_sst(&backend, "oob.sst", entries).await;
    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "oob.sst".to_string(), &fetcher)
        .await
        .unwrap();

    let result = handle.get_block(9999, &fetcher).await;
    assert!(
        result.is_err(),
        "get_block with out-of-range index should return error"
    );
}

#[tokio::test]
async fn test_handle_multiple_blocks_scan() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());

    // Write enough entries to span multiple blocks (with small block size)
    let entries: Vec<MemtableEntry> = (0..500)
        .map(|i| {
            make_entry(
                &format!("rec_{i:04}"),
                &format!("item_{i:04}"),
                &format!("value_with_some_padding_{i}"),
                i + 1,
            )
        })
        .collect();

    let config = SstConfig::default()
        .with_compression(CompressionType::None)
        .with_block_size(512); // Small blocks to force multiple
    let writer = SSTableWriter::new(config);
    let info = writer
        .write(&backend, "multiblock.sst", entries.into_iter())
        .await
        .unwrap();

    let meta = meta_from_info(&info, 1000);
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));

    let handle = SSTableHandle::open(meta, "multiblock.sst".to_string(), &fetcher)
        .await
        .unwrap();

    assert!(
        handle.block_count() > 1,
        "should have multiple blocks with 500 entries and 512-byte block size, got {}",
        handle.block_count()
    );

    // Full scan should still return all entries
    let start = CompositeKey::new(b"rec_0000", b"item_0000").unwrap();
    let result = handle.scan(&start, None, &fetcher).await.unwrap();
    assert_eq!(result.len(), 500, "scan across multiple blocks should return all entries");
}

#[tokio::test]
async fn test_fetcher_backend_accessor_round_trip() {
    let tmp = TempDir::new().unwrap();
    let fetcher = DirectBlockFetcher::new(LocalFsBackend::new(tmp.path()));
    let backend = fetcher.backend();

    backend.put("test-key", Bytes::from("test-value")).await.unwrap();
    let result = backend.get("test-key").await.unwrap();
    assert_eq!(result, Bytes::from("test-value"));
}
