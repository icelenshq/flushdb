use bytes::Bytes;
use flushdb_engine::sstable::block_reader::BlockEntry;
use flushdb_engine::sstable::reader::{SSTableReader, SstableIterator};
use flushdb_engine::sstable::writer::SSTableWriter;
use flushdb_engine::sstable::{CompressionType, SstConfig};
use flushdb_types::{
    CompositeKey, EntryType, EntryValue, FlushError, IdempotencyToken, LocalFsBackend,
    MemtableEntry, StorageBackend,
};
use tempfile::TempDir;

fn make_entries(count: usize) -> Vec<MemtableEntry> {
    (0..count)
        .map(|i| {
            let record_id = format!("record_{:04}", i);
            let item_key = format!("item_{:04}", i);
            let key = CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap();
            MemtableEntry::with_sequence(
                key,
                Bytes::from(format!("value_{i}")),
                Bytes::from(format!("meta_{i}")),
                IdempotencyToken::new(i as u64 + 1),
                i as u64 + 1,
                EntryType::Put,
            )
        })
        .collect()
}

fn default_config() -> SstConfig {
    SstConfig::default().with_compression(CompressionType::None)
}

async fn write_and_open_with_dir(
    dir: &std::path::Path,
    config: &SstConfig,
    entries: Vec<MemtableEntry>,
) -> SSTableReader<LocalFsBackend> {
    let backend = LocalFsBackend::new(dir);
    let writer = SSTableWriter::new(config.clone());
    writer
        .write(&backend, "test.sst", entries.into_iter())
        .await
        .unwrap();

    let data = backend.get("test.sst").await.unwrap();
    let file_size = data.len() as u64;

    let reader_backend = LocalFsBackend::new(dir);
    SSTableReader::open(reader_backend, "test.sst".to_string(), file_size)
        .await
        .unwrap()
}

async fn collect_all(
    iter: &mut SstableIterator<'_, LocalFsBackend>,
) -> Vec<BlockEntry> {
    let mut results = Vec::new();
    while let Some(entry) = iter.next().await.unwrap() {
        results.push(entry);
    }
    results
}

// --- Write-Read Round-trip Tests ---

#[tokio::test]
async fn test_write_read_single_entry() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(1);
    let expected_key = entries[0].composite_key.clone();
    let expected_value = entries[0].value.clone();
    let expected_meta = entries[0].metadata.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let result = reader.get(&expected_key).await.unwrap().unwrap();
    assert_eq!(result.composite_key, expected_key);
    assert_eq!(result.value, EntryValue::Inline(expected_value));
    assert_eq!(result.metadata, expected_meta);
    assert_eq!(result.entry_type, EntryType::Put);
    assert_eq!(result.sequence_number, 1);
}

#[tokio::test]
async fn test_write_read_100_entries() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(100);
    let expected: Vec<_> = entries
        .iter()
        .map(|e| (e.composite_key.clone(), e.value.clone()))
        .collect();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let mut iter = SstableIterator::new(&reader);
    let all = collect_all(&mut iter).await;
    assert_eq!(all.len(), 100);

    for (i, entry) in all.iter().enumerate() {
        assert_eq!(entry.composite_key, expected[i].0);
        assert_eq!(entry.value, EntryValue::Inline(expected[i].1.clone()));
    }
}

#[tokio::test]
async fn test_write_read_all_entry_types() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();

    let mut entries = vec![
        MemtableEntry::with_sequence(
            CompositeKey::new(b"rec_a", b"item_a").unwrap(),
            Bytes::from("val_a"),
            Bytes::from("meta_a"),
            IdempotencyToken::new(1),
            1,
            EntryType::Put,
        ),
        MemtableEntry::with_sequence(
            CompositeKey::new(b"rec_b", b"item_b").unwrap(),
            Bytes::new(),
            Bytes::new(),
            IdempotencyToken::new(2),
            2,
            EntryType::Delete,
        ),
        MemtableEntry::with_sequence(
            CompositeKey::new(b"rec_c", b"item_c").unwrap(),
            Bytes::from("end_key"),
            Bytes::new(),
            IdempotencyToken::new(3),
            3,
            EntryType::RangeDelete,
        ),
    ];
    entries.sort_by(|a, b| a.composite_key.cmp(&b.composite_key));

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let mut iter = SstableIterator::new(&reader);
    let all = collect_all(&mut iter).await;
    assert_eq!(all.len(), 3);

    let types: Vec<EntryType> = all.iter().map(|e| e.entry_type).collect();
    assert!(types.contains(&EntryType::Put));
    assert!(types.contains(&EntryType::Delete));
    assert!(types.contains(&EntryType::RangeDelete));
}

#[tokio::test]
async fn test_write_read_all_compression_types() {
    for compression in [
        CompressionType::None,
        CompressionType::Snappy,
        CompressionType::Zstd,
    ] {
        let tmp = TempDir::new().unwrap();
        let config = SstConfig::default().with_compression(compression);
        let entries = make_entries(50);
        let expected: Vec<_> = entries
            .iter()
            .map(|e| (e.composite_key.clone(), e.value.clone()))
            .collect();

        let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
        reader.load_metadata().await.unwrap();

        let mut iter = SstableIterator::new(&reader);
        let all = collect_all(&mut iter).await;
        assert_eq!(
            all.len(),
            50,
            "failed for compression {:?}",
            compression
        );

        for (i, entry) in all.iter().enumerate() {
            assert_eq!(
                entry.composite_key, expected[i].0,
                "key mismatch at index {i} for compression {:?}",
                compression
            );
            assert_eq!(
                entry.value,
                EntryValue::Inline(expected[i].1.clone()),
                "value mismatch at index {i} for compression {:?}",
                compression
            );
        }
    }
}

// --- Point Lookup Tests ---

#[tokio::test]
async fn test_get_existing_key() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(20);
    let target_key = entries[10].composite_key.clone();
    let target_value = entries[10].value.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let result = reader.get(&target_key).await.unwrap().unwrap();
    assert_eq!(result.composite_key, target_key);
    assert_eq!(result.value, EntryValue::Inline(target_value));
}

#[tokio::test]
async fn test_get_nonexistent_key() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(20);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let absent_key = CompositeKey::new(b"nonexistent_record", b"nonexistent_item").unwrap();
    let result = reader.get(&absent_key).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_get_bloom_eliminates_missing_record() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(20);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    // A record_id that definitely does not exist should be eliminated by bloom filter
    let absent_key = CompositeKey::new(b"zzz_absent_record", b"item_0000").unwrap();
    let result = reader.get(&absent_key).await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_get_key_in_non_first_block() {
    let tmp = TempDir::new().unwrap();
    // Small block size to produce many blocks
    let config = SstConfig::default()
        .with_compression(CompressionType::None)
        .with_block_size(128);
    let entries = make_entries(100);
    // The last entry should be in the last block
    let last_key = entries[99].composite_key.clone();
    let last_value = entries[99].value.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let block_count = reader.block_count().unwrap();
    assert!(
        block_count > 1,
        "need multiple blocks for this test, got {block_count}"
    );

    let result = reader.get(&last_key).await.unwrap().unwrap();
    assert_eq!(result.composite_key, last_key);
    assert_eq!(result.value, EntryValue::Inline(last_value));
}

#[tokio::test]
async fn test_get_first_and_last_key() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(50);
    let first_key = entries[0].composite_key.clone();
    let last_key = entries[49].composite_key.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let first = reader.get(&first_key).await.unwrap();
    assert!(first.is_some(), "first key should be found");
    assert_eq!(first.unwrap().composite_key, first_key);

    let last = reader.get(&last_key).await.unwrap();
    assert!(last.is_some(), "last key should be found");
    assert_eq!(last.unwrap().composite_key, last_key);
}

// --- Range Scan Tests ---

#[tokio::test]
async fn test_scan_full_range() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(30);
    let min_key = entries[0].composite_key.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let results = reader.scan(&min_key, None).await.unwrap();
    assert_eq!(results.len(), 30);

    // Verify sorted order
    for i in 1..results.len() {
        assert!(
            results[i].composite_key.as_bytes() >= results[i - 1].composite_key.as_bytes(),
            "entries should be in sorted order"
        );
    }
}

#[tokio::test]
async fn test_scan_subset() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(30);
    let start_key = entries[10].composite_key.clone();
    let end_key = entries[20].composite_key.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let results = reader.scan(&start_key, Some(&end_key)).await.unwrap();

    // [start, end) should contain entries 10..20 = 10 entries
    assert_eq!(results.len(), 10);
    assert_eq!(results[0].composite_key, start_key);
    // end_key should NOT be included
    assert!(results.iter().all(|e| e.composite_key.as_bytes() < end_key.as_bytes()));
}

#[tokio::test]
async fn test_scan_across_blocks() {
    let tmp = TempDir::new().unwrap();
    let config = SstConfig::default()
        .with_compression(CompressionType::None)
        .with_block_size(128);
    let entries = make_entries(100);
    let start_key = entries[10].composite_key.clone();
    let end_key = entries[90].composite_key.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let block_count = reader.block_count().unwrap();
    assert!(
        block_count >= 3,
        "need 3+ blocks for this test, got {block_count}"
    );

    let results = reader.scan(&start_key, Some(&end_key)).await.unwrap();
    assert_eq!(results.len(), 80);

    for i in 1..results.len() {
        assert!(
            results[i].composite_key.as_bytes() >= results[i - 1].composite_key.as_bytes(),
            "scan results should be in sorted order"
        );
    }
}

#[tokio::test]
async fn test_scan_empty_range() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(20);
    // start >= end produces empty result
    let start_key = entries[15].composite_key.clone();
    let end_key = entries[10].composite_key.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let results = reader.scan(&start_key, Some(&end_key)).await.unwrap();
    assert!(results.is_empty(), "start >= end should yield empty results");
}

// --- Scan Single Record Test ---

#[tokio::test]
async fn test_scan_single_record() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();

    // Create entries: 5 items under "record_A", 5 under "record_B"
    let mut entries = Vec::new();
    for i in 0..5 {
        let key =
            CompositeKey::new(b"record_A", format!("item_{:04}", i).as_bytes()).unwrap();
        entries.push(MemtableEntry::with_sequence(
            key,
            Bytes::from(format!("val_a_{i}")),
            Bytes::new(),
            IdempotencyToken::new(i as u64 + 1),
            i as u64 + 1,
            EntryType::Put,
        ));
    }
    for i in 0..5 {
        let key =
            CompositeKey::new(b"record_B", format!("item_{:04}", i).as_bytes()).unwrap();
        entries.push(MemtableEntry::with_sequence(
            key,
            Bytes::from(format!("val_b_{i}")),
            Bytes::new(),
            IdempotencyToken::new(i as u64 + 100),
            i as u64 + 100,
            EntryType::Put,
        ));
    }
    entries.sort_by(|a, b| a.composite_key.cmp(&b.composite_key));

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    // Scan for record_A items only
    let start = CompositeKey::new(b"record_A", b"").unwrap();
    let end = CompositeKey::new(b"record_A\x01", b"").unwrap();
    let results = reader.scan(&start, Some(&end)).await.unwrap();

    assert_eq!(results.len(), 5, "should find exactly 5 items for record_A");
    for entry in &results {
        assert_eq!(
            entry.composite_key.record_id(),
            b"record_A",
            "all scan results should belong to record_A"
        );
    }
}

// --- Metadata Tests ---

#[tokio::test]
async fn test_key_range_matches_first_and_last_block_keys() {
    let tmp = TempDir::new().unwrap();
    let config = SstConfig::default()
        .with_compression(CompressionType::None)
        .with_block_size(128);
    let entries = make_entries(50);
    let first_key = entries[0].composite_key.clone();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let block_count = reader.block_count().unwrap();
    assert!(block_count > 1, "need multiple blocks for this test, got {block_count}");

    let (min, max) = reader.key_range().unwrap().unwrap();

    assert_eq!(min, &first_key, "min key should match first entry");

    let last_block_entries = reader.get_block(block_count - 1).await.unwrap();
    let last_block_first_key = &last_block_entries[0].composite_key;
    assert_eq!(
        max, last_block_first_key,
        "max key should equal the first entry's key of the last block"
    );
}

#[tokio::test]
async fn test_entry_count_matches() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let count = 42;
    let entries = make_entries(count);

    let reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    assert_eq!(reader.entry_count(), count as u64);
}

#[tokio::test]
async fn test_contains_record_present() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(20);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    assert!(reader.contains_record(b"record_0005").unwrap());
}

#[tokio::test]
async fn test_contains_record_absent() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(20);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    // Bloom filter may have false positives, but "zzzzz_nonexistent" is very unlikely to match
    let result = reader.contains_record(b"zzzzz_nonexistent").unwrap();
    // We can't guarantee false here due to bloom FP, but for a well-sized filter this is safe
    assert!(!result, "expected absent record to not be contained by bloom filter");
}

// --- Dedup Tests ---

#[tokio::test]
async fn test_check_dedup_present_token() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(10);
    let token = entries[5].idempotency_key;

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    assert!(
        reader.check_dedup(&token).unwrap(),
        "token from written entry should be found in dedup block"
    );
}

#[tokio::test]
async fn test_check_dedup_absent_token() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(10);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let random_token = IdempotencyToken::from_parts(999999, [0xAA; 16]);
    assert!(
        !reader.check_dedup(&random_token).unwrap(),
        "random token should not be in dedup block"
    );
}

// --- Iterator Tests ---

#[tokio::test]
async fn test_iterator_all_entries_sorted() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(50);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let mut iter = SstableIterator::new(&reader);
    let all = collect_all(&mut iter).await;
    assert_eq!(all.len(), 50);

    for i in 1..all.len() {
        assert!(
            all[i].composite_key.as_bytes() >= all[i - 1].composite_key.as_bytes(),
            "iterator entries should be in sorted order at index {i}"
        );
    }
}

#[tokio::test]
async fn test_iterator_crosses_block_boundaries() {
    let tmp = TempDir::new().unwrap();
    let config = SstConfig::default()
        .with_compression(CompressionType::None)
        .with_block_size(128);
    let entries = make_entries(100);
    let expected_keys: Vec<_> = entries.iter().map(|e| e.composite_key.clone()).collect();

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let block_count = reader.block_count().unwrap();
    assert!(
        block_count > 1,
        "need multiple blocks for boundary test, got {block_count}"
    );

    let mut iter = SstableIterator::new(&reader);
    let all = collect_all(&mut iter).await;
    assert_eq!(all.len(), 100);

    for (i, entry) in all.iter().enumerate() {
        assert_eq!(
            entry.composite_key, expected_keys[i],
            "key mismatch at index {i} across block boundaries"
        );
    }
}

// --- Accessor Tests ---

#[tokio::test]
async fn test_reader_accessors() {
    let tmp = TempDir::new().unwrap();
    let config = SstConfig::default().with_compression(CompressionType::Snappy);
    let entries = make_entries(25);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;

    // footer() is available immediately after open
    let footer = reader.footer();
    assert_eq!(footer.entry_count, 25);
    assert_eq!(footer.compression_type, CompressionType::Snappy);

    // Scalar accessors
    assert_eq!(reader.entry_count(), 25);
    assert!(reader.file_size() > 0);
    assert_eq!(reader.compression(), CompressionType::Snappy);

    // block_count requires metadata
    reader.load_metadata().await.unwrap();
    assert!(reader.block_count().unwrap() >= 1);
}

#[tokio::test]
async fn test_get_block_out_of_range() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(5);

    let mut reader = write_and_open_with_dir(tmp.path(), &config, entries).await;
    reader.load_metadata().await.unwrap();

    let block_count = reader.block_count().unwrap();
    let result = reader.get_block(block_count + 10).await;
    assert!(
        matches!(result, Err(FlushError::InvalidArgument { .. })),
        "block index out of range should yield InvalidArgument"
    );
}

// --- Error Handling Tests ---

#[tokio::test]
async fn test_scan_without_metadata_errors() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(5);
    let start_key = entries[0].composite_key.clone();

    let reader = write_and_open_with_dir(tmp.path(), &config, entries).await;

    let result = reader.scan(&start_key, None).await;
    assert!(
        matches!(result, Err(FlushError::InvalidArgument { .. })),
        "scan without load_metadata should yield InvalidArgument"
    );
}

#[tokio::test]
async fn test_contains_record_without_metadata_errors() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(5);

    let reader = write_and_open_with_dir(tmp.path(), &config, entries).await;

    let result = reader.contains_record(b"record_0000");
    assert!(
        matches!(result, Err(FlushError::InvalidArgument { .. })),
        "contains_record without load_metadata should yield InvalidArgument"
    );
}

#[tokio::test]
async fn test_check_dedup_without_metadata_errors() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(5);

    let reader = write_and_open_with_dir(tmp.path(), &config, entries).await;

    let result = reader.check_dedup(&IdempotencyToken::new(1));
    assert!(
        matches!(result, Err(FlushError::InvalidArgument { .. })),
        "check_dedup without load_metadata should yield InvalidArgument"
    );
}

#[tokio::test]
async fn test_get_without_metadata_errors() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(5);
    let target_key = entries[2].composite_key.clone();

    // Open but do NOT call load_metadata
    let reader = write_and_open_with_dir(tmp.path(), &config, entries).await;

    let result = reader.get(&target_key).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("metadata"),
                "expected 'metadata' in message, got: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_get_block_without_metadata_errors() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(5);

    let reader = write_and_open_with_dir(tmp.path(), &config, entries).await;

    let result = reader.get_block(0).await;
    assert!(
        matches!(result, Err(FlushError::InvalidArgument { .. })),
        "get_block without load_metadata should yield InvalidArgument"
    );
}

#[tokio::test]
async fn test_key_range_without_metadata_errors() {
    let tmp = TempDir::new().unwrap();
    let config = default_config();
    let entries = make_entries(5);

    let reader = write_and_open_with_dir(tmp.path(), &config, entries).await;

    let result = reader.key_range();
    assert!(
        matches!(result, Err(FlushError::InvalidArgument { .. })),
        "key_range without load_metadata should yield InvalidArgument"
    );
}
