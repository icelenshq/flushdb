use bytes::Bytes;
use flushdb_engine::sstable::bloom_filter::FilterBlock;
use flushdb_engine::sstable::footer::{SstFooter, SstHeader};
use flushdb_engine::sstable::index_block::IndexBlock;
use flushdb_engine::sstable::writer::{generate_run_fragment_path, generate_sst_path, SSTableWriter};
use flushdb_engine::sstable::{CompressionType, SstConfig, FOOTER_SIZE, HEADER_SIZE};
use flushdb_types::{
    CompositeKey, EntryType, FlushError, IdempotencyToken, LocalFsBackend, MemtableEntry,
    StorageBackend,
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

async fn write_sst(
    backend: &LocalFsBackend,
    config: &SstConfig,
    entries: Vec<MemtableEntry>,
) -> flushdb_types::FlushResult<flushdb_engine::sstable::writer::SstInfo> {
    let writer = SSTableWriter::new(config.clone());
    writer
        .write(backend, "test.sst", entries.into_iter())
        .await
}

// --- Basic Write Tests ---

#[tokio::test]
async fn test_write_single_entry() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(1);

    let info = write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    assert!(!data.is_empty());

    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();
    assert_eq!(footer.entry_count, 1);
    assert_eq!(info.entry_count, 1);
}

#[tokio::test]
async fn test_write_100_entries() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(100);

    let info = write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    assert!(!data.is_empty());

    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();
    assert_eq!(footer.entry_count, 100);
    assert_eq!(info.entry_count, 100);
}

#[tokio::test]
async fn test_write_creates_file_at_path() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(5);

    let writer = SSTableWriter::new(config);
    writer
        .write(&backend, "subdir/my_table.sst", entries.into_iter())
        .await
        .unwrap();

    let data = backend.get("subdir/my_table.sst").await.unwrap();
    assert!(!data.is_empty());
}

#[tokio::test]
async fn test_write_returns_correct_sst_info() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(10);
    let first_key = entries[0].composite_key.clone();
    let last_key = entries[9].composite_key.clone();

    let info = write_sst(&backend, &config, entries).await.unwrap();

    assert_eq!(info.path, "test.sst");
    assert_eq!(info.entry_count, 10);
    assert_eq!(info.min_key, first_key);
    assert_eq!(info.max_key, last_key);
    assert!(info.file_size > 0);
}

// --- Block Rotation Tests ---

#[tokio::test]
async fn test_write_rotates_blocks_at_target() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    // Use a very small block size to force block rotation with fewer entries
    let config = SstConfig::default()
        .with_compression(CompressionType::None)
        .with_block_size(256);
    let entries = make_entries(100);

    write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();

    // With 100 entries and a 256-byte block target, there should be multiple blocks
    let index_bytes = &data[footer.index_block_offset as usize
        ..(footer.index_block_offset as usize + footer.index_block_size as usize)];
    let index = IndexBlock::deserialize(index_bytes).unwrap();
    assert!(
        index.block_count() > 1,
        "expected multiple blocks, got {}",
        index.block_count()
    );
}

#[tokio::test]
async fn test_write_index_entries_match_block_count() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = SstConfig::default()
        .with_compression(CompressionType::None)
        .with_block_size(256);
    let entries = make_entries(50);

    write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();

    let index_bytes = &data[footer.index_block_offset as usize
        ..(footer.index_block_offset as usize + footer.index_block_size as usize)];
    let index = IndexBlock::deserialize(index_bytes).unwrap();

    // Each block produces exactly one index entry
    assert!(index.block_count() >= 1);
    // Verify that the number of index entries is consistent: each index entry
    // references a data block at a unique offset
    let mut offsets: Vec<u64> = (0..index.block_count())
        .map(|i| index.get(i).unwrap().block_offset)
        .collect();
    offsets.sort();
    offsets.dedup();
    assert_eq!(offsets.len(), index.block_count());
}

// --- Compression Tests ---

#[tokio::test]
async fn test_write_compression_none() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = SstConfig::default().with_compression(CompressionType::None);
    let entries = make_entries(20);

    write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();
    assert_eq!(footer.compression_type, CompressionType::None);
}

#[tokio::test]
async fn test_write_compression_snappy() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let entries = make_entries(200);
    let entries_clone = entries.clone();

    // Write uncompressed
    let config_none = SstConfig::default().with_compression(CompressionType::None);
    let writer_none = SSTableWriter::new(config_none);
    writer_none
        .write(&backend, "none.sst", entries.into_iter())
        .await
        .unwrap();
    let size_none = backend.get("none.sst").await.unwrap().len();

    // Write snappy
    let config_snappy = SstConfig::default().with_compression(CompressionType::Snappy);
    let writer_snappy = SSTableWriter::new(config_snappy);
    writer_snappy
        .write(&backend, "snappy.sst", entries_clone.into_iter())
        .await
        .unwrap();
    let data_snappy = backend.get("snappy.sst").await.unwrap();
    let size_snappy = data_snappy.len();
    let footer = SstFooter::decode(&data_snappy[data_snappy.len() - FOOTER_SIZE..]).unwrap();

    assert_eq!(footer.compression_type, CompressionType::Snappy);
    assert!(
        size_snappy < size_none,
        "snappy ({size_snappy}) should be smaller than none ({size_none})"
    );
}

#[tokio::test]
async fn test_write_compression_zstd() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let entries = make_entries(200);
    let entries_clone = entries.clone();

    let config_none = SstConfig::default().with_compression(CompressionType::None);
    let writer_none = SSTableWriter::new(config_none);
    writer_none
        .write(&backend, "none.sst", entries.into_iter())
        .await
        .unwrap();
    let size_none = backend.get("none.sst").await.unwrap().len();

    let config_zstd = SstConfig::default().with_compression(CompressionType::Zstd);
    let writer_zstd = SSTableWriter::new(config_zstd);
    writer_zstd
        .write(&backend, "zstd.sst", entries_clone.into_iter())
        .await
        .unwrap();
    let data_zstd = backend.get("zstd.sst").await.unwrap();
    let size_zstd = data_zstd.len();
    let footer = SstFooter::decode(&data_zstd[data_zstd.len() - FOOTER_SIZE..]).unwrap();

    assert_eq!(footer.compression_type, CompressionType::Zstd);
    assert!(
        size_zstd < size_none,
        "zstd ({size_zstd}) should be smaller than none ({size_none})"
    );
}

// --- Metadata Embedding Tests ---

#[tokio::test]
async fn test_write_bloom_filter_readable() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(20);

    write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();

    let bloom_bytes = &data[footer.bloom_filter_offset as usize
        ..(footer.bloom_filter_offset as usize + footer.bloom_filter_size as usize)];
    let filter = FilterBlock::deserialize(bloom_bytes).expect("bloom filter deserialization failed");

    // Verify bloom filter contains the record_ids that were written
    assert!(filter.maybe_contains(b"record_0000"));
    assert!(filter.maybe_contains(b"record_0010"));
    assert!(filter.maybe_contains(b"record_0019"));
}

#[tokio::test]
async fn test_write_dedup_block_has_tokens() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(10);

    let info = write_sst(&backend, &config, entries).await.unwrap();

    // Entries have non-none idempotency tokens, so dedup block should have data
    assert!(
        info.dedup_block_size > 4,
        "dedup_block_size should be > 4 (header only) when entries have tokens, got {}",
        info.dedup_block_size
    );
}

#[tokio::test]
async fn test_write_footer_min_max_keys() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(10);
    let first_key = entries[0].composite_key.clone();
    let last_key = entries[9].composite_key.clone();

    write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();

    let expected_min = SstFooter::truncate_key(&first_key);
    let expected_max = SstFooter::truncate_key(&last_key);
    assert_eq!(footer.min_key, expected_min);
    assert_eq!(footer.max_key, expected_max);
}

#[tokio::test]
async fn test_write_header_consistent_with_footer() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(25);

    write_sst(&backend, &config, entries).await.unwrap();

    let data = backend.get("test.sst").await.unwrap();
    let header = SstHeader::decode(&data[..HEADER_SIZE]).unwrap();
    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();

    assert_eq!(header.entry_count, footer.entry_count);
    assert_eq!(header.compression, footer.compression_type);
}

// --- Edge Cases ---

#[tokio::test]
async fn test_write_empty_iterator_errors() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();

    let result = write_sst(&backend, &config, vec![]).await;

    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("empty"),
                "expected 'empty' in message, got: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_write_all_same_record_id() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();

    let entries: Vec<MemtableEntry> = (0..50)
        .map(|i| {
            let key =
                CompositeKey::new(b"same_record", format!("item_{:04}", i).as_bytes()).unwrap();
            MemtableEntry::with_sequence(
                key,
                Bytes::from(format!("value_{i}")),
                Bytes::from(format!("meta_{i}")),
                IdempotencyToken::new(i as u64 + 1),
                i as u64 + 1,
                EntryType::Put,
            )
        })
        .collect();

    let info = write_sst(&backend, &config, entries).await.unwrap();
    assert_eq!(info.entry_count, 50);

    let data = backend.get("test.sst").await.unwrap();
    let footer = SstFooter::decode(&data[data.len() - FOOTER_SIZE..]).unwrap();
    assert_eq!(footer.entry_count, 50);
}

#[tokio::test]
async fn test_write_entries_with_tombstones() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();

    let mut entries = Vec::new();

    // Put entry
    let key_put = CompositeKey::new(b"rec_a", b"item_a").unwrap();
    entries.push(MemtableEntry::with_sequence(
        key_put,
        Bytes::from("val_a"),
        Bytes::from("meta_a"),
        IdempotencyToken::new(1),
        1,
        EntryType::Put,
    ));

    // Delete entry
    let key_del = CompositeKey::new(b"rec_b", b"item_b").unwrap();
    entries.push(MemtableEntry::with_sequence(
        key_del,
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::new(2),
        2,
        EntryType::Delete,
    ));

    // RangeDelete entry
    let key_range = CompositeKey::new(b"rec_c", b"item_c").unwrap();
    entries.push(MemtableEntry::with_sequence(
        key_range,
        Bytes::from("end_key"),
        Bytes::new(),
        IdempotencyToken::new(3),
        3,
        EntryType::RangeDelete,
    ));

    // Sort entries by key for SSTable ordering
    entries.sort_by(|a, b| a.composite_key.cmp(&b.composite_key));

    let info = write_sst(&backend, &config, entries).await.unwrap();
    assert_eq!(info.entry_count, 3);
}

#[tokio::test]
async fn test_write_entries_with_none_tokens() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();

    let entries: Vec<MemtableEntry> = (0..10)
        .map(|i| {
            let key = CompositeKey::new(
                format!("record_{:04}", i).as_bytes(),
                format!("item_{:04}", i).as_bytes(),
            )
            .unwrap();
            MemtableEntry::with_sequence(
                key,
                Bytes::from(format!("value_{i}")),
                Bytes::from(format!("meta_{i}")),
                IdempotencyToken::none(),
                i as u64 + 1,
                EntryType::Put,
            )
        })
        .collect();

    let info = write_sst(&backend, &config, entries).await.unwrap();
    // When all tokens are none, the dedup block should only have its 4-byte count header
    assert_eq!(
        info.dedup_block_size, 4,
        "dedup_block_size should be 4 for all-none tokens, got {}",
        info.dedup_block_size
    );
}

// --- write_entries() Tests ---

#[tokio::test]
async fn test_write_entries_round_trip() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();
    let entries = make_entries(30);

    // Write initial SSTable with MemtableEntry iterator
    let writer = SSTableWriter::new(config.clone());
    writer
        .write(&backend, "original.sst", entries.into_iter())
        .await
        .unwrap();

    // Read entries back as BlockEntry
    let data = backend.get("original.sst").await.unwrap();
    let file_size = data.len() as u64;
    let reader_backend = LocalFsBackend::new(tmp.path());
    let mut reader =
        flushdb_engine::sstable::reader::SSTableReader::open(
            reader_backend,
            "original.sst".to_string(),
            file_size,
        )
        .await
        .unwrap();
    reader.load_metadata().await.unwrap();

    let mut iter = flushdb_engine::sstable::reader::SstableIterator::new(&reader);
    let mut block_entries = Vec::new();
    while let Some(entry) = iter.next().await.unwrap() {
        block_entries.push(entry);
    }
    assert_eq!(block_entries.len(), 30);

    // Rewrite using write_entries()
    let rewriter = SSTableWriter::new(config);
    let info = rewriter
        .write_entries(&backend, "rewritten.sst", block_entries.iter().cloned())
        .await
        .unwrap();

    assert_eq!(info.entry_count, 30);
    assert_eq!(info.path, "rewritten.sst");

    // Read the rewritten SSTable and verify all entries match
    let rewritten_data = backend.get("rewritten.sst").await.unwrap();
    let rewritten_size = rewritten_data.len() as u64;
    let rewritten_backend = LocalFsBackend::new(tmp.path());
    let mut rewritten_reader =
        flushdb_engine::sstable::reader::SSTableReader::open(
            rewritten_backend,
            "rewritten.sst".to_string(),
            rewritten_size,
        )
        .await
        .unwrap();
    rewritten_reader.load_metadata().await.unwrap();

    let mut rewritten_iter =
        flushdb_engine::sstable::reader::SstableIterator::new(&rewritten_reader);
    let mut rewritten_entries = Vec::new();
    while let Some(entry) = rewritten_iter.next().await.unwrap() {
        rewritten_entries.push(entry);
    }

    assert_eq!(rewritten_entries.len(), 30);
    for (i, (orig, rewritten)) in block_entries.iter().zip(rewritten_entries.iter()).enumerate() {
        assert_eq!(
            orig.composite_key, rewritten.composite_key,
            "key mismatch at index {i}"
        );
        assert_eq!(
            orig.value, rewritten.value,
            "value mismatch at index {i}"
        );
        assert_eq!(
            orig.entry_type, rewritten.entry_type,
            "entry_type mismatch at index {i}"
        );
        assert_eq!(
            orig.sequence_number, rewritten.sequence_number,
            "sequence_number mismatch at index {i}"
        );
    }
}

#[tokio::test]
async fn test_write_entries_empty_iterator_errors() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = default_config();

    let writer = SSTableWriter::new(config);
    let result = writer
        .write_entries(
            &backend,
            "empty.sst",
            std::iter::empty::<flushdb_engine::sstable::block_reader::BlockEntry>(),
        )
        .await;

    assert!(matches!(result, Err(FlushError::InvalidArgument { .. })));
}

// --- Path Utility Tests ---

#[tokio::test]
async fn test_generate_sst_path_format() {
    let path = generate_sst_path("my_namespace", 2);
    assert!(
        path.starts_with("flushdb/my_namespace/sstables/L2/"),
        "path should start with expected prefix, got: {path}"
    );
    assert!(
        path.ends_with(".sst"),
        "path should end with .sst, got: {path}"
    );
}

#[tokio::test]
async fn test_generate_run_fragment_path_format() {
    let path = generate_run_fragment_path("ns", 1, "run_abc", 3);
    assert_eq!(
        path,
        "flushdb/ns/sstables/L1/run-run_abc/frag-0003.sst"
    );
}
