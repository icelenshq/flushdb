use bytes::Bytes;
use flushdb_engine::sstable::reader::{SSTableReader, SstableIterator};
use flushdb_engine::sstable::writer::SSTableWriter;
use flushdb_engine::sstable::{CompressionType, SstConfig};
use flushdb_engine::{Memtable, MemtableConfig};
use flushdb_types::{
    CompositeKey, EntryType, EntryValue, IdempotencyToken, LocalFsBackend, MemtableEntry,
    StorageBackend,
};
use tempfile::TempDir;

fn node_to_entry(node: flushdb_engine::SkipNode) -> MemtableEntry {
    MemtableEntry::with_sequence(
        node.key,
        node.value,
        node.metadata,
        node.idempotency_key,
        node.sequence_number,
        node.entry_type,
    )
}

#[tokio::test]
async fn test_memtable_to_sstable_round_trip() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = SstConfig::default().with_compression(CompressionType::None);

    let mut memtable = Memtable::new(MemtableConfig::default(), 1);

    for i in 0..50 {
        let key = CompositeKey::new(
            format!("record_{:04}", i).as_bytes(),
            format!("item_{:04}", i).as_bytes(),
        )
        .unwrap();
        let entry = MemtableEntry::new(
            key,
            Bytes::from(format!("value_{i}")),
            Bytes::from(format!("meta_{i}")),
            IdempotencyToken::new(i as u64 + 100),
            EntryType::Put,
        );
        memtable.insert(entry).unwrap();
    }

    assert_eq!(memtable.entry_count(), 50);

    memtable.freeze();
    let skiplist = memtable.into_skiplist();

    // Collect entries from the skiplist via its IntoIterator
    let memtable_entries: Vec<MemtableEntry> = skiplist.into_iter().map(node_to_entry).collect();
    assert_eq!(memtable_entries.len(), 50);

    // Write SSTable
    let writer = SSTableWriter::new(config);
    let info = writer
        .write(&backend, "roundtrip.sst", memtable_entries.iter().cloned())
        .await
        .unwrap();

    assert_eq!(info.entry_count, 50);

    // Read it back
    let data = backend.get("roundtrip.sst").await.unwrap();
    let file_size = data.len() as u64;
    let reader_backend = LocalFsBackend::new(tmp.path());
    let mut reader =
        SSTableReader::open(reader_backend, "roundtrip.sst".to_string(), file_size)
            .await
            .unwrap();
    reader.load_metadata().await.unwrap();

    assert_eq!(reader.entry_count(), 50);

    // Iterate and verify all entries match
    let mut iter = SstableIterator::new(&reader);
    let mut read_count = 0;
    let mut entry_idx = 0;
    while let Some(block_entry) = iter.next().await.unwrap() {
        let expected = &memtable_entries[entry_idx];
        assert_eq!(
            block_entry.composite_key, expected.composite_key,
            "key mismatch at index {entry_idx}"
        );
        assert_eq!(
            block_entry.value,
            EntryValue::Inline(expected.value.clone()),
            "value mismatch at index {entry_idx}"
        );
        assert_eq!(
            block_entry.metadata, expected.metadata,
            "metadata mismatch at index {entry_idx}"
        );
        assert_eq!(
            block_entry.entry_type, expected.entry_type,
            "entry_type mismatch at index {entry_idx}"
        );
        read_count += 1;
        entry_idx += 1;
    }
    assert_eq!(read_count, 50);
}

#[tokio::test]
async fn test_sstable_with_range_tombstones() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = SstConfig::default().with_compression(CompressionType::None);

    let mut memtable = Memtable::new(MemtableConfig::default(), 1);

    // Insert Put entries
    for i in 0..10 {
        let key = CompositeKey::new(
            format!("record_{:04}", i).as_bytes(),
            format!("item_{:04}", i).as_bytes(),
        )
        .unwrap();
        let entry = MemtableEntry::new(
            key,
            Bytes::from(format!("value_{i}")),
            Bytes::from(format!("meta_{i}")),
            IdempotencyToken::new(i as u64 + 100),
            EntryType::Put,
        );
        memtable.insert(entry).unwrap();
    }

    // Insert Delete entry
    let del_key = CompositeKey::new(b"record_0020", b"item_0020").unwrap();
    let del_entry = MemtableEntry::new(
        del_key,
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::new(200),
        EntryType::Delete,
    );
    memtable.insert(del_entry).unwrap();

    // Insert RangeDelete entry
    let range_key = CompositeKey::new(b"record_0030", b"start_range").unwrap();
    let range_entry = MemtableEntry::new(
        range_key,
        Bytes::from("end_range"),
        Bytes::new(),
        IdempotencyToken::new(300),
        EntryType::RangeDelete,
    );
    memtable.insert(range_entry).unwrap();

    memtable.freeze();
    let skiplist = memtable.into_skiplist();
    let memtable_entries: Vec<MemtableEntry> = skiplist.into_iter().map(node_to_entry).collect();

    // Write SSTable
    let writer = SSTableWriter::new(config);
    let info = writer
        .write(&backend, "tombstones.sst", memtable_entries.iter().cloned())
        .await
        .unwrap();
    assert_eq!(info.entry_count, memtable_entries.len() as u64);

    // Read back
    let data = backend.get("tombstones.sst").await.unwrap();
    let file_size = data.len() as u64;
    let reader_backend = LocalFsBackend::new(tmp.path());
    let mut reader =
        SSTableReader::open(reader_backend, "tombstones.sst".to_string(), file_size)
            .await
            .unwrap();
    reader.load_metadata().await.unwrap();

    let mut iter = SstableIterator::new(&reader);
    let mut puts = Vec::new();
    let mut deletes = Vec::new();
    let mut range_deletes = Vec::new();
    while let Some(entry) = iter.next().await.unwrap() {
        match entry.entry_type {
            EntryType::Put => puts.push(entry),
            EntryType::Delete => deletes.push(entry),
            EntryType::RangeDelete => range_deletes.push(entry),
        }
    }

    assert!(!puts.is_empty(), "should contain Put entries");
    assert_eq!(deletes.len(), 1, "should contain 1 Delete entry");
    assert_eq!(range_deletes.len(), 1, "should contain 1 RangeDelete entry");

    // Verify Put values survived
    for put in &puts {
        assert!(put.value.is_inline(), "Put value should be Inline");
        assert!(!put.value.inline_value().unwrap().is_empty(), "Put value should not be empty");
    }

    // Verify Delete has empty value
    assert_eq!(deletes[0].value, EntryValue::Inline(Bytes::new()));

    // Verify RangeDelete has its end_key value
    assert_eq!(
        deletes[0].composite_key.record_id(),
        b"record_0020"
    );
    assert_eq!(
        range_deletes[0].value,
        EntryValue::Inline(Bytes::from("end_range"))
    );
}

#[tokio::test]
async fn test_sstable_large_entry_count() {
    let tmp = TempDir::new().unwrap();
    let backend = LocalFsBackend::new(tmp.path());
    let config = SstConfig::default().with_compression(CompressionType::Snappy);

    let count = 10_000;
    let entries: Vec<MemtableEntry> = (0..count)
        .map(|i| {
            let record_id = format!("record_{:06}", i);
            let item_key = format!("item_{:06}", i);
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
        .collect();

    let writer = SSTableWriter::new(config);
    let info = writer
        .write(&backend, "large.sst", entries.iter().cloned())
        .await
        .unwrap();
    assert_eq!(info.entry_count, count as u64);

    // Open and load
    let data = backend.get("large.sst").await.unwrap();
    let file_size = data.len() as u64;
    let reader_backend = LocalFsBackend::new(tmp.path());
    let mut reader = SSTableReader::open(reader_backend, "large.sst".to_string(), file_size)
        .await
        .unwrap();
    reader.load_metadata().await.unwrap();

    assert_eq!(reader.entry_count(), count as u64);

    let block_count = reader.block_count().unwrap();
    assert!(
        block_count > 1,
        "10,000 entries should produce multiple blocks, got {block_count}"
    );

    // Random point lookups
    for &idx in &[0, 100, 999, 5000, 7777, 9999] {
        let key = CompositeKey::new(
            format!("record_{:06}", idx).as_bytes(),
            format!("item_{:06}", idx).as_bytes(),
        )
        .unwrap();
        let result = reader.get(&key).await.unwrap();
        assert!(
            result.is_some(),
            "point lookup for entry {idx} should succeed"
        );
        let entry = result.unwrap();
        assert_eq!(entry.composite_key, key);
        assert_eq!(
            entry.value,
            EntryValue::Inline(Bytes::from(format!("value_{idx}")))
        );
    }

    // Verify a nonexistent key returns None
    let absent = CompositeKey::new(b"zzzzz_absent", b"item_absent").unwrap();
    assert!(reader.get(&absent).await.unwrap().is_none());

    // Full iteration: verify count and sorted order
    let mut iter = SstableIterator::new(&reader);
    let mut prev_key: Option<Bytes> = None;
    let mut iter_count = 0;
    while let Some(entry) = iter.next().await.unwrap() {
        if let Some(ref pk) = prev_key {
            assert!(
                entry.composite_key.as_bytes() >= pk.as_ref(),
                "iterator entries should be in sorted order at index {iter_count}"
            );
        }
        prev_key = Some(Bytes::copy_from_slice(entry.composite_key.as_bytes()));
        iter_count += 1;
    }
    assert_eq!(iter_count, count);
}
