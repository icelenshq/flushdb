use bytes::Bytes;
use flushdb_types::{EntryType, IdempotencyToken};
use flushdb_wal::{SegmentReader, SegmentWriter, WalEntry, SEGMENT_HEADER_SIZE};

fn make_entry(seq: u64) -> WalEntry {
    WalEntry {
        sequence_number: seq,
        entry_type: EntryType::Put,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"key"),
        item_value: Bytes::from(format!("value-{seq}")),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    }
}

// === Basic Write Tests ===

#[test]
fn test_create_segment_writes_header() {
    let dir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let path = writer.path().to_path_buf();
    drop(writer);

    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.segment_number(), 1);
    assert_eq!(reader.header().starting_sequence_number, 1);
}

#[test]
fn test_create_segment_size_is_header_size() {
    let dir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    assert_eq!(writer.current_size(), SEGMENT_HEADER_SIZE as u64);
}

#[test]
fn test_append_single_entry() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let entry = make_entry(1);
    let expected_increase = entry.total_size() as u64;
    writer.append(&entry).unwrap();
    assert_eq!(
        writer.current_size(),
        SEGMENT_HEADER_SIZE as u64 + expected_increase
    );
}

#[test]
fn test_append_multiple_entries() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let entries: Vec<WalEntry> = (1..=100).map(make_entry).collect();
    let total_entry_size: u64 = entries.iter().map(|e| e.total_size() as u64).sum();
    for entry in &entries {
        writer.append(entry).unwrap();
    }
    assert_eq!(
        writer.current_size(),
        SEGMENT_HEADER_SIZE as u64 + total_entry_size
    );
}

#[test]
fn test_append_batch_equivalent_to_individual() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=10).map(make_entry).collect();

    // Write individually
    let mut writer1 = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    for entry in &entries {
        writer1.append(entry).unwrap();
    }
    writer1.sync().unwrap();
    let path1 = writer1.path().to_path_buf();

    // Write as batch
    let dir2 = tempfile::tempdir().unwrap();
    let mut writer2 = SegmentWriter::create(dir2.path(), 1, 1).unwrap();
    writer2.append_batch(&entries).unwrap();
    writer2.sync().unwrap();
    let path2 = writer2.path().to_path_buf();

    // Compare file contents
    let data1 = std::fs::read(&path1).unwrap();
    let data2 = std::fs::read(&path2).unwrap();
    // Headers will differ (different created_at_ms), but entry data should match
    assert_eq!(&data1[SEGMENT_HEADER_SIZE..], &data2[SEGMENT_HEADER_SIZE..]);
}

#[test]
fn test_entry_count_tracks_appends() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    assert_eq!(writer.entry_count(), 0);

    writer.append(&make_entry(1)).unwrap();
    assert_eq!(writer.entry_count(), 1);

    writer.append(&make_entry(2)).unwrap();
    assert_eq!(writer.entry_count(), 2);

    let batch: Vec<WalEntry> = (3..=5).map(make_entry).collect();
    writer.append_batch(&batch).unwrap();
    assert_eq!(writer.entry_count(), 5);
}

#[test]
fn test_append_returns_correct_offset() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();

    let entry1 = make_entry(1);
    let offset1 = writer.append(&entry1).unwrap();
    assert_eq!(offset1, SEGMENT_HEADER_SIZE as u64);

    let entry2 = make_entry(2);
    let offset2 = writer.append(&entry2).unwrap();
    assert_eq!(offset2, SEGMENT_HEADER_SIZE as u64 + entry1.total_size() as u64);
}

// === File Content Tests ===

#[test]
fn test_written_bytes_are_decodable() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let entries: Vec<WalEntry> = (1..=10).map(make_entry).collect();
    for entry in &entries {
        writer.append(entry).unwrap();
    }
    writer.sync().unwrap();
    let path = writer.path().to_path_buf();
    drop(writer);

    let mut reader = SegmentReader::open(&path).unwrap();
    for expected in &entries {
        let actual = reader.next_entry().unwrap().unwrap();
        assert_eq!(expected, &actual);
    }
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_entries_written_contiguously() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let entries: Vec<WalEntry> = (1..=5).map(make_entry).collect();
    let total_entry_bytes: u64 = entries.iter().map(|e| e.total_size() as u64).sum();
    for entry in &entries {
        writer.append(entry).unwrap();
    }
    // File size should be exactly header + sum of entry sizes (no gaps)
    assert_eq!(
        writer.current_size(),
        SEGMENT_HEADER_SIZE as u64 + total_entry_bytes
    );
}

#[test]
fn test_large_entry_writes_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let entry = WalEntry {
        sequence_number: 1,
        entry_type: EntryType::Put,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"key"),
        item_value: Bytes::from(vec![0xAB; 1_048_576]),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    writer.append(&entry).unwrap();
    writer.sync().unwrap();
    let path = writer.path().to_path_buf();
    drop(writer);

    let mut reader = SegmentReader::open(&path).unwrap();
    let read_entry = reader.next_entry().unwrap().unwrap();
    assert_eq!(entry, read_entry);
}

// === Fsync Tests ===

#[test]
fn test_sync_completes_without_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    writer.append(&make_entry(1)).unwrap();
    writer.sync().unwrap();
}

#[test]
fn test_sync_after_no_writes() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    writer.sync().unwrap();
}

// === Error Tests ===

#[test]
fn test_create_fails_if_file_exists() {
    let dir = tempfile::tempdir().unwrap();
    let _writer1 = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let result = SegmentWriter::create(dir.path(), 1, 1);
    assert!(result.is_err());
}

#[test]
fn test_create_makes_parent_directories() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("deep").join("nested").join("path");
    let writer = SegmentWriter::create(&nested, 1, 1).unwrap();
    assert!(writer.path().exists());
}
