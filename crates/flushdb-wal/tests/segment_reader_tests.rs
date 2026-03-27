use std::io::Write;

use bytes::Bytes;
use flushdb_types::{EntryType, FlushError, IdempotencyToken};
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

fn write_entries(dir: &std::path::Path, entries: &[WalEntry]) -> std::path::PathBuf {
    let mut writer = SegmentWriter::create(dir, 1, 1).unwrap();
    for entry in entries {
        writer.append(entry).unwrap();
    }
    writer.sync().unwrap();
    writer.path().to_path_buf()
}

// === Happy Path Tests ===

#[test]
fn test_read_single_entry() {
    let dir = tempfile::tempdir().unwrap();
    let entry = make_entry(1);
    let path = write_entries(dir.path(), std::slice::from_ref(&entry));

    let mut reader = SegmentReader::open(&path).unwrap();
    let read = reader.next_entry().unwrap().unwrap();
    assert_eq!(entry, read);
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_read_multiple_entries() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=100).map(make_entry).collect();
    let path = write_entries(dir.path(), &entries);

    let mut reader = SegmentReader::open(&path).unwrap();
    for expected in &entries {
        let actual = reader.next_entry().unwrap().unwrap();
        assert_eq!(expected, &actual);
    }
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_read_empty_segment() {
    let dir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let path = writer.path().to_path_buf();
    drop(writer);

    let mut reader = SegmentReader::open(&path).unwrap();
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_header_parsed_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::create(dir.path(), 42, 100).unwrap();
    let path = writer.path().to_path_buf();
    drop(writer);

    let reader = SegmentReader::open(&path).unwrap();
    assert_eq!(reader.header().segment_number, 42);
    assert_eq!(reader.header().starting_sequence_number, 100);
    assert_eq!(reader.segment_number(), 42);
}

#[test]
fn test_entries_iterator() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=5).map(make_entry).collect();
    let path = write_entries(dir.path(), &entries);

    let reader = SegmentReader::open(&path).unwrap();
    let read: Vec<WalEntry> = reader.entries().map(|r| r.unwrap()).collect();
    assert_eq!(entries, read);
}

// === Corruption Detection Tests ===

#[test]
fn test_tail_truncation_extra_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=3).map(make_entry).collect();
    let path = write_entries(dir.path(), &entries);

    // Append random bytes at the end
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06])
        .unwrap();
    drop(file);

    let mut reader = SegmentReader::open(&path).unwrap();
    for _ in 0..3 {
        assert!(reader.next_entry().unwrap().is_some());
    }
    // Tail garbage should result in None (not an error)
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_tail_truncation_partial_length_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=3).map(make_entry).collect();
    let path = write_entries(dir.path(), &entries);

    // Append only 2 bytes (incomplete length prefix)
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    file.write_all(&[0x01, 0x02]).unwrap();
    drop(file);

    let mut reader = SegmentReader::open(&path).unwrap();
    for _ in 0..3 {
        assert!(reader.next_entry().unwrap().is_some());
    }
    // Partial length prefix at tail → None
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_tail_truncation_partial_body() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=3).map(make_entry).collect();
    let path = write_entries(dir.path(), &entries);

    // Append a valid entry_length but incomplete body
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    let fake_length: u32 = 100;
    file.write_all(&fake_length.to_le_bytes()).unwrap();
    file.write_all(&[0u8; 10]).unwrap(); // only 10 of 100 body bytes
    drop(file);

    let mut reader = SegmentReader::open(&path).unwrap();
    for _ in 0..3 {
        assert!(reader.next_entry().unwrap().is_some());
    }
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_mid_segment_corruption_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=5).map(make_entry).collect();
    let path = write_entries(dir.path(), &entries);

    // Corrupt a byte in entry 3's body (offset into the file)
    let mut data = std::fs::read(&path).unwrap();
    // Find approximate location of entry 3
    let mut offset = SEGMENT_HEADER_SIZE;
    for _ in 0..2 {
        let len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4 + len + 4;
    }
    // Now at entry 3, corrupt a body byte
    data[offset + 10] ^= 0xFF;
    std::fs::write(&path, &data).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    // First 2 entries should read fine
    assert!(reader.next_entry().unwrap().is_some());
    assert!(reader.next_entry().unwrap().is_some());
    // Entry 3 should fail with CrcMismatch
    let result = reader.next_entry();
    assert!(matches!(result, Err(FlushError::CrcMismatch { .. })));
}

#[test]
fn test_zero_entry_length_stops_iteration() {
    let dir = tempfile::tempdir().unwrap();
    let entries: Vec<WalEntry> = (1..=2).map(make_entry).collect();
    let path = write_entries(dir.path(), &entries);

    // Read file, insert 4 zero bytes after entry 1
    let data = std::fs::read(&path).unwrap();
    let mut offset = SEGMENT_HEADER_SIZE;
    let len = u32::from_le_bytes([
        data[offset],
        data[offset + 1],
        data[offset + 2],
        data[offset + 3],
    ]) as usize;
    offset += 4 + len + 4; // past entry 1

    let mut new_data = Vec::new();
    new_data.extend_from_slice(&data[..offset]);
    new_data.extend_from_slice(&[0u8; 4]); // zero entry_length
    new_data.extend_from_slice(&data[offset..]);
    std::fs::write(&path, &new_data).unwrap();

    let mut reader = SegmentReader::open(&path).unwrap();
    assert!(reader.next_entry().unwrap().is_some()); // entry 1
    assert!(reader.next_entry().unwrap().is_none()); // zero length stops
}

// === Edge Cases ===

#[test]
fn test_segment_with_only_header() {
    let dir = tempfile::tempdir().unwrap();
    let writer = SegmentWriter::create(dir.path(), 1, 1).unwrap();
    let path = writer.path().to_path_buf();
    drop(writer);

    let mut reader = SegmentReader::open(&path).unwrap();
    assert!(reader.next_entry().unwrap().is_none());
}

#[test]
fn test_entry_with_empty_variable_fields() {
    let dir = tempfile::tempdir().unwrap();
    let entry = WalEntry {
        sequence_number: 1,
        entry_type: EntryType::Put,
        namespace: Bytes::new(),
        record_id: Bytes::new(),
        item_key: Bytes::new(),
        item_value: Bytes::new(),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    let path = write_entries(dir.path(), std::slice::from_ref(&entry));

    let mut reader = SegmentReader::open(&path).unwrap();
    let read = reader.next_entry().unwrap().unwrap();
    assert_eq!(entry, read);
}

#[test]
fn test_entry_with_large_value() {
    let dir = tempfile::tempdir().unwrap();
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
    let path = write_entries(dir.path(), std::slice::from_ref(&entry));

    let mut reader = SegmentReader::open(&path).unwrap();
    let read = reader.next_entry().unwrap().unwrap();
    assert_eq!(entry, read);
}

#[test]
fn test_binary_data_in_all_fields() {
    let dir = tempfile::tempdir().unwrap();
    let binary: Vec<u8> = (1..=255).collect(); // skip 0x00 for record_id
    let entry = WalEntry {
        sequence_number: 1,
        entry_type: EntryType::Put,
        namespace: Bytes::copy_from_slice(&binary[..128]),
        record_id: Bytes::copy_from_slice(&binary[..100]),
        item_key: Bytes::copy_from_slice(&binary),
        item_value: Bytes::copy_from_slice(&binary),
        item_metadata: Bytes::copy_from_slice(&binary[..64]),
        idempotency_token: IdempotencyToken::none(),
    };
    let path = write_entries(dir.path(), std::slice::from_ref(&entry));

    let mut reader = SegmentReader::open(&path).unwrap();
    let read = reader.next_entry().unwrap().unwrap();
    assert_eq!(entry, read);
}

// === Header Validation Tests ===

#[test]
fn test_open_rejects_empty_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("empty.wal");
    std::fs::write(&path, []).unwrap();
    let result = SegmentReader::open(&path);
    assert!(result.is_err());
}

#[test]
fn test_open_rejects_short_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.wal");
    std::fs::write(&path, [0u8; 16]).unwrap();
    let result = SegmentReader::open(&path);
    assert!(result.is_err());
}

#[test]
fn test_open_rejects_bad_magic() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.wal");
    let mut data = [0u8; 32];
    data[0..4].copy_from_slice(b"XWAL");
    data[4] = 1;
    std::fs::write(&path, data).unwrap();
    let result = SegmentReader::open(&path);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}
