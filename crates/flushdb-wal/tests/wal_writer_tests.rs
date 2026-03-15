use bytes::Bytes;
use flushdb_types::{EntryType, IdempotencyToken};
use flushdb_wal::{
    segment_filename, SegmentReader, WalConfig, WalEntry, WalWriter, SEGMENT_HEADER_SIZE,
};

fn make_entry() -> WalEntry {
    WalEntry {
        sequence_number: 0, // assigned by writer
        entry_type: EntryType::Put,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"key"),
        item_value: Bytes::from_static(b"value"),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    }
}

fn small_config() -> WalConfig {
    WalConfig {
        segment_size_target: 1024, // very small for rotation tests
        ..WalConfig::default()
    }
}

// === Startup Tests ===

#[test]
fn test_open_creates_first_segment() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let writer = WalWriter::open(dir.path(), &config).unwrap();

    assert!(dir.path().join(segment_filename(1)).exists());
    assert_eq!(writer.current_segment_number(), 1);
    assert_eq!(writer.next_sequence_number(), 1);
}

#[test]
fn test_open_recovers_sequence_from_existing() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();

    // Write some entries
    {
        let mut writer = WalWriter::open(dir.path(), &config).unwrap();
        for _ in 0..10 {
            writer.append(&mut make_entry()).unwrap();
        }
        writer.sync().unwrap();
    }

    // Reopen
    let writer = WalWriter::open(dir.path(), &config).unwrap();
    assert_eq!(writer.next_sequence_number(), 11);
}

#[test]
fn test_open_recovers_from_empty_last_segment() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();

    {
        let mut writer = WalWriter::open(dir.path(), &config).unwrap();
        for _ in 0..5 {
            writer.append(&mut make_entry()).unwrap();
        }
        writer.rotate().unwrap(); // creates empty new segment
    }

    let writer = WalWriter::open(dir.path(), &config).unwrap();
    assert_eq!(writer.next_sequence_number(), 6);
}

#[test]
fn test_open_handles_gaps_in_segment_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();

    // Create segments 1, 3, 5 (with gaps)
    {
        let mut writer = WalWriter::open(dir.path(), &config).unwrap();
        // Write to segment 1
        for _ in 0..3 {
            writer.append(&mut make_entry()).unwrap();
        }
        writer.rotate().unwrap(); // segment 2
        writer.rotate().unwrap(); // segment 3
        writer.sync().unwrap();

        // Remove segment 2 to create a gap
        writer.remove_segment(2).unwrap();
    }

    let writer = WalWriter::open(dir.path(), &config).unwrap();
    // Segments on disk: 1 (3 entries, seqs 1-3), 3 (empty). Segment 2 was removed.
    // Recovery scans from last segment backward: seg 3 is empty, seg 1 has seq 3 as highest.
    assert_eq!(writer.next_sequence_number(), 4);
}

// === Append Tests ===

#[test]
fn test_append_assigns_monotonic_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    let mut entries: Vec<WalEntry> = (0..100).map(|_| make_entry()).collect();
    for (i, entry) in entries.iter_mut().enumerate() {
        writer.append(entry).unwrap();
        assert_eq!(entry.sequence_number, (i + 1) as u64);
    }
}

#[test]
fn test_append_batch_assigns_consecutive_sequences() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    let mut entries: Vec<WalEntry> = (0..10).map(|_| make_entry()).collect();
    writer.append_batch(&mut entries).unwrap();

    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(entry.sequence_number, (i + 1) as u64);
    }
}

#[test]
fn test_appended_entries_readable() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    let mut entries: Vec<WalEntry> = (0..10).map(|_| make_entry()).collect();
    for entry in entries.iter_mut() {
        writer.append(entry).unwrap();
    }
    writer.sync().unwrap();

    let seg_path = dir.path().join(segment_filename(1));
    let reader = SegmentReader::open(&seg_path).unwrap();
    let read: Vec<WalEntry> = reader.entries().map(|r| r.unwrap()).collect();

    // The reader reads the current active segment created by open (segment 1)
    // But open creates segment 1, then if we reopen it creates segment 2 reading from segment 1
    // Actually open creates segment 1 initially, entries are written to it
    assert_eq!(read.len(), 10);
    for (i, entry) in read.iter().enumerate() {
        assert_eq!(entry.sequence_number, (i + 1) as u64);
    }
}

// === Rotation Tests ===

#[test]
fn test_rotation_at_size_threshold() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    let initial_seg = writer.current_segment_number();
    // Write until rotation
    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }

    assert!(
        writer.current_segment_number() > initial_seg,
        "should have rotated"
    );
}

#[test]
fn test_rotation_preserves_sequence_continuity() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    let mut all_seqs = Vec::new();
    for _ in 0..100 {
        let mut entry = make_entry();
        writer.append(&mut entry).unwrap();
        all_seqs.push(entry.sequence_number);
    }
    writer.sync().unwrap();

    // Verify monotonic and continuous
    for window in all_seqs.windows(2) {
        assert_eq!(window[1], window[0] + 1, "sequence gap detected");
    }
}

#[test]
fn test_rotation_syncs_old_segment() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    // Write enough to rotate
    for _ in 0..50 {
        writer.append(&mut make_entry()).unwrap();
    }

    // Old segment(s) should be readable
    let seg_path = dir.path().join(segment_filename(1));
    let reader = SegmentReader::open(&seg_path).unwrap();
    let entries: Vec<WalEntry> = reader.entries().map(|r| r.unwrap()).collect();
    assert!(!entries.is_empty());
}

#[test]
fn test_multiple_rotations() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    for _ in 0..500 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();

    assert!(
        writer.active_segment_numbers().len() >= 3,
        "expected at least 3 segments from multiple rotations"
    );

    // All segments should be valid and readable
    for &seg_num in writer.active_segment_numbers() {
        let path = dir.path().join(segment_filename(seg_num));
        let reader = SegmentReader::open(&path).unwrap();
        // Should not panic
        for result in reader.entries() {
            result.unwrap();
        }
    }
}

#[test]
fn test_rotation_creates_consecutive_segment_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    for _ in 0..200 {
        writer.append(&mut make_entry()).unwrap();
    }

    let nums = writer.active_segment_numbers();
    for window in nums.windows(2) {
        assert_eq!(window[1], window[0] + 1, "segment number gap");
    }
}

// === Size Tracking Tests ===

#[test]
fn test_fresh_writer_total_size_equals_header() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let writer = WalWriter::open(dir.path(), &config).unwrap();

    let size = writer.total_size().unwrap();
    assert_eq!(
        size, SEGMENT_HEADER_SIZE as u64,
        "fresh writer should have exactly one segment containing only the header"
    );
}

#[test]
fn test_total_size_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();

    let total = writer.total_size().unwrap();
    assert!(total > 0);

    // Verify by summing file sizes
    let mut expected = 0u64;
    for &seg_num in writer.active_segment_numbers() {
        let path = dir.path().join(segment_filename(seg_num));
        expected += std::fs::metadata(&path).unwrap().len();
    }
    assert_eq!(total, expected);
}

#[test]
fn test_total_size_after_segment_removal() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();

    let size_before = writer.total_size().unwrap();
    let first_seg = writer.active_segment_numbers()[0];
    writer.remove_segment(first_seg).unwrap();
    let size_after = writer.total_size().unwrap();

    assert!(size_after < size_before);
}

// === Segment Removal Tests ===

#[test]
fn test_remove_segment_deletes_file() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    for _ in 0..50 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();

    let first_seg = writer.active_segment_numbers()[0];
    let path = dir.path().join(segment_filename(first_seg));
    assert!(path.exists());

    writer.remove_segment(first_seg).unwrap();
    assert!(!path.exists());
}

#[test]
fn test_remove_segment_updates_list() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    for _ in 0..50 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();

    let first_seg = writer.active_segment_numbers()[0];
    let count_before = writer.active_segment_numbers().len();
    writer.remove_segment(first_seg).unwrap();
    assert_eq!(writer.active_segment_numbers().len(), count_before - 1);
    assert!(!writer.active_segment_numbers().contains(&first_seg));
}

#[test]
fn test_remove_current_segment_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    let current = writer.current_segment_number();
    let result = writer.remove_segment(current);
    assert!(result.is_err());
}
