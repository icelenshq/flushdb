use bytes::Bytes;
use flushdb_types::{EntryType, IdempotencyToken};
use flushdb_wal::{
    segment_filename, WalConfig, WalEntry, WalReader, WalWriter, SEGMENT_HEADER_SIZE,
};

fn make_entry() -> WalEntry {
    WalEntry {
        sequence_number: 0,
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
        segment_size_target: 512,
        ..WalConfig::default()
    }
}

fn write_entries_with_rotation(dir: &std::path::Path, count: usize) -> Vec<u64> {
    let config = small_config();
    let mut writer = WalWriter::open(dir, &config).unwrap();
    let mut seqs = Vec::new();
    for _ in 0..count {
        let mut entry = make_entry();
        writer.append(&mut entry).unwrap();
        seqs.push(entry.sequence_number);
    }
    writer.sync().unwrap();
    seqs
}

// === Multi-Segment Replay Tests ===

#[test]
fn test_replay_single_segment() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 100);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence_number, (i + 1) as u64);
    }
}

#[test]
fn test_replay_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let seqs = write_entries_with_rotation(dir.path(), 200);

    let reader = WalReader::open(dir.path()).unwrap();
    assert!(reader.segment_count() >= 3, "expected 3+ segments");

    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 200);
    for (i, e) in entries.iter().enumerate() {
        assert_eq!(e.sequence_number, seqs[i]);
    }
}

#[test]
fn test_replay_empty_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let writer = WalWriter::open(dir.path(), &config).unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    // The open creates a segment, so we have 1 segment with 0 entries
    assert!(entries.is_empty());
}

#[test]
fn test_replay_preserves_entry_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    let mut entry = WalEntry {
        sequence_number: 0,
        entry_type: EntryType::Delete,
        namespace: Bytes::from_static(b"prod"),
        record_id: Bytes::from_static(b"user-123"),
        item_key: Bytes::from_static(b"profile"),
        item_value: Bytes::new(),
        item_metadata: Bytes::from_static(b"deleted-reason"),
        idempotency_token: IdempotencyToken::none(),
    };
    writer.append(&mut entry).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.entry_type, EntryType::Delete);
    assert_eq!(e.namespace.as_ref(), b"prod");
    assert_eq!(e.record_id.as_ref(), b"user-123");
    assert_eq!(e.item_key.as_ref(), b"profile");
    assert!(e.item_value.is_empty());
    assert_eq!(e.item_metadata.as_ref(), b"deleted-reason");
}

// === Sequence Filtering Tests ===

#[test]
fn test_replay_from_filters_by_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_from(50).unwrap();
    assert_eq!(entries.len(), 51); // 50 through 100
    assert_eq!(entries[0].sequence_number, 50);
    assert_eq!(entries.last().unwrap().sequence_number, 100);
}

#[test]
fn test_replay_from_skips_entire_segments() {
    let dir = tempfile::tempdir().unwrap();
    let seqs = write_entries_with_rotation(dir.path(), 200);

    let reader = WalReader::open(dir.path()).unwrap();
    let last_seq = *seqs.last().unwrap();
    // Replay from near the end
    let entries = reader.replay_from(last_seq - 5).unwrap();
    assert!(entries.len() <= 10);
    assert!(entries.iter().all(|e| e.sequence_number >= last_seq - 5));
}

#[test]
fn test_replay_from_with_min_zero() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..10 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let all = reader.replay_all().unwrap();
    let from_zero = reader.replay_from(0).unwrap();
    assert_eq!(all.len(), from_zero.len());
}

#[test]
fn test_replay_from_with_min_beyond_max() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..10 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_from(999).unwrap();
    assert!(entries.is_empty());
}

#[test]
fn test_replay_from_at_segment_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let seqs = write_entries_with_rotation(dir.path(), 100);

    let reader = WalReader::open(dir.path()).unwrap();
    // Find a sequence at a segment boundary
    let mid_seq = seqs[seqs.len() / 2];
    let entries = reader.replay_from(mid_seq).unwrap();
    assert!(entries.iter().all(|e| e.sequence_number >= mid_seq));
}

// === Iterator Tests ===

#[test]
fn test_iter_yields_all_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..50 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let replay = reader.replay_all().unwrap();
    let iter_entries: Vec<WalEntry> = reader.iter().map(|r| r.unwrap()).collect();
    assert_eq!(replay.len(), iter_entries.len());
}

#[test]
fn test_iter_from_skips_early_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries: Vec<WalEntry> = reader.iter_from(50).map(|r| r.unwrap()).collect();
    assert!(entries.iter().all(|e| e.sequence_number >= 50));
    assert_eq!(entries[0].sequence_number, 50);
}

// === Cross-Segment Iterator Tests ===

#[test]
fn test_iter_yields_entries_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let seqs = write_entries_with_rotation(dir.path(), 200);

    let reader = WalReader::open(dir.path()).unwrap();
    assert!(
        reader.segment_count() >= 3,
        "expected 3+ segments to validate cross-segment iteration"
    );

    let iter_entries: Vec<WalEntry> = reader.iter().map(|r| r.unwrap()).collect();
    assert_eq!(iter_entries.len(), 200);
    for (i, entry) in iter_entries.iter().enumerate() {
        assert_eq!(
            entry.sequence_number, seqs[i],
            "sequence mismatch at index {i}"
        );
    }
}

#[test]
fn test_iter_from_across_segments() {
    let dir = tempfile::tempdir().unwrap();
    let seqs = write_entries_with_rotation(dir.path(), 200);

    let reader = WalReader::open(dir.path()).unwrap();
    assert!(
        reader.segment_count() >= 3,
        "expected 3+ segments to validate cross-segment iter_from"
    );

    let mid_seq = seqs[seqs.len() / 2];
    let iter_entries: Vec<WalEntry> = reader.iter_from(mid_seq).map(|r| r.unwrap()).collect();

    assert!(
        !iter_entries.is_empty(),
        "iter_from should yield entries from midpoint onward"
    );
    assert!(
        iter_entries
            .iter()
            .all(|e| e.sequence_number >= mid_seq),
        "all entries should have sequence >= mid_seq"
    );
    assert_eq!(iter_entries[0].sequence_number, mid_seq);

    // Verify the count matches what replay_from returns
    let replay_entries = reader.replay_from(mid_seq).unwrap();
    assert_eq!(iter_entries.len(), replay_entries.len());
}

// === Corruption Handling Tests ===

#[test]
fn test_replay_with_tail_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..10 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    // Find the active segment and append garbage
    // The writer creates segment 1 (the initial one)
    let seg_path = dir.path().join(segment_filename(1));
    if seg_path.exists() {
        let mut file = std::fs::OpenOptions::new()
            .append(true)
            .open(&seg_path)
            .unwrap();
        use std::io::Write;
        file.write_all(&[0xDE; 20]).unwrap();
    }

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 10);
}

#[test]
fn test_replay_stops_on_mid_segment_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..10 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    // Corrupt entry 5 in segment 1
    let seg_path = dir.path().join(segment_filename(1));
    let mut data = std::fs::read(&seg_path).unwrap();
    let mut offset = SEGMENT_HEADER_SIZE;
    for _ in 0..4 {
        let len = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]) as usize;
        offset += 4 + len + 4;
    }
    // Corrupt entry 5's body
    data[offset + 10] ^= 0xFF;
    std::fs::write(&seg_path, &data).unwrap();

    let reader = WalReader::open(dir.path()).unwrap();
    let result = reader.replay_all();
    assert!(result.is_err());
}

// === Edge Cases ===

#[test]
fn test_replay_with_gaps_in_segment_numbers() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();

    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();

    // Remove a middle segment to create a gap
    let segments = writer.active_segment_numbers().to_vec();
    if segments.len() > 2 {
        let middle = segments[1];
        writer.remove_segment(middle).unwrap();
    }
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    // Should still work, just missing entries from the removed segment
    assert!(!entries.is_empty());
}

#[test]
fn test_replay_single_entry() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    writer.append(&mut make_entry()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].sequence_number, 1);
}

#[test]
fn test_is_empty_with_no_segments() {
    let dir = tempfile::tempdir().unwrap();
    // Create an empty directory (no segment files)
    let reader = WalReader::open(dir.path()).unwrap();
    assert!(reader.is_empty());
    assert_eq!(reader.segment_count(), 0);
}

#[test]
fn test_is_empty_with_segments() {
    let dir = tempfile::tempdir().unwrap();
    write_entries_with_rotation(dir.path(), 10);

    let reader = WalReader::open(dir.path()).unwrap();
    assert!(!reader.is_empty());
}

#[test]
fn test_segment_count_matches_written_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = small_config();
    let mut writer = WalWriter::open(dir.path(), &config).unwrap();
    for _ in 0..100 {
        writer.append(&mut make_entry()).unwrap();
    }
    writer.sync().unwrap();
    let expected_segments = writer.active_segment_numbers().len();
    drop(writer);

    let reader = WalReader::open(dir.path()).unwrap();
    assert_eq!(reader.segment_count(), expected_segments);
}

#[test]
fn test_open_nonexistent_directory() {
    let result = WalReader::open(std::path::Path::new("/nonexistent/wal/dir"));
    assert!(result.is_err());
}
