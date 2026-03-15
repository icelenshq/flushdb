use bytes::Bytes;
use flushdb_engine::sstable::block_builder::BlockBuilder;
use flushdb_engine::sstable::block_reader::{BlockEntryIterator, decode_block};
use flushdb_engine::sstable::types::CompressionType;
use flushdb_engine::sstable::varint::encode_varint;
use flushdb_types::{CompositeKey, EntryType, EntryValue, FlushError, IdempotencyToken};

type TestEntry<'a> = (&'a CompositeKey, &'a [u8], &'a [u8], EntryType, u64);

fn make_key(record_id: &[u8], item_key: &[u8]) -> CompositeKey {
    CompositeKey::new(record_id, item_key).unwrap()
}

fn build_block(entries: &[TestEntry], compression: CompressionType) -> Bytes {
    let mut builder = BlockBuilder::new(65536);
    for &(key, value, metadata, entry_type, seq) in entries {
        builder.add_entry(key, value, metadata, entry_type, seq, IdempotencyToken::none());
    }
    builder.finish(compression).unwrap().data
}

// ---------------------------------------------------------------------------
// Round-trip Tests (Builder -> Reader)
// ---------------------------------------------------------------------------

#[test]
fn test_round_trip_single_entry() {
    let key = make_key(b"rec1", b"item1");
    let value = b"hello world";
    let metadata = b"some_meta";
    let entry_type = EntryType::Put;
    let seq = 42u64;

    let data = build_block(&[(&key, value, metadata, entry_type, seq)], CompressionType::None);
    let entries = decode_block(&data, CompressionType::None).unwrap();

    assert_eq!(entries.len(), 1);
    let e = &entries[0];
    assert_eq!(e.composite_key, key);
    assert_eq!(e.value, EntryValue::Inline(Bytes::from_static(value)));
    assert_eq!(e.metadata.as_ref(), metadata);
    assert_eq!(e.entry_type, entry_type);
    assert_eq!(e.sequence_number, seq);
}

#[test]
fn test_round_trip_multiple_entries() {
    let keys: Vec<CompositeKey> = (0..10)
        .map(|i| make_key(format!("rec{i}").as_bytes(), format!("item{i}").as_bytes()))
        .collect();

    let entries_input: Vec<TestEntry> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k, b"value_data" as &[u8], b"meta" as &[u8], EntryType::Put, i as u64))
        .collect();

    let data = build_block(&entries_input, CompressionType::None);
    let decoded = decode_block(&data, CompressionType::None).unwrap();

    assert_eq!(decoded.len(), 10);
    for (i, entry) in decoded.iter().enumerate() {
        assert_eq!(entry.composite_key, keys[i]);
        assert_eq!(
            entry.value,
            EntryValue::Inline(Bytes::from_static(b"value_data"))
        );
        assert_eq!(entry.metadata.as_ref(), b"meta");
        assert_eq!(entry.entry_type, EntryType::Put);
        assert_eq!(entry.sequence_number, i as u64);
    }
}

#[test]
fn test_round_trip_all_entry_types() {
    let key_put = make_key(b"rec1", b"item1");
    let key_del = make_key(b"rec2", b"item2");
    let key_range = make_key(b"rec3", b"item3");

    let data = build_block(
        &[
            (&key_put, b"value", b"", EntryType::Put, 1),
            (&key_del, b"", b"", EntryType::Delete, 2),
            (&key_range, b"end_key", b"", EntryType::RangeDelete, 3),
        ],
        CompressionType::None,
    );

    let decoded = decode_block(&data, CompressionType::None).unwrap();
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0].entry_type, EntryType::Put);
    assert_eq!(decoded[1].entry_type, EntryType::Delete);
    assert_eq!(decoded[2].entry_type, EntryType::RangeDelete);
}

#[test]
fn test_round_trip_empty_value() {
    let key = make_key(b"rec1", b"item1");
    let data = build_block(
        &[(&key, b"", b"meta", EntryType::Delete, 1)],
        CompressionType::None,
    );

    let decoded = decode_block(&data, CompressionType::None).unwrap();
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].value, EntryValue::Inline(Bytes::new()));
}

#[test]
fn test_round_trip_empty_metadata() {
    let key = make_key(b"rec1", b"item1");
    let data = build_block(
        &[(&key, b"value", b"", EntryType::Put, 1)],
        CompressionType::None,
    );

    let decoded = decode_block(&data, CompressionType::None).unwrap();
    assert_eq!(decoded.len(), 1);
    assert!(decoded[0].metadata.is_empty());
}

#[test]
fn test_round_trip_large_value() {
    let key = make_key(b"rec1", b"item1");
    let large_value = vec![0xABu8; 32 * 1024]; // 32KB

    let mut builder = BlockBuilder::new(64 * 1024);
    builder.add_entry(
        &key,
        &large_value,
        b"meta",
        EntryType::Put,
        99,
        IdempotencyToken::none(),
    );
    let finished = builder.finish(CompressionType::None).unwrap();

    let decoded = decode_block(&finished.data, CompressionType::None).unwrap();
    assert_eq!(decoded.len(), 1);
    let val = decoded[0].value.inline_value().unwrap();
    assert_eq!(val.len(), 32 * 1024);
    assert!(val.iter().all(|&b| b == 0xAB));
    assert_eq!(decoded[0].sequence_number, 99);
}

// ---------------------------------------------------------------------------
// Record ID Carry-forward Tests
// ---------------------------------------------------------------------------

#[test]
fn test_record_id_dedup_carry_forward() {
    let keys: Vec<CompositeKey> = (0..5)
        .map(|i| make_key(b"shared_record", format!("item{i}").as_bytes()))
        .collect();

    let entries_input: Vec<TestEntry> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k, b"val" as &[u8], b"" as &[u8], EntryType::Put, i as u64))
        .collect();

    let data = build_block(&entries_input, CompressionType::None);
    let decoded = decode_block(&data, CompressionType::None).unwrap();

    assert_eq!(decoded.len(), 5);
    for (i, entry) in decoded.iter().enumerate() {
        assert_eq!(
            entry.composite_key.record_id(),
            b"shared_record",
            "entry {i} should have record_id 'shared_record'"
        );
        assert_eq!(
            entry.composite_key.item_key(),
            format!("item{i}").as_bytes(),
            "entry {i} should have correct item_key"
        );
    }
}

#[test]
fn test_record_id_changes_mid_block() {
    // Pattern: [A, A, B, B, A]
    let keys = [
        make_key(b"record_A", b"item0"),
        make_key(b"record_A", b"item1"),
        make_key(b"record_B", b"item2"),
        make_key(b"record_B", b"item3"),
        make_key(b"record_A", b"item4"),
    ];

    let entries_input: Vec<TestEntry> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k, b"val" as &[u8], b"" as &[u8], EntryType::Put, i as u64))
        .collect();

    let data = build_block(&entries_input, CompressionType::None);
    let decoded = decode_block(&data, CompressionType::None).unwrap();

    assert_eq!(decoded.len(), 5);
    assert_eq!(decoded[0].composite_key.record_id(), b"record_A");
    assert_eq!(decoded[1].composite_key.record_id(), b"record_A");
    assert_eq!(decoded[2].composite_key.record_id(), b"record_B");
    assert_eq!(decoded[3].composite_key.record_id(), b"record_B");
    assert_eq!(decoded[4].composite_key.record_id(), b"record_A");
}

#[test]
fn test_first_entry_always_has_record_id() {
    // Use a multi-entry block with shared record_id to prove the first entry
    // doesn't rely on carry-forward (it must have the full record_id written)
    let key1 = make_key(b"shared_record", b"item0");
    let key2 = make_key(b"shared_record", b"item1");
    let key3 = make_key(b"shared_record", b"item2");

    let data = build_block(
        &[
            (&key1, b"v", b"", EntryType::Put, 1),
            (&key2, b"v", b"", EntryType::Put, 2),
            (&key3, b"v", b"", EntryType::Put, 3),
        ],
        CompressionType::None,
    );

    let decoded = decode_block(&data, CompressionType::None).unwrap();
    assert_eq!(decoded.len(), 3);
    // First entry must have full record_id (not dedup'd), and carry-forward
    // must work for subsequent entries
    assert_eq!(decoded[0].composite_key.record_id(), b"shared_record");
    assert_eq!(decoded[0].composite_key.item_key(), b"item0");
    assert_eq!(decoded[1].composite_key.record_id(), b"shared_record");
    assert_eq!(decoded[2].composite_key.record_id(), b"shared_record");
}

// ---------------------------------------------------------------------------
// Compression Round-trip Tests
// ---------------------------------------------------------------------------

#[test]
fn test_round_trip_compression_none() {
    let keys: Vec<CompositeKey> = (0..5)
        .map(|i| make_key(format!("rec{i}").as_bytes(), format!("item{i}").as_bytes()))
        .collect();

    let entries_input: Vec<TestEntry> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k, b"value" as &[u8], b"meta" as &[u8], EntryType::Put, i as u64))
        .collect();

    let data = build_block(&entries_input, CompressionType::None);
    let decoded = decode_block(&data, CompressionType::None).unwrap();

    assert_eq!(decoded.len(), 5);
    for (i, entry) in decoded.iter().enumerate() {
        assert_eq!(entry.composite_key, keys[i]);
        assert_eq!(entry.value, EntryValue::Inline(Bytes::from_static(b"value")));
        assert_eq!(entry.metadata.as_ref(), b"meta");
        assert_eq!(entry.sequence_number, i as u64);
    }
}

#[test]
fn test_round_trip_compression_snappy() {
    let keys: Vec<CompositeKey> = (0..5)
        .map(|i| make_key(format!("rec{i}").as_bytes(), format!("item{i}").as_bytes()))
        .collect();

    let entries_input: Vec<TestEntry> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k, b"value" as &[u8], b"meta" as &[u8], EntryType::Put, i as u64))
        .collect();

    let data = build_block(&entries_input, CompressionType::Snappy);
    let decoded = decode_block(&data, CompressionType::Snappy).unwrap();

    assert_eq!(decoded.len(), 5);
    for (i, entry) in decoded.iter().enumerate() {
        assert_eq!(entry.composite_key, keys[i]);
        assert_eq!(
            entry.value,
            EntryValue::Inline(Bytes::from_static(b"value"))
        );
        assert_eq!(entry.metadata.as_ref(), b"meta");
        assert_eq!(entry.sequence_number, i as u64);
    }
}

#[test]
fn test_round_trip_compression_zstd() {
    let keys: Vec<CompositeKey> = (0..5)
        .map(|i| make_key(format!("rec{i}").as_bytes(), format!("item{i}").as_bytes()))
        .collect();

    let entries_input: Vec<TestEntry> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k, b"value" as &[u8], b"meta" as &[u8], EntryType::Put, i as u64))
        .collect();

    let data = build_block(&entries_input, CompressionType::Zstd);
    let decoded = decode_block(&data, CompressionType::Zstd).unwrap();

    assert_eq!(decoded.len(), 5);
    for (i, entry) in decoded.iter().enumerate() {
        assert_eq!(entry.composite_key, keys[i]);
        assert_eq!(
            entry.value,
            EntryValue::Inline(Bytes::from_static(b"value"))
        );
        assert_eq!(entry.metadata.as_ref(), b"meta");
        assert_eq!(entry.sequence_number, i as u64);
    }
}

// ---------------------------------------------------------------------------
// CRC Validation Tests
// ---------------------------------------------------------------------------

#[test]
fn test_crc_corruption_detected() {
    let key = make_key(b"rec1", b"item1");
    let data = build_block(
        &[(&key, b"value", b"meta", EntryType::Put, 1)],
        CompressionType::None,
    );

    // Flip one byte in the data portion (not the CRC trailer)
    let mut corrupted = data.to_vec();
    // Flip a byte early in the payload (the CRC is the last 4 bytes)
    if corrupted.len() > 5 {
        corrupted[2] ^= 0xFF;
    }

    let result = decode_block(&corrupted, CompressionType::None);
    assert!(
        matches!(result, Err(FlushError::CrcMismatch { .. })),
        "expected CrcMismatch error, got: {result:?}"
    );
}

#[test]
fn test_crc_valid_for_good_block() {
    let key = make_key(b"rec1", b"item1");
    let data = build_block(
        &[(&key, b"value", b"meta", EntryType::Put, 1)],
        CompressionType::None,
    );

    let entries = decode_block(&data, CompressionType::None)
        .expect("unmodified block should pass CRC check");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].composite_key, key);
}

// ---------------------------------------------------------------------------
// Block Validation Tests
// ---------------------------------------------------------------------------

#[test]
fn test_decode_block_too_short_for_crc() {
    // A block with less than 4 bytes cannot hold a CRC trailer
    let result = decode_block(&[0x01, 0x02], CompressionType::None);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "block shorter than 4 bytes should yield CorruptedData"
    );
}

// ---------------------------------------------------------------------------
// EntryValue Parsing Tests
// ---------------------------------------------------------------------------

#[test]
fn test_inline_value_parsed() {
    let key = make_key(b"rec1", b"item1");
    let value = b"inline_data_here";

    let data = build_block(
        &[(&key, value, b"", EntryType::Put, 1)],
        CompressionType::None,
    );

    let decoded = decode_block(&data, CompressionType::None).unwrap();
    assert_eq!(decoded.len(), 1);
    assert!(decoded[0].value.is_inline());
    assert_eq!(
        decoded[0].value.inline_value().unwrap().as_ref(),
        value
    );
}

#[test]
fn test_blob_ref_value_parsed() {
    // Manually construct raw block bytes with a BlobRef entry
    let record_id = b"rec1";
    let item_key = b"item1";
    let blob_id = b"blob-uuid-1234";
    let blob_offset: u64 = 1024;
    let blob_size: u32 = 8192;
    let metadata = b"meta";
    let entry_type = EntryType::Put;
    let sequence_number: u64 = 77;

    let mut buf: Vec<u8> = Vec::new();

    // record_id_len + record_id
    encode_varint(record_id.len() as u64, &mut buf);
    buf.extend_from_slice(record_id);

    // item_key_len + item_key
    encode_varint(item_key.len() as u64, &mut buf);
    buf.extend_from_slice(item_key);

    // value: tag=0x01 (BlobRef) + value_data_len + blob_id_len(u16 LE) + blob_id + offset(u64 LE) + size(u32 LE)
    buf.push(EntryValue::BLOB_REF_TAG);

    // value_data = blob_id_len(2) + blob_id(14) + offset(8) + size(4) = 28 bytes
    let value_data_len = 2 + blob_id.len() + 8 + 4;
    encode_varint(value_data_len as u64, &mut buf);
    buf.extend_from_slice(&(blob_id.len() as u16).to_le_bytes());
    buf.extend_from_slice(blob_id);
    buf.extend_from_slice(&blob_offset.to_le_bytes());
    buf.extend_from_slice(&blob_size.to_le_bytes());

    // metadata_len + metadata
    encode_varint(metadata.len() as u64, &mut buf);
    buf.extend_from_slice(metadata);

    // entry_type
    buf.push(entry_type.as_u8());

    // sequence_number
    encode_varint(sequence_number, &mut buf);

    // CRC32 of all the above
    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    let decoded = decode_block(&buf, CompressionType::None).unwrap();
    assert_eq!(decoded.len(), 1);

    let entry = &decoded[0];
    assert_eq!(entry.composite_key.record_id(), record_id);
    assert_eq!(entry.composite_key.item_key(), item_key);
    assert_eq!(entry.metadata.as_ref(), metadata);
    assert_eq!(entry.entry_type, EntryType::Put);
    assert_eq!(entry.sequence_number, 77);

    match &entry.value {
        EntryValue::BlobRef {
            blob_id: decoded_blob_id,
            offset,
            size,
        } => {
            assert_eq!(decoded_blob_id.as_ref(), blob_id);
            assert_eq!(*offset, blob_offset);
            assert_eq!(*size, blob_size);
        }
        other => panic!("expected BlobRef, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Iterator Tests
// ---------------------------------------------------------------------------

#[test]
fn test_iterator_yields_all_entries() {
    let keys: Vec<CompositeKey> = (0..7)
        .map(|i| make_key(format!("rec{i}").as_bytes(), format!("item{i}").as_bytes()))
        .collect();

    let mut builder = BlockBuilder::new(65536);
    for (i, key) in keys.iter().enumerate() {
        builder.add_entry(
            key,
            b"value",
            b"meta",
            EntryType::Put,
            i as u64,
            IdempotencyToken::none(),
        );
    }
    let finished = builder.finish(CompressionType::None).unwrap();

    let iter = BlockEntryIterator::new(&finished.data, CompressionType::None).unwrap();
    let collected: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(
        collected.len(),
        7,
        "iterator should yield exactly 7 entries"
    );

    for (i, entry) in collected.iter().enumerate() {
        assert_eq!(entry.composite_key, keys[i]);
        assert_eq!(entry.sequence_number, i as u64);
    }
}

#[test]
fn test_iterator_empty_after_exhaustion() {
    let key = make_key(b"rec1", b"item1");
    let data = build_block(
        &[(&key, b"v", b"", EntryType::Put, 1)],
        CompressionType::None,
    );

    let mut iter = BlockEntryIterator::new(&data, CompressionType::None).unwrap();

    // First call yields the entry
    let first = iter.next();
    assert!(first.is_some());
    assert!(first.unwrap().is_ok());

    // Subsequent calls should return None
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
    assert!(iter.next().is_none());
}
