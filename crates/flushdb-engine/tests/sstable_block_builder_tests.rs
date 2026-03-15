use flushdb_engine::sstable::block_builder::BlockBuilder;
use flushdb_engine::sstable::block_reader::decode_block;
use flushdb_engine::sstable::types::CompressionType;
use flushdb_types::{CompositeKey, EntryType, EntryValue, FlushError, IdempotencyToken};

fn make_key(record_id: &[u8], item_key: &[u8]) -> CompositeKey {
    CompositeKey::new(record_id, item_key).unwrap()
}

// ---------------------------------------------------------------------------
// Basic Building Tests
// ---------------------------------------------------------------------------

#[test]
fn test_single_entry_block() {
    let mut builder = BlockBuilder::new(4096);
    let key = make_key(b"rec1", b"item1");

    builder.add_entry(
        &key,
        b"hello",
        b"meta",
        EntryType::Put,
        1,
        IdempotencyToken::none(),
    );

    let finished = builder.finish(CompressionType::None).unwrap();
    assert_eq!(finished.entry_count, 1);
    assert_eq!(finished.first_key, key);
    assert_eq!(finished.last_key, key);
}

#[test]
fn test_multiple_entries_block() {
    let mut builder = BlockBuilder::new(4096);
    let keys: Vec<CompositeKey> = (0..5)
        .map(|i| make_key(format!("rec{i}").as_bytes(), format!("item{i}").as_bytes()))
        .collect();

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
    assert_eq!(finished.entry_count, 5);
    assert_eq!(finished.first_key, keys[0]);
    assert_eq!(finished.last_key, keys[4]);
}

#[test]
fn test_empty_block_finish_errors() {
    let builder = BlockBuilder::new(4096);
    let result = builder.finish(CompressionType::None);
    assert!(matches!(result, Err(FlushError::InvalidArgument { .. })));
}

#[test]
fn test_entry_with_empty_value() {
    let mut builder = BlockBuilder::new(4096);
    let key = make_key(b"rec1", b"item1");

    builder.add_entry(&key, &[], b"meta", EntryType::Delete, 1, IdempotencyToken::none());

    let finished = builder.finish(CompressionType::None).unwrap();
    assert_eq!(finished.entry_count, 1);

    let entries = decode_block(&finished.data, CompressionType::None).unwrap();
    assert_eq!(entries[0].value, EntryValue::Inline(bytes::Bytes::new()));
    assert_eq!(entries[0].entry_type, EntryType::Delete);
}

#[test]
fn test_entry_with_empty_metadata() {
    let mut builder = BlockBuilder::new(4096);
    let key = make_key(b"rec1", b"item1");

    builder.add_entry(&key, b"value", &[], EntryType::Put, 1, IdempotencyToken::none());

    let finished = builder.finish(CompressionType::None).unwrap();
    assert_eq!(finished.entry_count, 1);

    let entries = decode_block(&finished.data, CompressionType::None).unwrap();
    assert!(entries[0].metadata.is_empty());
    assert_eq!(entries[0].value, EntryValue::Inline(bytes::Bytes::from_static(b"value")));
}

#[test]
fn test_all_entry_types() {
    let mut builder = BlockBuilder::new(4096);

    let key_put = make_key(b"rec1", b"item1");
    builder.add_entry(
        &key_put,
        b"value",
        b"meta",
        EntryType::Put,
        1,
        IdempotencyToken::none(),
    );

    let key_del = make_key(b"rec2", b"item2");
    builder.add_entry(
        &key_del,
        &[],
        &[],
        EntryType::Delete,
        2,
        IdempotencyToken::none(),
    );

    let key_range_del = make_key(b"rec3", b"item3");
    builder.add_entry(
        &key_range_del,
        b"end_key",
        &[],
        EntryType::RangeDelete,
        3,
        IdempotencyToken::none(),
    );

    let finished = builder.finish(CompressionType::None).unwrap();
    assert_eq!(finished.entry_count, 3);

    let entries = decode_block(&finished.data, CompressionType::None).unwrap();
    assert_eq!(entries[0].entry_type, EntryType::Put);
    assert_eq!(entries[1].entry_type, EntryType::Delete);
    assert_eq!(entries[2].entry_type, EntryType::RangeDelete);
}

// ---------------------------------------------------------------------------
// Record ID Dedup Tests
// ---------------------------------------------------------------------------

#[test]
fn test_first_entry_always_full_record_id() {
    let mut builder = BlockBuilder::new(4096);
    let key1 = make_key(b"shared_record", b"item1");
    let key2 = make_key(b"shared_record", b"item2");

    builder.add_entry(&key1, b"v", &[], EntryType::Put, 1, IdempotencyToken::none());
    builder.add_entry(&key2, b"v", &[], EntryType::Put, 2, IdempotencyToken::none());

    let finished = builder.finish(CompressionType::None).unwrap();
    let entries = decode_block(&finished.data, CompressionType::None).unwrap();

    // Both entries must have correct record_id — the first entry's record_id must
    // have been written in full (not dedup'd), which we verify by round-tripping.
    assert_eq!(entries[0].composite_key.record_id(), b"shared_record");
    assert_eq!(entries[1].composite_key.record_id(), b"shared_record");
}

#[test]
fn test_consecutive_same_record_dedup() {
    // Build a block with 2 entries sharing the same record_id
    let mut builder_dedup = BlockBuilder::new(4096);
    let key1 = make_key(b"shared_record", b"item1");
    let key2 = make_key(b"shared_record", b"item2");

    builder_dedup.add_entry(
        &key1,
        b"value",
        b"meta",
        EntryType::Put,
        1,
        IdempotencyToken::none(),
    );
    let size_after_first = builder_dedup.estimated_size();

    builder_dedup.add_entry(
        &key2,
        b"value",
        b"meta",
        EntryType::Put,
        2,
        IdempotencyToken::none(),
    );
    let size_after_second = builder_dedup.estimated_size();

    // The second entry uses dedup (record_id_len=0, just one varint byte for 0),
    // so its size contribution should be smaller than the first entry's.
    let first_entry_size = size_after_first;
    let second_entry_size = size_after_second - size_after_first;

    // Second entry saves the record_id bytes ("shared_record" = 13 bytes + varint for length)
    // so it should be noticeably smaller
    assert!(
        second_entry_size < first_entry_size,
        "dedup entry ({second_entry_size}) should be smaller than full entry ({first_entry_size})"
    );
}

#[test]
fn test_different_record_id_resets_dedup() {
    let mut builder = BlockBuilder::new(4096);
    let key1 = make_key(b"record_a", b"item1");
    let key2 = make_key(b"record_b", b"item1");

    builder.add_entry(
        &key1,
        b"value",
        b"meta",
        EntryType::Put,
        1,
        IdempotencyToken::none(),
    );
    let size_after_first = builder.estimated_size();

    builder.add_entry(
        &key2,
        b"value",
        b"meta",
        EntryType::Put,
        2,
        IdempotencyToken::none(),
    );
    let size_after_second = builder.estimated_size();

    let first_entry_size = size_after_first;
    let second_entry_size = size_after_second - size_after_first;

    // Both entries have similar-length record_ids written in full, so sizes should be close
    let diff = (first_entry_size as i64 - second_entry_size as i64).unsigned_abs();
    // The only difference is "record_a" vs "record_b" (same length), so diff should be 0
    assert!(
        diff <= 1,
        "both entries should be about the same size when record_ids differ: {first_entry_size} vs {second_entry_size}"
    );
}

#[test]
fn test_dedup_reduces_block_size() {
    // Block with 10 entries sharing the same record_id
    let mut builder_same = BlockBuilder::new(65536);
    for i in 0..10u64 {
        let key = make_key(b"same_record_id_here", format!("item{i}").as_bytes());
        builder_same.add_entry(
            &key,
            b"some_value_data",
            b"metadata",
            EntryType::Put,
            i,
            IdempotencyToken::none(),
        );
    }
    let size_same = builder_same.estimated_size();

    // Block with 10 entries each with a distinct record_id of similar length
    let mut builder_distinct = BlockBuilder::new(65536);
    for i in 0..10u64 {
        let rid = format!("distinct_record_{i:03}");
        let key = make_key(rid.as_bytes(), format!("item{i}").as_bytes());
        builder_distinct.add_entry(
            &key,
            b"some_value_data",
            b"metadata",
            EntryType::Put,
            i,
            IdempotencyToken::none(),
        );
    }
    let size_distinct = builder_distinct.estimated_size();

    assert!(
        size_same < size_distinct,
        "dedup block ({size_same} bytes) should be smaller than distinct block ({size_distinct} bytes)"
    );
}

// ---------------------------------------------------------------------------
// Block Size & Rotation Tests
// ---------------------------------------------------------------------------

#[test]
fn test_is_full_at_target() {
    let target = 128;
    let mut builder = BlockBuilder::new(target);

    // Add entries until we exceed the target
    let mut seq = 0u64;
    while !builder.is_full() {
        let key = make_key(
            format!("rec{seq}").as_bytes(),
            format!("item{seq}").as_bytes(),
        );
        builder.add_entry(
            &key,
            b"some_value_that_takes_space",
            b"metadata",
            EntryType::Put,
            seq,
            IdempotencyToken::none(),
        );
        seq += 1;
        // Safety limit
        if seq > 1000 {
            panic!("should have become full by now");
        }
    }

    assert!(builder.is_full());
    assert!(builder.estimated_size() >= target);
}

#[test]
fn test_is_full_below_target() {
    let mut builder = BlockBuilder::new(4096);
    let key = make_key(b"r", b"k");
    builder.add_entry(
        &key,
        b"v",
        &[],
        EntryType::Put,
        1,
        IdempotencyToken::none(),
    );
    assert!(!builder.is_full());
}

#[test]
fn test_estimated_size_grows() {
    let mut builder = BlockBuilder::new(65536);
    let mut prev_size = 0;

    for i in 0..10u64 {
        let key = make_key(format!("rec{i}").as_bytes(), format!("item{i}").as_bytes());
        builder.add_entry(
            &key,
            b"value",
            b"meta",
            EntryType::Put,
            i,
            IdempotencyToken::none(),
        );
        let current_size = builder.estimated_size();
        assert!(
            current_size > prev_size,
            "size should grow: was {prev_size}, now {current_size} after entry {i}"
        );
        prev_size = current_size;
    }
}

// ---------------------------------------------------------------------------
// Compression Tests
// ---------------------------------------------------------------------------

#[test]
fn test_finish_compression_none() {
    let mut builder = BlockBuilder::new(4096);
    let key = make_key(b"rec1", b"item1");
    builder.add_entry(
        &key,
        b"hello world",
        b"meta",
        EntryType::Put,
        1,
        IdempotencyToken::none(),
    );

    let finished = builder.finish(CompressionType::None).unwrap();
    // Uncompressed: data should be decodable by the reader (verified in reader tests)
    assert!(!finished.data.is_empty());
    assert_eq!(finished.uncompressed_size, finished.data.len() as u32);
}

#[test]
fn test_finish_compression_snappy() {
    let mut builder = BlockBuilder::new(4096);
    for i in 0..20u64 {
        let key = make_key(b"record", format!("item{i}").as_bytes());
        builder.add_entry(
            &key,
            b"repeated_value_data_for_compression",
            b"metadata",
            EntryType::Put,
            i,
            IdempotencyToken::none(),
        );
    }

    let finished = builder.finish(CompressionType::Snappy).unwrap();
    assert!(!finished.data.is_empty());

    // Verify we can decompress and CRC is valid
    let mut decoder = snap::raw::Decoder::new();
    let decompressed = decoder.decompress_vec(&finished.data).unwrap();
    assert_eq!(decompressed.len(), finished.uncompressed_size as usize);

    let payload = &decompressed[..decompressed.len() - 4];
    let stored_crc = u32::from_le_bytes(decompressed[decompressed.len() - 4..].try_into().unwrap());
    assert_eq!(crc32fast::hash(payload), stored_crc, "CRC must match payload");
}

#[test]
fn test_finish_compression_zstd() {
    let mut builder = BlockBuilder::new(4096);
    for i in 0..20u64 {
        let key = make_key(b"record", format!("item{i}").as_bytes());
        builder.add_entry(
            &key,
            b"repeated_value_data_for_compression",
            b"metadata",
            EntryType::Put,
            i,
            IdempotencyToken::none(),
        );
    }

    let finished = builder.finish(CompressionType::Zstd).unwrap();
    assert!(!finished.data.is_empty());

    // Verify we can decompress and CRC is valid
    let decompressed = zstd::stream::decode_all(finished.data.as_ref()).unwrap();
    assert_eq!(decompressed.len(), finished.uncompressed_size as usize);

    let payload = &decompressed[..decompressed.len() - 4];
    let stored_crc = u32::from_le_bytes(decompressed[decompressed.len() - 4..].try_into().unwrap());
    assert_eq!(crc32fast::hash(payload), stored_crc, "CRC must match payload");
}

// ---------------------------------------------------------------------------
// Metadata Tracking Tests
// ---------------------------------------------------------------------------

#[test]
fn test_record_ids_collected() {
    let mut builder = BlockBuilder::new(65536);

    // 5 entries with 3 distinct record_ids: rec_a (x2), rec_b (x2), rec_c (x1)
    let entries = [
        (b"rec_a" as &[u8], b"item1" as &[u8]),
        (b"rec_a", b"item2"),
        (b"rec_b", b"item1"),
        (b"rec_b", b"item2"),
        (b"rec_c", b"item1"),
    ];

    for (i, (rid, ik)) in entries.iter().enumerate() {
        let key = make_key(rid, ik);
        builder.add_entry(
            &key,
            b"val",
            &[],
            EntryType::Put,
            i as u64,
            IdempotencyToken::none(),
        );
    }

    let finished = builder.finish(CompressionType::None).unwrap();
    assert_eq!(finished.record_ids.len(), 3);
    assert!(finished.record_ids.contains(&bytes::Bytes::from_static(b"rec_a")));
    assert!(finished.record_ids.contains(&bytes::Bytes::from_static(b"rec_b")));
    assert!(finished.record_ids.contains(&bytes::Bytes::from_static(b"rec_c")));
}

#[test]
fn test_idempotency_tokens_collected() {
    let mut builder = BlockBuilder::new(65536);

    let token1 = IdempotencyToken::new(1000);
    let token2 = IdempotencyToken::new(2000);
    let token3 = IdempotencyToken::new(3000);

    let entries: Vec<(CompositeKey, IdempotencyToken)> = vec![
        (make_key(b"r1", b"k1"), token1),
        (make_key(b"r2", b"k2"), IdempotencyToken::none()),
        (make_key(b"r3", b"k3"), token2),
        (make_key(b"r4", b"k4"), IdempotencyToken::none()),
        (make_key(b"r5", b"k5"), token3),
    ];

    for (i, (key, token)) in entries.into_iter().enumerate() {
        builder.add_entry(&key, b"val", &[], EntryType::Put, i as u64, token);
    }

    let finished = builder.finish(CompressionType::None).unwrap();
    assert_eq!(
        finished.idempotency_tokens.len(),
        3,
        "only non-none tokens should be collected"
    );
}

#[test]
fn test_reset_clears_state() {
    let mut builder = BlockBuilder::new(4096);
    let key = make_key(b"rec", b"item");

    builder.add_entry(
        &key,
        b"value",
        b"meta",
        EntryType::Put,
        1,
        IdempotencyToken::new(999),
    );

    assert!(!builder.is_empty());
    assert!(builder.estimated_size() > 0);
    assert_eq!(builder.entry_count(), 1);

    builder.reset();

    assert!(builder.is_empty());
    assert_eq!(builder.estimated_size(), 0);
    assert_eq!(builder.entry_count(), 0);

    // Verify record_ids and tokens are cleared by adding a new entry and finishing
    let key2 = make_key(b"new_rec", b"new_item");
    builder.add_entry(&key2, b"v2", &[], EntryType::Put, 2, IdempotencyToken::new(888));
    let finished = builder.finish(CompressionType::None).unwrap();

    // Only the post-reset entry's record_id and token should be present
    assert_eq!(finished.record_ids.len(), 1);
    assert!(finished.record_ids.contains(&bytes::Bytes::from_static(b"new_rec")));
    assert_eq!(finished.idempotency_tokens.len(), 1);
}
