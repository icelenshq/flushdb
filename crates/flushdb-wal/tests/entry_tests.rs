use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, FlushError, IdempotencyToken, MemtableEntry};
use flushdb_wal::WalEntry;

fn make_put_entry(seq: u64, ns: &[u8], rec: &[u8], key: &[u8], val: &[u8]) -> WalEntry {
    WalEntry {
        sequence_number: seq,
        entry_type: EntryType::Put,
        namespace: Bytes::copy_from_slice(ns),
        record_id: Bytes::copy_from_slice(rec),
        item_key: Bytes::copy_from_slice(key),
        item_value: Bytes::copy_from_slice(val),
        item_metadata: Bytes::copy_from_slice(b"meta"),
        idempotency_token: IdempotencyToken::none(),
    }
}

// === Round-trip Tests ===

#[test]
fn test_encode_decode_put_entry() {
    let entry = make_put_entry(1, b"ns1", b"rec1", b"key1", b"value1");
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let body = &encoded[4..4 + len];
    let crc_bytes = &encoded[4 + len..4 + len + 4];
    let expected_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    WalEntry::validate_crc(body, expected_crc).unwrap();
    let decoded = WalEntry::decode_body(body).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn test_encode_decode_delete_entry() {
    let entry = WalEntry {
        sequence_number: 42,
        entry_type: EntryType::Delete,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"key"),
        item_value: Bytes::new(),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let body = &encoded[4..4 + len];
    let decoded = WalEntry::decode_body(body).unwrap();
    assert_eq!(entry, decoded);
    assert_eq!(decoded.entry_type, EntryType::Delete);
    assert!(decoded.item_value.is_empty());
}

#[test]
fn test_encode_decode_range_delete_entry() {
    let entry = WalEntry {
        sequence_number: 99,
        entry_type: EntryType::RangeDelete,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"\xFFstart"),
        item_value: Bytes::new(),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let decoded = WalEntry::decode_body(&encoded[4..4 + len]).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn test_encode_decode_empty_fields() {
    let entry = WalEntry {
        sequence_number: 0,
        entry_type: EntryType::Put,
        namespace: Bytes::new(),
        record_id: Bytes::new(),
        item_key: Bytes::new(),
        item_value: Bytes::new(),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let decoded = WalEntry::decode_body(&encoded[4..4 + len]).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn test_encode_decode_large_value() {
    let big_val = vec![0xABu8; 1_048_576]; // 1MB
    let entry = WalEntry {
        sequence_number: 10,
        entry_type: EntryType::Put,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"key"),
        item_value: Bytes::from(big_val),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let decoded = WalEntry::decode_body(&encoded[4..4 + len]).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn test_encode_decode_binary_data() {
    let all_bytes: Vec<u8> = (0..=255).collect();
    let entry = WalEntry {
        sequence_number: 5,
        entry_type: EntryType::Put,
        namespace: Bytes::copy_from_slice(&all_bytes[..128]),
        record_id: Bytes::copy_from_slice(&all_bytes[1..100]), // skip 0x00 for record_id
        item_key: Bytes::copy_from_slice(&all_bytes),
        item_value: Bytes::copy_from_slice(&all_bytes),
        item_metadata: Bytes::copy_from_slice(&all_bytes[..64]),
        idempotency_token: IdempotencyToken::none(),
    };
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let decoded = WalEntry::decode_body(&encoded[4..4 + len]).unwrap();
    assert_eq!(entry, decoded);
}

#[test]
fn test_encode_decode_max_size_fields() {
    let record_id = vec![b'A'; 256];
    let item_key = vec![b'B'; 4096];
    let entry = WalEntry {
        sequence_number: 1,
        entry_type: EntryType::Put,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from(record_id),
        item_key: Bytes::from(item_key),
        item_value: Bytes::from_static(b"val"),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let decoded = WalEntry::decode_body(&encoded[4..4 + len]).unwrap();
    assert_eq!(entry, decoded);
}

// === CRC Tests ===

#[test]
fn test_crc_validates_for_valid_entry() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"val");
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let body = &encoded[4..4 + len];
    let crc_bytes = &encoded[4 + len..];
    let expected_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    assert!(WalEntry::validate_crc(body, expected_crc).is_ok());
}

#[test]
fn test_crc_detects_corrupted_sequence_number() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"val");
    let mut encoded = entry.encode().to_vec();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    // Flip a bit in sequence_number (offset 4 in encoded = first byte of body)
    encoded[4] ^= 0x01;
    let body = &encoded[4..4 + len];
    let crc_bytes = &encoded[4 + len..];
    let expected_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    let result = WalEntry::validate_crc(body, expected_crc);
    assert!(matches!(result, Err(FlushError::CrcMismatch { .. })));
}

#[test]
fn test_crc_detects_corrupted_value() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"hello world");
    let mut encoded = entry.encode().to_vec();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    // Corrupt a byte in the value region (somewhere in the body)
    let body_end = 4 + len;
    encoded[body_end - 30] ^= 0xFF;
    let body = &encoded[4..body_end];
    let crc_bytes = &encoded[body_end..];
    let expected_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    assert!(WalEntry::validate_crc(body, expected_crc).is_err());
}

#[test]
fn test_crc_detects_corrupted_namespace() {
    let entry = make_put_entry(1, b"namespace123", b"rec", b"key", b"val");
    let mut encoded = entry.encode().to_vec();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    // Corrupt namespace area (after seq_number + entry_type + ns_len = 4+8+1+2 = 15)
    encoded[15] ^= 0xFF;
    let body = &encoded[4..4 + len];
    let crc_bytes = &encoded[4 + len..];
    let expected_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
    assert!(WalEntry::validate_crc(body, expected_crc).is_err());
}

#[test]
fn test_crc_covers_all_body_bytes() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"val");
    let encoded = entry.encode();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    let crc_bytes = &encoded[4 + len..];
    let expected_crc = u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);

    // Corrupt each byte position in the body and verify CRC detects it
    for i in 0..len {
        let mut corrupted = encoded.to_vec();
        corrupted[4 + i] ^= 0x01;
        let body = &corrupted[4..4 + len];
        assert!(
            WalEntry::validate_crc(body, expected_crc).is_err(),
            "CRC did not detect corruption at body offset {i}"
        );
    }
}

// === Size Calculation Tests ===

#[test]
fn test_body_size_empty_variable_fields() {
    let entry = WalEntry {
        sequence_number: 0,
        entry_type: EntryType::Put,
        namespace: Bytes::new(),
        record_id: Bytes::new(),
        item_key: Bytes::new(),
        item_value: Bytes::new(),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    };
    // 8+1+2+2+2+4+2+24 = 45
    assert_eq!(entry.body_size(), 45);
}

#[test]
fn test_body_size_with_variable_fields() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"value");
    let expected = 45 + 2 + 3 + 3 + 5 + 4; // ns(2) + rec(3) + key(3) + value(5) + meta(4)
    assert_eq!(entry.body_size(), expected);
}

#[test]
fn test_total_size_includes_length_and_crc() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"val");
    assert_eq!(entry.total_size(), entry.body_size() + 8);
}

#[test]
fn test_encoded_bytes_length_matches_total_size() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"val");
    assert_eq!(entry.encode().len(), entry.total_size());
}

// === Validation Tests ===

#[test]
fn test_decode_rejects_truncated_body() {
    let body = vec![0u8; 10]; // way too short
    let result = WalEntry::decode_body(&body);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_decode_rejects_unknown_entry_type() {
    let entry = make_put_entry(1, b"ns", b"rec", b"key", b"val");
    let mut encoded = entry.encode().to_vec();
    let len = WalEntry::read_entry_length(&encoded).unwrap() as usize;
    // entry_type is at body offset 8 (after sequence_number)
    encoded[4 + 8] = 42;
    let body = &encoded[4..4 + len];
    let result = WalEntry::decode_body(body);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_read_entry_length_too_short() {
    let data = vec![0u8; 3];
    let result = WalEntry::read_entry_length(&data);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_decode_rejects_field_length_overflow() {
    // Create a body where namespace_len claims more bytes than available
    let mut body = vec![0u8; 45];
    // sequence_number: 0 (8 bytes)
    // entry_type: 0 (Put)
    body[8] = 0;
    // namespace_len: 0xFFFF (huge)
    body[9] = 0xFF;
    body[10] = 0xFF;
    let result = WalEntry::decode_body(&body);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

// === Conversion Tests ===

#[test]
fn test_from_memtable_entry_extracts_fields() {
    let key = CompositeKey::new(b"record1", b"item1").unwrap();
    let me = MemtableEntry::with_sequence(
        key,
        Bytes::from_static(b"value1"),
        Bytes::from_static(b"meta1"),
        IdempotencyToken::none(),
        7,
        EntryType::Put,
    );
    let wal_entry = WalEntry::from_memtable_entry(&me, b"namespace");
    assert_eq!(wal_entry.sequence_number, 7);
    assert_eq!(wal_entry.entry_type, EntryType::Put);
    assert_eq!(wal_entry.namespace.as_ref(), b"namespace");
    assert_eq!(wal_entry.record_id.as_ref(), b"record1");
    assert_eq!(wal_entry.item_key.as_ref(), b"item1");
    assert_eq!(wal_entry.item_value.as_ref(), b"value1");
    assert_eq!(wal_entry.item_metadata.as_ref(), b"meta1");
}

#[test]
fn test_to_memtable_entry_reconstructs_correctly() {
    let key = CompositeKey::new(b"rec", b"item").unwrap();
    let me = MemtableEntry::with_sequence(
        key,
        Bytes::from_static(b"val"),
        Bytes::from_static(b"meta"),
        IdempotencyToken::none(),
        10,
        EntryType::Delete,
    );
    let wal_entry = WalEntry::from_memtable_entry(&me, b"ns");
    let reconstructed = wal_entry.to_memtable_entry().unwrap();
    assert_eq!(reconstructed.record_id(), b"rec");
    assert_eq!(reconstructed.item_key(), b"item");
    assert_eq!(reconstructed.value.as_ref(), b"val");
    assert_eq!(reconstructed.metadata.as_ref(), b"meta");
    assert_eq!(reconstructed.entry_type, EntryType::Delete);
}

#[test]
fn test_roundtrip_memtable_entry_preserves_sequence() {
    let key = CompositeKey::new(b"rec", b"item").unwrap();
    let me = MemtableEntry::with_sequence(
        key,
        Bytes::from_static(b"val"),
        Bytes::new(),
        IdempotencyToken::none(),
        42,
        EntryType::Put,
    );
    let wal_entry = WalEntry::from_memtable_entry(&me, b"ns");
    let reconstructed = wal_entry.to_memtable_entry().unwrap();
    assert_eq!(reconstructed.sequence_number, 42);
}
