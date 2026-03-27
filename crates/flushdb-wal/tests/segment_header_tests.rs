use flushdb_types::FlushError;
use flushdb_wal::{SegmentHeader, SEGMENT_HEADER_SIZE, WAL_MAGIC};

// === Round-trip Tests ===

#[test]
fn test_encode_decode_roundtrip() {
    let header = SegmentHeader::new(42, 100);
    let encoded = header.encode();
    let decoded = SegmentHeader::decode(&encoded).unwrap();
    assert_eq!(header.segment_number, decoded.segment_number);
    assert_eq!(
        header.starting_sequence_number,
        decoded.starting_sequence_number
    );
    assert_eq!(header.created_at_ms, decoded.created_at_ms);
}

#[test]
fn test_encode_decode_zero_values() {
    let header = SegmentHeader {
        segment_number: 0,
        starting_sequence_number: 0,
        created_at_ms: 0,
    };
    let encoded = header.encode();
    let decoded = SegmentHeader::decode(&encoded).unwrap();
    assert_eq!(decoded, header);
}

#[test]
fn test_encode_decode_max_values() {
    let header = SegmentHeader {
        segment_number: u64::MAX,
        starting_sequence_number: u64::MAX,
        created_at_ms: u64::MAX,
    };
    let encoded = header.encode();
    let decoded = SegmentHeader::decode(&encoded).unwrap();
    assert_eq!(decoded, header);
}

// === Wire Format Tests ===

#[test]
fn test_encoded_size_is_32_bytes() {
    let header = SegmentHeader::new(1, 1);
    assert_eq!(header.encode().len(), SEGMENT_HEADER_SIZE);
}

#[test]
fn test_magic_bytes_at_offset_0() {
    let header = SegmentHeader::new(1, 1);
    let encoded = header.encode();
    assert_eq!(&encoded[0..4], &WAL_MAGIC);
}

#[test]
fn test_version_at_offset_4() {
    let header = SegmentHeader::new(1, 1);
    let encoded = header.encode();
    assert_eq!(encoded[4], 1);
}

#[test]
fn test_segment_number_little_endian() {
    let header = SegmentHeader {
        segment_number: 0x0102030405060708,
        starting_sequence_number: 0,
        created_at_ms: 0,
    };
    let encoded = header.encode();
    // segment_number at offset 8..16, little-endian
    assert_eq!(&encoded[8..16], &0x0102030405060708u64.to_le_bytes());
}

// === Validation Tests ===

#[test]
fn test_decode_rejects_short_data() {
    let data = vec![0u8; 31];
    let result = SegmentHeader::decode(&data);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_decode_rejects_wrong_magic() {
    let header = SegmentHeader::new(1, 1);
    let mut encoded = header.encode();
    encoded[0] = b'X';
    let result = SegmentHeader::decode(&encoded);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_decode_rejects_wrong_version() {
    let header = SegmentHeader::new(1, 1);
    let mut encoded = header.encode();
    encoded[4] = 2;
    let result = SegmentHeader::decode(&encoded);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_decode_accepts_nonzero_flags() {
    let header = SegmentHeader::new(1, 1);
    let mut encoded = header.encode();
    encoded[5] = 0xFF;
    let result = SegmentHeader::decode(&encoded);
    assert!(result.is_ok());
}

#[test]
fn test_decode_accepts_nonzero_reserved() {
    let header = SegmentHeader::new(1, 1);
    let mut encoded = header.encode();
    encoded[6] = 0xAA;
    encoded[7] = 0xBB;
    let result = SegmentHeader::decode(&encoded);
    assert!(result.is_ok());
}
