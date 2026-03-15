use flushdb_engine::sstable::types::{
    CompressionType, SstConfig, DEFAULT_BLOCK_SIZE, DEFAULT_BLOOM_BITS_PER_KEY,
};
use flushdb_engine::sstable::varint::{decode_varint, encode_varint, varint_len};
use flushdb_types::FlushError;

// ---------------------------------------------------------------------------
// Varint round-trip tests
// ---------------------------------------------------------------------------

#[test]
fn test_varint_zero() {
    let mut buf = Vec::new();
    encode_varint(0, &mut buf);
    assert_eq!(buf.len(), 1);

    let (value, consumed) = decode_varint(&buf).unwrap();
    assert_eq!(value, 0);
    assert_eq!(consumed, 1);
}

#[test]
fn test_varint_single_byte_max() {
    let mut buf = Vec::new();
    encode_varint(127, &mut buf);
    assert_eq!(buf.len(), 1);

    let (value, consumed) = decode_varint(&buf).unwrap();
    assert_eq!(value, 127);
    assert_eq!(consumed, 1);
}

#[test]
fn test_varint_two_byte_min() {
    let mut buf = Vec::new();
    encode_varint(128, &mut buf);
    assert_eq!(buf.len(), 2);

    let (value, consumed) = decode_varint(&buf).unwrap();
    assert_eq!(value, 128);
    assert_eq!(consumed, 2);
}

#[test]
fn test_varint_medium_value() {
    let mut buf = Vec::new();
    encode_varint(300, &mut buf);
    assert_eq!(buf.len(), 2);

    let (value, consumed) = decode_varint(&buf).unwrap();
    assert_eq!(value, 300);
    assert_eq!(consumed, 2);
}

#[test]
fn test_varint_u32_max() {
    let v = u32::MAX as u64;
    let mut buf = Vec::new();
    encode_varint(v, &mut buf);
    assert_eq!(buf.len(), 5);

    let (value, consumed) = decode_varint(&buf).unwrap();
    assert_eq!(value, v);
    assert_eq!(consumed, 5);
}

#[test]
fn test_varint_u64_max() {
    let mut buf = Vec::new();
    encode_varint(u64::MAX, &mut buf);
    assert_eq!(buf.len(), 10);

    let (value, consumed) = decode_varint(&buf).unwrap();
    assert_eq!(value, u64::MAX);
    assert_eq!(consumed, 10);
}

#[test]
fn test_varint_len_matches_encode() {
    let test_values: &[u64] = &[0, 1, 127, 128, 300, u32::MAX as u64, u64::MAX];

    for &v in test_values {
        let mut buf = Vec::new();
        encode_varint(v, &mut buf);
        assert_eq!(
            varint_len(v),
            buf.len(),
            "varint_len({v}) should match actual encoded length"
        );
    }
}

#[test]
fn test_varint_decode_empty_buffer() {
    let result = decode_varint(&[]);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_varint_decode_truncated() {
    // 0x80 has the continuation bit set, so it expects at least one more byte.
    let result = decode_varint(&[0x80]);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));

    // Two continuation bytes with no terminating byte.
    let result = decode_varint(&[0x80, 0x80]);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_varint_decode_exceeds_max_bytes() {
    // 11 continuation bytes (all have high bit set) — exceeds the 10-byte limit
    let data = [0x80u8; 11];
    let result = decode_varint(&data);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "varint exceeding 10 bytes should yield CorruptedData"
    );
}

// ---------------------------------------------------------------------------
// CompressionType tests
// ---------------------------------------------------------------------------

#[test]
fn test_compression_type_round_trip() {
    let variants = [
        CompressionType::None,
        CompressionType::Snappy,
        CompressionType::Zstd,
    ];

    for original in variants {
        let byte = original.as_u8();
        let recovered = CompressionType::from_u8(byte).unwrap();
        assert_eq!(recovered, original);
    }
}

#[test]
fn test_compression_type_invalid() {
    let result = CompressionType::from_u8(3);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_compression_type_discriminants() {
    assert_eq!(CompressionType::None.as_u8(), 0);
    assert_eq!(CompressionType::Snappy.as_u8(), 1);
    assert_eq!(CompressionType::Zstd.as_u8(), 2);
}

// ---------------------------------------------------------------------------
// SstConfig tests
// ---------------------------------------------------------------------------

#[test]
fn test_config_defaults() {
    let config = SstConfig::default();
    assert_eq!(config.block_size_target, DEFAULT_BLOCK_SIZE);
    assert_eq!(config.block_size_target, 4096);
    assert_eq!(config.compression, CompressionType::Snappy);
    assert_eq!(config.bloom_bits_per_key, DEFAULT_BLOOM_BITS_PER_KEY);
    assert_eq!(config.bloom_bits_per_key, 10);
}

#[test]
fn test_config_builder_pattern() {
    let config = SstConfig::default()
        .with_block_size(8192)
        .with_compression(CompressionType::Zstd)
        .with_bloom_bits_per_key(20);

    assert_eq!(config.block_size_target, 8192);
    assert_eq!(config.compression, CompressionType::Zstd);
    assert_eq!(config.bloom_bits_per_key, 20);
}
