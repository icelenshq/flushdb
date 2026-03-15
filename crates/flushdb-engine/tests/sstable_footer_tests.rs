use flushdb_engine::sstable::footer::{SstFooter, SstHeader};
use flushdb_engine::sstable::types::{
    CompressionType, FOOTER_SIZE, HEADER_SIZE, MIN_MAX_KEY_TRUNCATION_LEN,
};
use flushdb_types::{CompositeKey, FlushError};

fn make_footer(compression: CompressionType) -> SstFooter {
    let min_key = SstFooter::truncate_key(
        &CompositeKey::new(b"aaa", b"000").unwrap(),
    );
    let max_key = SstFooter::truncate_key(
        &CompositeKey::new(b"zzz", b"999").unwrap(),
    );

    SstFooter {
        bloom_filter_offset: 8192,
        bloom_filter_size: 512,
        index_block_offset: 8704,
        index_block_size: 256,
        entry_count: 1000,
        min_key,
        max_key,
        compression_type: compression,
        format_version: 1,
        dedup_block_size: 128,
    }
}

// ─── Round-trip tests ────────────────────────────────────────────────

#[test]
fn test_footer_round_trip() {
    let footer = make_footer(CompressionType::Snappy);
    let encoded = footer.encode();
    let decoded = SstFooter::decode(&encoded).unwrap();
    assert_eq!(footer, decoded);
}

#[test]
fn test_footer_round_trip_all_compression_types() {
    for compression in [CompressionType::None, CompressionType::Snappy, CompressionType::Zstd] {
        let footer = make_footer(compression);
        let encoded = footer.encode();
        let decoded = SstFooter::decode(&encoded).unwrap();
        assert_eq!(decoded.compression_type, compression);
        assert_eq!(footer, decoded);
    }
}

#[test]
fn test_footer_encode_produces_80_bytes() {
    let footer = make_footer(CompressionType::None);
    let encoded = footer.encode();
    assert_eq!(encoded.len(), 80);
    assert_eq!(encoded.len(), FOOTER_SIZE);
}

#[test]
fn test_header_round_trip() {
    let header = SstHeader {
        compression: CompressionType::Zstd,
        entry_count: 42_000,
    };
    let encoded = header.encode();
    let decoded = SstHeader::decode(&encoded).unwrap();
    assert_eq!(header, decoded);
}

#[test]
fn test_header_encode_produces_16_bytes() {
    let header = SstHeader {
        compression: CompressionType::None,
        entry_count: 0,
    };
    let encoded = header.encode();
    assert_eq!(encoded.len(), 16);
    assert_eq!(encoded.len(), HEADER_SIZE);
}

// ─── Validation tests ────────────────────────────────────────────────

#[test]
fn test_footer_rejects_wrong_magic() {
    let footer = make_footer(CompressionType::None);
    let mut encoded = footer.encode();
    // Magic lives at bytes [72..76]; corrupt it.
    encoded[72] = 0xFF;
    encoded[73] = 0xFF;
    encoded[74] = 0xFF;
    encoded[75] = 0xFF;

    let result = SstFooter::decode(&encoded);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_footer_rejects_corrupted_crc() {
    let footer = make_footer(CompressionType::None);
    let mut encoded = footer.encode();

    // Flip a data byte in the CRC-protected region [0..68) so the CRC no longer matches.
    encoded[0] ^= 0x01;

    let result = SstFooter::decode(&encoded);
    match result {
        Err(FlushError::CrcMismatch { expected, actual }) => {
            // `expected` is what was stored (computed from the original data),
            // `actual` is recomputed from the corrupted data — they must differ.
            assert_ne!(expected, actual);
        }
        other => panic!("expected CrcMismatch, got {other:?}"),
    }
}

#[test]
fn test_footer_rejects_short_input() {
    let short = [0u8; 79];
    let result = SstFooter::decode(&short);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_header_rejects_wrong_magic() {
    let header = SstHeader {
        compression: CompressionType::None,
        entry_count: 1,
    };
    let mut encoded = header.encode();
    // Magic lives at bytes [0..4] in the header.
    encoded[0] = 0x00;
    encoded[1] = 0x00;
    encoded[2] = 0x00;
    encoded[3] = 0x00;

    let result = SstHeader::decode(&encoded);
    assert!(matches!(result, Err(FlushError::CorruptedData { .. })));
}

#[test]
fn test_header_rejects_short_input() {
    let short = [0u8; 15];
    let result = SstHeader::decode(&short);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "header shorter than 16 bytes should yield CorruptedData"
    );
}

#[test]
fn test_header_rejects_invalid_compression() {
    let header = SstHeader {
        compression: CompressionType::None,
        entry_count: 1,
    };
    let mut encoded = header.encode();
    encoded[6] = 0xFF;

    let result = SstHeader::decode(&encoded);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "invalid compression byte 0xFF should yield CorruptedData"
    );
}

// ─── Key truncation tests ────────────────────────────────────────────

#[test]
fn test_truncate_short_key() {
    // "abc" + 0x00 separator + "d" = 5 bytes
    let key = CompositeKey::new(b"abc", b"d").unwrap();
    assert_eq!(key.as_bytes().len(), 5);

    let truncated = SstFooter::truncate_key(&key);
    assert_eq!(truncated.len(), MIN_MAX_KEY_TRUNCATION_LEN);

    // First 5 bytes are the key bytes, remaining 11 are zero-padding.
    assert_eq!(&truncated[..5], key.as_bytes());
    assert_eq!(&truncated[5..], &[0u8; 11]);
}

#[test]
fn test_truncate_exact_16_key() {
    // Need record_id + 0x00 + item_key = 16 bytes.
    // record_id = "abcdefg" (7 bytes), separator = 1 byte, item_key = "hijklmnp" (8 bytes) → 16.
    let key = CompositeKey::new(b"abcdefg", b"hijklmnp").unwrap();
    assert_eq!(key.as_bytes().len(), 16);

    let truncated = SstFooter::truncate_key(&key);
    assert_eq!(&truncated[..], key.as_bytes());
}

#[test]
fn test_truncate_long_key() {
    // "long_record_id_" (15) + 0x00 (1) + "some_item" (9) = 25 bytes, well over 16.
    let key = CompositeKey::new(b"long_record_id_", b"some_item").unwrap();
    assert!(key.as_bytes().len() > MIN_MAX_KEY_TRUNCATION_LEN);

    let truncated = SstFooter::truncate_key(&key);
    assert_eq!(truncated.len(), MIN_MAX_KEY_TRUNCATION_LEN);
    assert_eq!(&truncated[..], &key.as_bytes()[..16]);
}

#[test]
fn test_may_contain_key_in_range() {
    let footer = make_footer(CompressionType::None);

    // min_key is from ("aaa", "000"), max_key is from ("zzz", "999").
    // A key in between should be contained.
    let mid_key = CompositeKey::new(b"mmm", b"500").unwrap();
    assert!(footer.may_contain_key(&mid_key));

    // Keys equal to boundaries should also be contained.
    let min_boundary = CompositeKey::new(b"aaa", b"000").unwrap();
    let max_boundary = CompositeKey::new(b"zzz", b"999").unwrap();
    assert!(footer.may_contain_key(&min_boundary));
    assert!(footer.may_contain_key(&max_boundary));
}

#[test]
fn test_may_contain_key_outside_range() {
    let footer = make_footer(CompressionType::None);

    // A key whose truncated form is lexicographically before the min.
    // min is from ("aaa","000") → starts with b'a'; use a key starting with 0x01.
    let before_key = CompositeKey::new(b"\x01\x01\x01", b"").unwrap();
    let truncated_before = SstFooter::truncate_key(&before_key);
    assert!(truncated_before < footer.min_key);
    assert!(!footer.may_contain_key(&before_key));

    // A key whose truncated form is lexicographically after the max.
    // max is from ("zzz","999") → starts with b'z'; use a key starting with 0x7F.
    let after_key = CompositeKey::new(b"\x7f\x7f\x7f", b"\x7f\x7f").unwrap();
    let truncated_after = SstFooter::truncate_key(&after_key);
    assert!(truncated_after > footer.max_key);
    assert!(!footer.may_contain_key(&after_key));
}

// ─── Derived fields tests ────────────────────────────────────────────

#[test]
fn test_dedup_block_offset() {
    let test_cases: Vec<(u64, u32)> = vec![
        (8192, 128),   // 8192 - 128 = 8064
        (4096, 0),     // 4096 - 0 = 4096
        (10000, 1000), // 10000 - 1000 = 9000
        (256, 256),    // 256 - 256 = 0
    ];

    for (bloom_offset, dedup_size) in test_cases {
        let mut footer = make_footer(CompressionType::None);
        footer.bloom_filter_offset = bloom_offset;
        footer.dedup_block_size = dedup_size;

        assert_eq!(
            footer.dedup_block_offset(),
            bloom_offset - dedup_size as u64,
            "dedup_block_offset mismatch for bloom_filter_offset={bloom_offset}, dedup_block_size={dedup_size}"
        );
    }
}
