use std::collections::HashSet;

use bytes::Bytes;
use flushdb_engine::sstable::bloom_filter::{BloomFilter, BloomFilterBuilder, FilterBlock};
use flushdb_types::FlushError;

fn make_record_id(i: u64) -> Vec<u8> {
    format!("record_{i:06}").into_bytes()
}

fn build_filter_with_keys(n: u64) -> BloomFilter {
    let mut builder = BloomFilterBuilder::new(10);
    for i in 0..n {
        builder.add(&make_record_id(i));
    }
    builder.build()
}

// ─── No False Negatives ──────────────────────────────────────────────

#[test]
fn test_no_false_negatives_1000_keys() {
    let filter = build_filter_with_keys(1000);

    for i in 0..1000 {
        assert!(
            filter.maybe_contains(&make_record_id(i)),
            "inserted key {i} must be found"
        );
    }
}

#[test]
fn test_no_false_negatives_single_key() {
    let mut builder = BloomFilterBuilder::new(10);
    builder.add(b"only_key");
    let filter = builder.build();

    assert!(filter.maybe_contains(b"only_key"));
}

#[test]
fn test_no_false_negatives_after_serialize() {
    let filter = build_filter_with_keys(1000);
    let serialized = filter.serialize();
    let restored = BloomFilter::deserialize(&serialized).expect("deserialize must succeed");

    for i in 0..1000 {
        assert!(
            restored.maybe_contains(&make_record_id(i)),
            "key {i} must survive serialization round-trip"
        );
    }
}

// ─── False Positive Rate ─────────────────────────────────────────────

#[test]
fn test_false_positive_rate_under_threshold() {
    let filter = build_filter_with_keys(10_000);

    let num_absent = 10_000u64;
    let mut false_positives = 0u64;
    for i in 0..num_absent {
        let absent_key = format!("absent_{i:08}").into_bytes();
        if filter.maybe_contains(&absent_key) {
            false_positives += 1;
        }
    }

    let fpr = false_positives as f64 / num_absent as f64;
    assert!(
        fpr < 0.02,
        "false positive rate {fpr:.4} exceeds 2% threshold"
    );
}

#[test]
fn test_empty_filter_always_false() {
    let builder = BloomFilterBuilder::new(10);
    let filter = builder.build();

    assert!(!filter.maybe_contains(b"anything"));
    assert!(!filter.maybe_contains(b""));
    assert!(!filter.maybe_contains(b"some_other_key"));
}

// ─── Serialization ──────────────────────────────────────────────────

#[test]
fn test_serialize_deserialize_round_trip() {
    let filter = build_filter_with_keys(500);
    let serialized = filter.serialize();
    let restored = BloomFilter::deserialize(&serialized).expect("deserialize must succeed");

    assert_eq!(filter.size_bytes(), restored.size_bytes());
    assert!(
        (filter.false_positive_rate() - restored.false_positive_rate()).abs() < f64::EPSILON,
        "false_positive_rate must be preserved"
    );

    for i in 0..500 {
        let key = make_record_id(i);
        assert_eq!(filter.maybe_contains(&key), restored.maybe_contains(&key));
    }
}

#[test]
fn test_serialize_deterministic() {
    let keys: Vec<Vec<u8>> = (0..200).map(make_record_id).collect();

    let mut builder_a = BloomFilterBuilder::new(10);
    let mut builder_b = BloomFilterBuilder::new(10);
    for key in &keys {
        builder_a.add(key);
        builder_b.add(key);
    }

    let bytes_a = builder_a.build().serialize();
    let bytes_b = builder_b.build().serialize();
    assert_eq!(
        bytes_a, bytes_b,
        "same keys must produce identical serialized bytes"
    );
}

#[test]
fn test_deserialize_rejects_truncated() {
    let short = [0u8; 19];
    let result = BloomFilter::deserialize(&short);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "expected CorruptedData for input shorter than 20-byte header"
    );
}

// ─── FilterBlock Enum ───────────────────────────────────────────────

#[test]
fn test_filter_block_bloom_round_trip() {
    let filter = build_filter_with_keys(100);
    let block = FilterBlock::Bloom(filter);
    let serialized = block.serialize();
    let restored = FilterBlock::deserialize(&serialized).expect("deserialize must succeed");

    for i in 0..100 {
        let key = make_record_id(i);
        assert!(
            restored.maybe_contains(&key),
            "FilterBlock round-trip must preserve membership for key {i}"
        );
    }
}

#[test]
fn test_filter_block_serialize_has_type_prefix() {
    let filter = build_filter_with_keys(10);
    let block = FilterBlock::Bloom(filter);
    let serialized = block.serialize();

    assert_eq!(serialized[0], 0x00, "first byte must be the Bloom tag 0x00");
}

#[test]
fn test_filter_block_deserialize_unknown_type() {
    let data = [0xFF, 0x01, 0x02, 0x03];
    let result = FilterBlock::deserialize(&data);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "unknown type prefix 0xFF must yield CorruptedData"
    );
}

#[test]
fn test_filter_block_deserialize_empty_data() {
    let result = FilterBlock::deserialize(&[]);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "empty data must yield CorruptedData"
    );
}

// ─── Builder ─────────────────────────────────────────────────────────

#[test]
fn test_builder_deduplicates_record_ids() {
    let mut builder_once = BloomFilterBuilder::new(10);
    builder_once.add(b"duplicate_key");
    let filter_once = builder_once.build();

    let mut builder_many = BloomFilterBuilder::new(10);
    for _ in 0..100 {
        builder_many.add(b"duplicate_key");
    }
    let filter_many = builder_many.build();

    let bytes_once = filter_once.serialize();
    let bytes_many = filter_many.serialize();
    assert_eq!(
        bytes_once, bytes_many,
        "adding the same key 100 times must produce the same filter as adding it once"
    );
}

#[test]
fn test_builder_add_all_from_hashset() {
    let keys: HashSet<Bytes> = (0..50).map(|i| Bytes::from(make_record_id(i))).collect();

    let mut builder_individual = BloomFilterBuilder::new(10);
    for k in &keys {
        builder_individual.add(k);
    }
    let filter_individual = builder_individual.build();

    let mut builder_batch = BloomFilterBuilder::new(10);
    builder_batch.add_all(&keys);
    let filter_batch = builder_batch.build();

    assert_eq!(
        filter_individual.serialize(),
        filter_batch.serialize(),
        "add_all must produce the same filter as individual add calls"
    );
}

#[test]
fn test_builder_estimated_size() {
    let mut builder = BloomFilterBuilder::new(10);
    for i in 0..1000 {
        builder.add(&make_record_id(i));
    }
    let estimated = builder.estimated_size_bytes();
    let filter = builder.build();
    let actual = filter.size_bytes();

    assert!(
        estimated <= actual * 2,
        "estimated size {estimated} should be within 2x of actual size {actual}"
    );
    assert!(
        actual <= estimated * 2,
        "actual size {actual} should be within 2x of estimated size {estimated}"
    );
}

// ─── Theoretical False Positive Rate ─────────────────────────────────

#[test]
fn test_false_positive_rate_method_under_threshold() {
    let filter = build_filter_with_keys(10_000);
    let fpr = filter.false_positive_rate();
    assert!(
        fpr < 0.02,
        "false_positive_rate() returned {fpr:.6}, expected < 0.02 for 10 bits/key"
    );
}

// ─── Hash Distribution ──────────────────────────────────────────────

#[test]
fn test_different_keys_different_positions() {
    let mut builder = BloomFilterBuilder::new(10);
    builder.add(b"key_alpha");
    let filter = builder.build();

    assert!(filter.maybe_contains(b"key_alpha"));
    assert!(
        !filter.maybe_contains(b"key_beta"),
        "a distinct key should (with high probability) not be found in a single-key filter"
    );
}
