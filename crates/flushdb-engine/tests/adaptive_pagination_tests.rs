use flushdb_engine::cache::{NamespaceSizeEstimator, DEFAULT_ITEM_SIZE, MIN_ITEMS_PER_PAGE};
use flushdb_engine::read_path::PageToken;
use flushdb_types::CompositeKey;

// === NamespaceSizeEstimator Tests ===

#[test]
fn test_empty_estimator_returns_none() {
    let estimator = NamespaceSizeEstimator::new();
    assert!(estimator.estimate_avg_item_size("unknown").is_none());
}

#[test]
fn test_record_and_estimate() {
    let mut estimator = NamespaceSizeEstimator::new();
    estimator.record_items("ns1", 5000, 10);
    assert_eq!(estimator.estimate_avg_item_size("ns1"), Some(500));
}

#[test]
fn test_estimate_item_count() {
    let mut estimator = NamespaceSizeEstimator::new();
    estimator.record_items("ns1", 5000, 10); // avg = 500
    assert_eq!(estimator.estimate_item_count("ns1", 2048), 4);
}

#[test]
fn test_estimate_item_count_default() {
    let estimator = NamespaceSizeEstimator::new();
    let count = estimator.estimate_item_count("unknown", 4096);
    assert_eq!(count, 4096 / DEFAULT_ITEM_SIZE);
}

#[test]
fn test_multiple_namespaces_independent() {
    let mut estimator = NamespaceSizeEstimator::new();
    estimator.record_items("ns1", 1000, 10); // avg = 100
    estimator.record_items("ns2", 5000, 10); // avg = 500

    assert_eq!(estimator.estimate_avg_item_size("ns1"), Some(100));
    assert_eq!(estimator.estimate_avg_item_size("ns2"), Some(500));
}

#[test]
fn test_running_average_updates() {
    let mut estimator = NamespaceSizeEstimator::new();
    estimator.record_items("ns1", 1000, 10); // avg = 100
    estimator.record_items("ns1", 3000, 10); // cumulative: 4000/20 = 200

    assert_eq!(estimator.estimate_avg_item_size("ns1"), Some(200));
}

#[test]
fn test_reset() {
    let mut estimator = NamespaceSizeEstimator::new();
    estimator.record_items("ns1", 5000, 10);
    assert!(estimator.estimate_avg_item_size("ns1").is_some());

    estimator.reset("ns1");
    assert!(estimator.estimate_avg_item_size("ns1").is_none());
}

#[test]
fn test_estimate_item_count_min_items() {
    let mut estimator = NamespaceSizeEstimator::new();
    // avg item = 10000, page_size = 100 -> 0 items, but min is 1
    estimator.record_items("ns1", 10000, 1);
    assert_eq!(
        estimator.estimate_item_count("ns1", 100),
        MIN_ITEMS_PER_PAGE
    );
}

// === PageToken with avg_item_size_bytes Tests ===

#[test]
fn test_page_token_with_avg_round_trip() {
    let key = CompositeKey::new(b"rec1", b"item42").unwrap();
    let token = PageToken {
        last_composite_key: key.clone(),
        last_sequence_number: 12345,
        avg_item_size_bytes: Some(512),
    };

    let encoded = token.encode();
    let decoded = PageToken::decode(&encoded).unwrap();

    assert_eq!(decoded.last_composite_key, key);
    assert_eq!(decoded.last_sequence_number, 12345);
    assert_eq!(decoded.avg_item_size_bytes, Some(512));
}

#[test]
fn test_page_token_without_avg_round_trip() {
    let key = CompositeKey::new(b"rec1", b"item42").unwrap();
    let token = PageToken {
        last_composite_key: key.clone(),
        last_sequence_number: 12345,
        avg_item_size_bytes: None,
    };

    let encoded = token.encode();
    let decoded = PageToken::decode(&encoded).unwrap();

    assert_eq!(decoded.last_composite_key, key);
    assert_eq!(decoded.last_sequence_number, 12345);
    assert_eq!(decoded.avg_item_size_bytes, None);
}

#[test]
fn test_page_token_backwards_compatible() {
    let key = CompositeKey::new(b"rec1", b"item42").unwrap();
    let key_bytes = key.as_bytes();
    let key_len = key_bytes.len() as u32;
    let seq: u64 = 12345;

    // Build old-format token (no avg field)
    let mut old_data = Vec::new();
    old_data.extend_from_slice(&key_len.to_le_bytes());
    old_data.extend_from_slice(&key_bytes);
    old_data.extend_from_slice(&seq.to_le_bytes());

    let decoded = PageToken::decode(&old_data).unwrap();
    assert_eq!(decoded.last_composite_key, key);
    assert_eq!(decoded.last_sequence_number, 12345);
    assert_eq!(decoded.avg_item_size_bytes, None);
}

#[test]
fn test_page_token_base64_round_trip_with_avg() {
    let key = CompositeKey::new(b"rec1", b"item42").unwrap();
    let token = PageToken {
        last_composite_key: key.clone(),
        last_sequence_number: 999,
        avg_item_size_bytes: Some(2048),
    };

    let b64 = token.to_base64();
    let decoded = PageToken::from_base64(&b64).unwrap();

    assert_eq!(decoded.last_composite_key, key);
    assert_eq!(decoded.last_sequence_number, 999);
    assert_eq!(decoded.avg_item_size_bytes, Some(2048));
}
