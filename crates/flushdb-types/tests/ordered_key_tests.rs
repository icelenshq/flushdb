use flushdb_types::{FlushError, OrderedKey};

#[test]
fn test_new_construction() {
    let key = OrderedKey::new(1000, 42, 7);
    assert_eq!(key.timestamp_ms(), 1000);
    assert_eq!(key.node_id(), 42);
    assert_eq!(key.sequence(), 7);
}

#[test]
fn test_round_trip_bytes() {
    let original = OrderedKey::new(1_700_000_000_000, 300, 65000);
    let bytes = original.to_bytes();
    let restored = OrderedKey::from_bytes(&bytes).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn test_from_bytes_wrong_length() {
    let too_short = [0u8; 11];
    let err = OrderedKey::from_bytes(&too_short).unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidArgument { .. }),
        "expected InvalidArgument, got: {err:?}"
    );

    let too_long = [0u8; 13];
    let err = OrderedKey::from_bytes(&too_long).unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidArgument { .. }),
        "expected InvalidArgument, got: {err:?}"
    );
}

#[test]
fn test_sort_by_timestamp() {
    let a = OrderedKey::new(100, 0, 0);
    let b = OrderedKey::new(200, 0, 0);
    assert!(a < b);
}

#[test]
fn test_sort_by_node_id() {
    let a = OrderedKey::new(100, 1, 0);
    let b = OrderedKey::new(100, 2, 0);
    assert!(a < b);
}

#[test]
fn test_sort_by_sequence() {
    let a = OrderedKey::new(100, 1, 1);
    let b = OrderedKey::new(100, 1, 2);
    assert!(a < b);
}

#[test]
fn test_sort_timestamp_dominates() {
    let a = OrderedKey::new(100, 999, 999);
    let b = OrderedKey::new(101, 0, 0);
    assert!(a < b);
}

#[test]
fn test_sort_matches_byte_comparison() {
    let keys = vec![
        OrderedKey::new(300, 10, 5),
        OrderedKey::new(100, 999, 999),
        OrderedKey::new(200, 0, 0),
        OrderedKey::new(100, 0, 1),
        OrderedKey::new(300, 10, 4),
        OrderedKey::new(100, 0, 0),
    ];

    let mut sorted_by_ord = keys.clone();
    sorted_by_ord.sort();

    let mut sorted_by_bytes: Vec<([u8; 12], OrderedKey)> =
        keys.iter().map(|k| (k.to_bytes(), *k)).collect();
    sorted_by_bytes.sort_by(|a, b| a.0.cmp(&b.0));
    let sorted_by_bytes_keys: Vec<OrderedKey> = sorted_by_bytes.into_iter().map(|(_, k)| k).collect();

    assert_eq!(sorted_by_ord, sorted_by_bytes_keys);
}

#[test]
fn test_big_endian_encoding() {
    let key = OrderedKey::new(0x0102030405060708, 0x090A, 0x0B0C);
    let bytes = key.to_bytes();
    assert_eq!(
        bytes,
        [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C]
    );
}

#[test]
fn test_max_values() {
    let key = OrderedKey::new(u64::MAX, u16::MAX, u16::MAX);
    let bytes = key.to_bytes();
    let restored = OrderedKey::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
    assert_eq!(restored.timestamp_ms(), u64::MAX);
    assert_eq!(restored.node_id(), u16::MAX);
    assert_eq!(restored.sequence(), u16::MAX);
}

#[test]
fn test_zero_values() {
    let key = OrderedKey::new(0, 0, 0);
    let bytes = key.to_bytes();
    let restored = OrderedKey::from_bytes(&bytes).unwrap();
    assert_eq!(key, restored);
    assert_eq!(bytes, [0u8; 12]);
}

#[test]
fn test_copy_semantics() {
    let a = OrderedKey::new(42, 1, 2);
    let b = a; // Copy
    // Both should be usable after the assignment.
    assert_eq!(a.timestamp_ms(), 42);
    assert_eq!(b.timestamp_ms(), 42);
    assert_eq!(a, b);
}

#[test]
fn test_from_bytes_empty_input() {
    let err = OrderedKey::from_bytes(&[]).unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidArgument { .. }),
        "expected InvalidArgument for empty input, got: {err:?}"
    );
}

#[test]
fn test_equality() {
    let a = OrderedKey::new(1000, 42, 7);
    let b = OrderedKey::new(1000, 42, 7);
    assert_eq!(a, b);

    let c = OrderedKey::new(1000, 42, 8);
    assert_ne!(a, c);
}
