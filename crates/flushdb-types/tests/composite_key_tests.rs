use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use flushdb_types::{
    CompositeKey, FlushError, MAX_ITEM_KEY_LEN, MAX_RECORD_ID_LEN, RANGE_TOMBSTONE_PREFIX,
};

// Round-trip tests

#[test]
fn test_round_trip_basic() {
    let key = CompositeKey::new(b"user:123", b"name").unwrap();
    assert_eq!(key.record_id(), b"user:123");
    assert_eq!(key.item_key(), b"name");
}

#[test]
fn test_round_trip_empty_item_key() {
    let key = CompositeKey::new(b"user:123", b"").unwrap();
    assert_eq!(key.record_id(), b"user:123");
    assert_eq!(key.item_key(), b"");
    assert!(key.is_empty_item_key());
}

#[test]
fn test_round_trip_binary_item_key() {
    let item_key = b"\x00\x01\x02\xff\x00\xfe";
    let key = CompositeKey::new(b"rec", item_key).unwrap();
    assert_eq!(key.record_id(), b"rec");
    assert_eq!(key.item_key(), item_key.as_slice());
}

#[test]
fn test_round_trip_max_length_keys() {
    let record_id = vec![b'a'; MAX_RECORD_ID_LEN];
    let item_key = vec![b'b'; MAX_ITEM_KEY_LEN];
    let key = CompositeKey::new(&record_id, &item_key).unwrap();
    assert_eq!(key.record_id(), record_id.as_slice());
    assert_eq!(key.item_key(), item_key.as_slice());
}

#[test]
fn test_round_trip_unicode_record_id() {
    let record_id = "日本語テスト🚀".as_bytes();
    let key = CompositeKey::new(record_id, b"val").unwrap();
    assert_eq!(key.record_id(), record_id);
    assert_eq!(key.item_key(), b"val");
}

#[test]
fn test_from_record_only() {
    let key = CompositeKey::from_record_only(b"rec").unwrap();
    assert_eq!(key.record_id(), b"rec");
    assert!(key.is_empty_item_key());
    assert_eq!(key.item_key(), b"");
}

// Sort order tests

#[test]
fn test_sort_different_record_ids() {
    let a = CompositeKey::new(b"a", b"z").unwrap();
    let b = CompositeKey::new(b"b", b"a").unwrap();
    assert!(a < b);
}

#[test]
fn test_sort_same_record_different_items() {
    let a = CompositeKey::new(b"r", b"a").unwrap();
    let b = CompositeKey::new(b"r", b"b").unwrap();
    assert!(a < b);
}

#[test]
fn test_sort_empty_item_key_first() {
    let a = CompositeKey::new(b"r", b"").unwrap();
    let b = CompositeKey::new(b"r", b"a").unwrap();
    assert!(a < b);
}

#[test]
fn test_sort_null_byte_in_item_key() {
    let a = CompositeKey::new(b"r", b"\x00").unwrap();
    let b = CompositeKey::new(b"r", b"\x01").unwrap();
    assert!(a < b);
}

#[test]
fn test_sort_range_tombstone_after_data() {
    let data = CompositeKey::new(b"r", b"z").unwrap();
    let tombstone = CompositeKey::range_tombstone_key(b"r", b"a").unwrap();
    assert!(
        data < tombstone,
        "range tombstone must sort after data keys"
    );
}

#[test]
fn test_sort_memcmp_equivalence() {
    let keys = vec![
        CompositeKey::new(b"c", b"x").unwrap(),
        CompositeKey::new(b"a", b"z").unwrap(),
        CompositeKey::new(b"b", b"").unwrap(),
        CompositeKey::new(b"a", b"a").unwrap(),
        CompositeKey::new(b"b", b"b").unwrap(),
        CompositeKey::new(b"a", b"").unwrap(),
    ];

    let mut sorted_by_ord = keys.clone();
    sorted_by_ord.sort();

    let mut raw_pairs: Vec<(Vec<u8>, usize)> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.as_bytes().to_vec(), i))
        .collect();
    raw_pairs.sort_by(|a, b| a.0.cmp(&b.0));

    let sorted_by_bytes: Vec<CompositeKey> = raw_pairs
        .into_iter()
        .map(|(_, i)| keys[i].clone())
        .collect();

    assert_eq!(sorted_by_ord, sorted_by_bytes);
}

#[test]
fn test_sort_cross_record_boundary() {
    let aaa_keys = vec![
        CompositeKey::new(b"aaa", b"z").unwrap(),
        CompositeKey::new(b"aaa", b"a").unwrap(),
        CompositeKey::new(b"aaa", b"m").unwrap(),
    ];
    let aab_keys = vec![
        CompositeKey::new(b"aab", b"a").unwrap(),
        CompositeKey::new(b"aab", b"").unwrap(),
    ];

    for aaa in &aaa_keys {
        for aab in &aab_keys {
            assert!(aaa < aab, "all 'aaa' keys must sort before any 'aab' key");
        }
    }
}

// Validation tests

#[test]
fn test_rejects_empty_record_id() {
    let err = CompositeKey::new(b"", b"val").unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidKey { ref reason } if reason.contains("empty")),
        "expected InvalidKey about empty, got: {err:?}"
    );
}

#[test]
fn test_rejects_null_in_record_id() {
    let err = CompositeKey::new(b"a\x00b", b"val").unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidKey { ref reason } if reason.contains("null")),
        "expected InvalidKey about null bytes, got: {err:?}"
    );
}

#[test]
fn test_rejects_oversized_record_id() {
    let big = vec![b'x'; MAX_RECORD_ID_LEN + 1];
    let err = CompositeKey::new(&big, b"val").unwrap_err();
    assert!(
        matches!(
            err,
            FlushError::KeyTooLong {
                field: "record_id",
                actual,
                max: MAX_RECORD_ID_LEN
            } if actual == MAX_RECORD_ID_LEN + 1
        ),
        "expected KeyTooLong for record_id, got: {err:?}"
    );
}

#[test]
fn test_rejects_oversized_item_key() {
    let big = vec![b'y'; MAX_ITEM_KEY_LEN + 1];
    let err = CompositeKey::new(b"r", &big).unwrap_err();
    assert!(
        matches!(
            err,
            FlushError::KeyTooLong {
                field: "item_key",
                actual,
                max: MAX_ITEM_KEY_LEN
            } if actual == MAX_ITEM_KEY_LEN + 1
        ),
        "expected KeyTooLong for item_key, got: {err:?}"
    );
}

#[test]
fn test_accepts_max_size_record_id() {
    let record_id = vec![b'a'; MAX_RECORD_ID_LEN];
    let key = CompositeKey::new(&record_id, b"").unwrap();
    assert_eq!(key.record_id().len(), MAX_RECORD_ID_LEN);
}

#[test]
fn test_accepts_max_size_item_key() {
    let item_key = vec![b'b'; MAX_ITEM_KEY_LEN];
    let key = CompositeKey::new(b"r", &item_key).unwrap();
    assert_eq!(key.item_key().len(), MAX_ITEM_KEY_LEN);
}

// Range tombstone tests

#[test]
fn test_range_tombstone_key_encoding() {
    let key = CompositeKey::range_tombstone_key(b"rec", b"start").unwrap();
    let item = key.item_key();
    assert_eq!(item[0], RANGE_TOMBSTONE_PREFIX);
    assert_eq!(&item[1..], b"start");
}

#[test]
fn test_range_tombstone_is_detected() {
    let key = CompositeKey::range_tombstone_key(b"rec", b"sk").unwrap();
    assert!(key.is_range_tombstone());
}

#[test]
fn test_data_key_not_tombstone() {
    let key = CompositeKey::new(b"rec", b"name").unwrap();
    assert!(!key.is_range_tombstone());
}

#[test]
fn test_range_tombstone_sorts_after_all_data() {
    // All printable and high-value data keys should sort before a tombstone
    // in the same record, because 0xFF > any other first byte of item_key.
    let data_keys: Vec<CompositeKey> = (0u8..=0xFE)
        .map(|b| CompositeKey::new(b"rec", &[b]).unwrap())
        .collect();
    let tombstone = CompositeKey::range_tombstone_key(b"rec", b"").unwrap();

    for dk in &data_keys {
        assert!(
            dk < &tombstone,
            "data key {:?} must sort before tombstone",
            dk.item_key()
        );
    }
}

// Edge-case tests

#[test]
fn test_from_bytes_valid() {
    let original = CompositeKey::new(b"myrecord", b"field").unwrap();
    let raw = Bytes::copy_from_slice(original.as_bytes());
    let parsed = CompositeKey::from_bytes(raw).unwrap();
    assert_eq!(parsed.record_id(), b"myrecord");
    assert_eq!(parsed.item_key(), b"field");
}

#[test]
fn test_from_bytes_no_separator() {
    let raw = Bytes::from_static(b"noseparator");
    let err = CompositeKey::from_bytes(raw).unwrap_err();
    assert!(
        matches!(err, FlushError::InvalidKey { .. }),
        "expected InvalidKey, got: {err:?}"
    );
}

#[test]
fn test_single_byte_record_id() {
    let key = CompositeKey::new(b"a", b"val").unwrap();
    assert_eq!(key.record_id(), b"a");
    assert_eq!(key.item_key(), b"val");
}

#[test]
fn test_clone_is_cheap() {
    let key = CompositeKey::new(b"rec", b"item").unwrap();
    let cloned = key.clone();
    // Bytes uses reference counting — the underlying data pointer is shared.
    assert_eq!(key.as_bytes().as_ptr(), cloned.as_bytes().as_ptr());
    assert_eq!(key, cloned);
}

#[test]
fn test_into_bytes() {
    let key = CompositeKey::new(b"user", b"email").unwrap();
    let raw = key.as_bytes().to_vec();
    let owned: Bytes = key.into_bytes();
    assert_eq!(owned.as_ref(), raw.as_slice());
}

#[test]
fn test_min_key_for_record() {
    let min = CompositeKey::min_key_for_record(b"rec").unwrap();
    // min_key_for_record is equivalent to from_record_only: [rec][0x00]
    let from_record = CompositeKey::from_record_only(b"rec").unwrap();
    assert_eq!(min, from_record);
    assert!(min.is_empty_item_key());
}

#[test]
fn test_max_key_for_record() {
    let max = CompositeKey::max_key_for_record(b"rec").unwrap();
    // max_key_for_record produces [rec][0x00][0xFF]
    assert_eq!(max.record_id(), b"rec");
    assert_eq!(max.item_key(), &[RANGE_TOMBSTONE_PREFIX]);
}

#[test]
fn test_max_key_sorts_between_data_and_tombstone() {
    let data = CompositeKey::new(b"rec", b"\xFE").unwrap();
    let max = CompositeKey::max_key_for_record(b"rec").unwrap();
    let tombstone = CompositeKey::range_tombstone_key(b"rec", b"a").unwrap();

    // data < max <= tombstone (max has just 0xFF, tombstone has 0xFF + start_key)
    assert!(data < max, "data keys must sort before max_key");
    assert!(
        max <= tombstone,
        "max_key must sort at or before tombstone keys with payload"
    );
}

#[test]
fn test_from_bytes_with_multiple_separators() {
    // item_key can contain 0x00 bytes; only the first 0x00 is the separator.
    let key = CompositeKey::new(b"rec", b"a\x00b").unwrap();
    let raw = Bytes::copy_from_slice(key.as_bytes());
    let parsed = CompositeKey::from_bytes(raw).unwrap();
    assert_eq!(parsed.record_id(), b"rec");
    assert_eq!(parsed.item_key(), b"a\x00b");
}

#[test]
fn test_hash_consistency() {
    let a = CompositeKey::new(b"rec", b"item").unwrap();
    let b = CompositeKey::new(b"rec", b"item").unwrap();
    assert_eq!(a, b);

    fn hash_of(key: &CompositeKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    assert_eq!(
        hash_of(&a),
        hash_of(&b),
        "equal keys must have equal hashes"
    );
}

#[test]
fn test_min_max_key_validation() {
    // Empty record_id should fail for both min and max.
    assert!(CompositeKey::min_key_for_record(b"").is_err());
    assert!(CompositeKey::max_key_for_record(b"").is_err());
}

#[test]
fn test_range_tombstone_key_oversized_start_key() {
    let oversized = vec![b'x'; MAX_ITEM_KEY_LEN]; // 1 prefix byte + 4096 data bytes > MAX
    let result = CompositeKey::range_tombstone_key(b"rec", &oversized);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::KeyTooLong { field, actual, max } => {
            assert_eq!(field, "item_key");
            assert_eq!(actual, 1 + oversized.len());
            assert_eq!(max, MAX_ITEM_KEY_LEN);
        }
        other => panic!("expected KeyTooLong, got: {other:?}"),
    }
}

#[test]
fn test_range_tombstone_key_at_exact_limit() {
    let exact = vec![b'x'; MAX_ITEM_KEY_LEN - 1]; // 1 prefix + 4095 = 4096 = MAX
    let result = CompositeKey::range_tombstone_key(b"rec", &exact);
    assert!(result.is_ok());
}

#[test]
fn test_range_tombstone_key_empty_record_id_rejected() {
    let result = CompositeKey::range_tombstone_key(b"", b"start");
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidKey { reason } => {
            assert!(reason.contains("empty"), "got: {reason}");
        }
        other => panic!("expected InvalidKey, got: {other:?}"),
    }
}
