use bytes::Bytes;
use flushdb_types::{EntryType, FlushError, Item};

// Item tests

#[test]
fn test_item_new_defaults() {
    let item = Item::new(Bytes::from("key"), Bytes::from("value"));
    assert_eq!(item.key, Bytes::from("key"));
    assert_eq!(item.value, Bytes::from("value"));
    assert!(item.metadata.is_empty());
    assert_eq!(item.chunk, 0);
}

#[test]
fn test_item_with_metadata() {
    let item = Item::with_metadata(
        Bytes::from("k"),
        Bytes::from("v"),
        Bytes::from("content-type:json"),
    );
    assert_eq!(item.key, Bytes::from("k"));
    assert_eq!(item.value, Bytes::from("v"));
    assert_eq!(item.metadata, Bytes::from("content-type:json"));
    assert_eq!(item.chunk, 0);
}

#[test]
fn test_item_with_all() {
    let item = Item::with_all(
        Bytes::from("k"),
        Bytes::from("v"),
        Bytes::from("meta"),
        42,
    );
    assert_eq!(item.key, Bytes::from("k"));
    assert_eq!(item.value, Bytes::from("v"));
    assert_eq!(item.metadata, Bytes::from("meta"));
    assert_eq!(item.chunk, 42);
}

#[test]
fn test_item_empty_key_and_value() {
    let item = Item::new(Bytes::new(), Bytes::new());
    assert!(item.key.is_empty());
    assert!(item.value.is_empty());
    assert!(item.metadata.is_empty());
    assert_eq!(item.chunk, 0);
}

#[test]
fn test_item_inequality() {
    let base = Item::new(Bytes::from("k"), Bytes::from("v"));

    // Different key
    let different_key = Item::new(Bytes::from("other"), Bytes::from("v"));
    assert_ne!(base, different_key);

    // Different value
    let different_value = Item::new(Bytes::from("k"), Bytes::from("other"));
    assert_ne!(base, different_value);

    // Different metadata
    let different_meta = Item::with_metadata(
        Bytes::from("k"),
        Bytes::from("v"),
        Bytes::from("meta"),
    );
    assert_ne!(base, different_meta);

    // Different chunk
    let different_chunk = Item::with_all(
        Bytes::from("k"),
        Bytes::from("v"),
        Bytes::new(),
        5,
    );
    assert_ne!(base, different_chunk);
}

// EntryType tests

#[test]
fn test_entry_type_round_trip() {
    for variant in [EntryType::Put, EntryType::Delete, EntryType::RangeDelete] {
        let byte = variant.as_u8();
        let recovered = EntryType::from_u8(byte).expect("round-trip should succeed");
        assert_eq!(variant, recovered);
    }
}

#[test]
fn test_entry_type_invalid_u8() {
    let err = EntryType::from_u8(3).unwrap_err();
    match err {
        FlushError::CorruptedData { ref message } => {
            assert!(
                message.contains("3"),
                "error message should contain the invalid value"
            );
        }
        other => panic!("expected CorruptedData, got: {other:?}"),
    }
}

#[test]
fn test_entry_type_invalid_u8_255() {
    let err = EntryType::from_u8(255).unwrap_err();
    match err {
        FlushError::CorruptedData { ref message } => {
            assert!(
                message.contains("255"),
                "error message should contain the invalid value"
            );
        }
        other => panic!("expected CorruptedData, got: {other:?}"),
    }
}

