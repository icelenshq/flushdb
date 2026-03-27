use bytes::Bytes;
use flushdb_types::EntryValue;

#[test]
fn test_inline_creation() {
    let data = Bytes::from("hello world");
    let entry = EntryValue::Inline(data.clone());

    assert_eq!(entry.inline_value(), Some(&data));
}

#[test]
fn test_inline_value_accessor() {
    let data = Bytes::from("value");
    let inline = EntryValue::Inline(data.clone());
    assert_eq!(inline.inline_value(), Some(&data));

    let blob_ref = EntryValue::BlobRef {
        blob_id: Bytes::from("id"),
        offset: 0,
        size: 0,
    };
    assert_eq!(blob_ref.inline_value(), None);
}

#[test]
fn test_as_inline_consumes() {
    let data = Bytes::from("consumed");
    let entry = EntryValue::Inline(data.clone());

    let result = entry.as_inline();
    assert_eq!(result, Some(data));
    // `entry` is consumed and no longer accessible

    let blob_ref = EntryValue::BlobRef {
        blob_id: Bytes::from("id"),
        offset: 0,
        size: 0,
    };
    assert_eq!(blob_ref.as_inline(), None);
}

#[test]
fn test_inline_size() {
    let entry = EntryValue::Inline(Bytes::from("12345"));
    assert_eq!(entry.inline_size(), 5);

    let blob_ref = EntryValue::BlobRef {
        blob_id: Bytes::from("id"),
        offset: 0,
        size: 9999,
    };
    assert_eq!(blob_ref.inline_size(), 0);
}

#[test]
fn test_inline_empty_value() {
    let entry = EntryValue::Inline(Bytes::new());

    assert!(entry.is_inline());
    assert_eq!(entry.inline_size(), 0);
    assert_eq!(entry.inline_value(), Some(&Bytes::new()));
}

#[test]
fn test_tag_constants() {
    assert_eq!(EntryValue::INLINE_TAG, 0x00);
    assert_eq!(EntryValue::BLOB_REF_TAG, 0x01);
}

#[test]
fn test_inequality_across_variants() {
    let inline = EntryValue::Inline(Bytes::from("data"));
    let blob_ref = EntryValue::BlobRef {
        blob_id: Bytes::from("data"),
        offset: 0,
        size: 4,
    };
    assert_ne!(inline, blob_ref);
}
