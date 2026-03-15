use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, IdempotencyToken, MemtableEntry};

// ────────────────────────────────────────────────────────────────────
// Helpers
// ────────────────────────────────────────────────────────────────────

fn make_key(record: &[u8], item: &[u8]) -> CompositeKey {
    CompositeKey::new(record, item).expect("valid composite key")
}

fn make_put_entry(record: &[u8], item: &[u8], value: &[u8]) -> MemtableEntry {
    MemtableEntry::new(
        make_key(record, item),
        Bytes::from(value.to_vec()),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Put,
    )
}

fn make_delete_entry(record: &[u8], item: &[u8]) -> MemtableEntry {
    MemtableEntry::new(
        make_key(record, item),
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Delete,
    )
}

fn make_range_delete_entry(record: &[u8], start_key: &[u8], end_key: &[u8]) -> MemtableEntry {
    MemtableEntry::new(
        make_key(record, start_key),
        Bytes::from(end_key.to_vec()),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::RangeDelete,
    )
}

// ────────────────────────────────────────────────────────────────────
// Construction tests
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_new_defaults_sequence_to_zero() {
    let entry = make_put_entry(b"rec", b"item", b"val");
    assert_eq!(entry.sequence_number, 0);
}

#[test]
fn test_with_sequence() {
    let entry = MemtableEntry::with_sequence(
        make_key(b"rec", b"item"),
        Bytes::from("value"),
        Bytes::from("meta"),
        IdempotencyToken::none(),
        42,
        EntryType::Put,
    );
    assert_eq!(entry.sequence_number, 42);
}

// ────────────────────────────────────────────────────────────────────
// Delegation tests
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_record_id_delegation() {
    let entry = make_put_entry(b"my-record", b"my-item", b"v");
    assert_eq!(entry.record_id(), b"my-record");
    assert_eq!(entry.record_id(), entry.composite_key.record_id());
}

#[test]
fn test_item_key_delegation() {
    let entry = make_put_entry(b"rec", b"my-item-key", b"v");
    assert_eq!(entry.item_key(), b"my-item-key");
    assert_eq!(entry.item_key(), entry.composite_key.item_key());
}

// ────────────────────────────────────────────────────────────────────
// Tombstone / Put predicate tests
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_is_tombstone_delete() {
    let entry = make_delete_entry(b"rec", b"item");
    assert!(entry.is_tombstone());
}

#[test]
fn test_is_tombstone_range_delete() {
    let entry = make_range_delete_entry(b"rec", b"start", b"end");
    assert!(entry.is_tombstone());
}

#[test]
fn test_is_tombstone_put() {
    let entry = make_put_entry(b"rec", b"item", b"val");
    assert!(!entry.is_tombstone());
}

#[test]
fn test_is_put() {
    let put = make_put_entry(b"rec", b"item", b"val");
    assert!(put.is_put());

    let delete = make_delete_entry(b"rec", b"item");
    assert!(!delete.is_put());

    let range_delete = make_range_delete_entry(b"rec", b"s", b"e");
    assert!(!range_delete.is_put());
}

// ────────────────────────────────────────────────────────────────────
// Value semantics tests
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_put_entry_has_value() {
    let entry = make_put_entry(b"rec", b"item", b"hello world");
    assert!(!entry.value.is_empty());
    assert_eq!(entry.value.as_ref(), b"hello world");
}

#[test]
fn test_delete_entry_empty_value() {
    let entry = make_delete_entry(b"rec", b"item");
    assert!(entry.value.is_empty());
}

#[test]
fn test_range_delete_entry_value_is_end_key() {
    let entry = make_range_delete_entry(b"rec", b"start", b"end-key");
    assert_eq!(entry.value.as_ref(), b"end-key");
}

// ────────────────────────────────────────────────────────────────────
// Clone and equality tests
// ────────────────────────────────────────────────────────────────────

#[test]
fn test_clone_preserves_all_fields() {
    let token = IdempotencyToken::new(12345);
    let entry = MemtableEntry::with_sequence(
        make_key(b"rec", b"item"),
        Bytes::from("value"),
        Bytes::from("metadata"),
        token,
        99,
        EntryType::Put,
    );

    let cloned = entry.clone();

    assert_eq!(cloned.composite_key, entry.composite_key);
    assert_eq!(cloned.value, entry.value);
    assert_eq!(cloned.metadata, entry.metadata);
    assert_eq!(cloned.idempotency_key, entry.idempotency_key);
    assert_eq!(cloned.sequence_number, entry.sequence_number);
    assert_eq!(cloned.entry_type, entry.entry_type);
}

#[test]
fn test_equality() {
    let token = IdempotencyToken::none();
    let a = MemtableEntry::with_sequence(
        make_key(b"rec", b"item"),
        Bytes::from("value"),
        Bytes::from("meta"),
        token,
        7,
        EntryType::Delete,
    );
    let b = MemtableEntry::with_sequence(
        make_key(b"rec", b"item"),
        Bytes::from("value"),
        Bytes::from("meta"),
        token,
        7,
        EntryType::Delete,
    );
    assert_eq!(a, b);
}

#[test]
fn test_inequality_different_composite_key() {
    let a = make_put_entry(b"rec-a", b"item", b"val");
    let b = make_put_entry(b"rec-b", b"item", b"val");
    assert_ne!(a, b);
}

#[test]
fn test_inequality_different_entry_type() {
    let put = make_put_entry(b"rec", b"item", b"val");
    let delete = make_delete_entry(b"rec", b"item");
    assert_ne!(put, delete);
}

#[test]
fn test_inequality_different_sequence_number() {
    let a = MemtableEntry::with_sequence(
        make_key(b"rec", b"item"),
        Bytes::from("v"),
        Bytes::new(),
        IdempotencyToken::none(),
        1,
        EntryType::Put,
    );
    let b = MemtableEntry::with_sequence(
        make_key(b"rec", b"item"),
        Bytes::from("v"),
        Bytes::new(),
        IdempotencyToken::none(),
        2,
        EntryType::Put,
    );
    assert_ne!(a, b);
}

#[test]
fn test_entry_preserves_non_none_idempotency_token() {
    let token = IdempotencyToken::new(42_000);
    let entry = MemtableEntry::new(
        make_key(b"rec", b"item"),
        Bytes::from("val"),
        Bytes::new(),
        token,
        EntryType::Put,
    );
    assert!(!entry.idempotency_key.is_none());
    assert_eq!(entry.idempotency_key.generation_time(), 42_000);
}

#[test]
fn test_metadata_field_preserved() {
    let entry = MemtableEntry::new(
        make_key(b"rec", b"item"),
        Bytes::from("val"),
        Bytes::from("content-type:json"),
        IdempotencyToken::none(),
        EntryType::Put,
    );
    assert_eq!(entry.metadata, Bytes::from("content-type:json"));
}
