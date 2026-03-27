use bytes::Bytes;
use flushdb_engine::memtable::{DedupSet, Memtable, MemtableConfig};
use flushdb_types::{CompositeKey, EntryType, FlushError, IdempotencyToken, MemtableEntry};

fn make_token(id: u64) -> IdempotencyToken {
    IdempotencyToken::from_parts(1000, {
        let mut buf = [0u8; 16];
        buf[..8].copy_from_slice(&id.to_be_bytes());
        buf
    })
}

fn make_put_with_token(
    record: &str,
    key: &str,
    value: &str,
    token: IdempotencyToken,
) -> MemtableEntry {
    MemtableEntry::new(
        CompositeKey::new(record.as_bytes(), key.as_bytes()).unwrap(),
        Bytes::from(value.to_string()),
        Bytes::new(),
        token,
        EntryType::Put,
    )
}

fn new_memtable() -> Memtable {
    Memtable::new(MemtableConfig::default(), 1)
}

// === DedupSet Unit Tests ===

#[test]
fn test_dedup_set_insert_and_contains() {
    let mut ds = DedupSet::new();
    let token = make_token(1);
    ds.insert(token);
    assert!(ds.contains(&token));
}

#[test]
fn test_dedup_set_missing_token() {
    let ds = DedupSet::new();
    let token = make_token(42);
    assert!(!ds.contains(&token));
}

#[test]
fn test_dedup_set_all_zero_token_bypasses() {
    let mut ds = DedupSet::new();
    let zero = IdempotencyToken::none();

    assert!(!ds.contains(&zero));

    ds.insert(zero);

    assert!(!ds.contains(&zero));
    assert_eq!(ds.len(), 0);
}

#[test]
fn test_dedup_set_len() {
    let mut ds = DedupSet::new();
    assert_eq!(ds.len(), 0);

    ds.insert(make_token(1));
    assert_eq!(ds.len(), 1);

    ds.insert(make_token(2));
    assert_eq!(ds.len(), 2);

    ds.insert(make_token(3));
    assert_eq!(ds.len(), 3);
}

#[test]
fn test_dedup_set_duplicate_insert_idempotent() {
    let mut ds = DedupSet::new();
    let token = make_token(1);

    ds.insert(token);
    assert_eq!(ds.len(), 1);

    ds.insert(token);
    assert_eq!(ds.len(), 1);
}

// === Memtable Dedup Integration Tests ===

#[test]
fn test_insert_rejects_duplicate_token() {
    let mut mt = new_memtable();
    let token = make_token(1);

    mt.insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();

    let result = mt.insert(make_put_with_token("r1", "k2", "v2", token));
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::DuplicateToken { .. } => {}
        other => panic!("expected DuplicateToken, got: {other:?}"),
    }
}

#[test]
fn test_insert_different_tokens_both_succeed() {
    let mut mt = new_memtable();

    mt.insert(make_put_with_token("r1", "k1", "v1", make_token(1)))
        .unwrap();
    mt.insert(make_put_with_token("r1", "k2", "v2", make_token(2)))
        .unwrap();

    assert_eq!(mt.entry_count(), 2);
}

#[test]
fn test_insert_all_zero_token_never_rejected() {
    let mut mt = new_memtable();
    let zero = IdempotencyToken::none();

    mt.insert(make_put_with_token("r1", "k1", "v1", zero))
        .unwrap();
    mt.insert(make_put_with_token("r1", "k2", "v2", zero))
        .unwrap();
    mt.insert(make_put_with_token("r1", "k3", "v3", zero))
        .unwrap();

    assert_eq!(mt.entry_count(), 3);
}

#[test]
fn test_insert_all_zero_then_real_token() {
    let mut mt = new_memtable();

    mt.insert(make_put_with_token(
        "r1",
        "k1",
        "v1",
        IdempotencyToken::none(),
    ))
    .unwrap();
    mt.insert(make_put_with_token("r1", "k2", "v2", make_token(1)))
        .unwrap();

    assert_eq!(mt.entry_count(), 2);
}

#[test]
fn test_dedup_across_different_keys() {
    let mut mt = new_memtable();
    let token = make_token(42);

    mt.insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();

    let result = mt.insert(make_put_with_token("r2", "k2", "v2", token));
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::DuplicateToken { .. } => {}
        other => panic!("expected DuplicateToken, got: {other:?}"),
    }
}

#[test]
fn test_check_dedup_returns_true_for_inserted() {
    let mut mt = new_memtable();
    let token = make_token(1);

    mt.insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();

    assert!(mt.check_dedup(&token));
}

#[test]
fn test_check_dedup_returns_false_for_missing() {
    let mt = new_memtable();
    assert!(!mt.check_dedup(&make_token(99)));
}

#[test]
fn test_check_dedup_all_zero_always_false() {
    let mut mt = new_memtable();
    let zero = IdempotencyToken::none();

    mt.insert(make_put_with_token(
        "r1",
        "k1",
        "v1",
        IdempotencyToken::none(),
    ))
    .unwrap();

    assert!(!mt.check_dedup(&zero));
}

// === Error Tests ===

#[test]
fn test_duplicate_token_error_contains_token_info() {
    let mut mt = new_memtable();
    let token = make_token(1);

    mt.insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();
    let err = mt
        .insert(make_put_with_token("r1", "k2", "v2", token))
        .unwrap_err();

    let msg = format!("{err}");
    assert!(
        msg.contains("duplicate token"),
        "error message should contain 'duplicate token', got: {msg}"
    );
}

#[test]
fn test_duplicate_token_does_not_assign_sequence() {
    let mut mt = new_memtable();
    let token = make_token(1);

    let seq1 = mt
        .insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();
    assert_eq!(seq1, 1);

    let _ = mt.insert(make_put_with_token("r1", "k2", "v2", token));

    assert_eq!(mt.next_sequence_number(), 2);

    let seq3 = mt
        .insert(make_put_with_token("r1", "k3", "v3", make_token(2)))
        .unwrap();
    assert_eq!(seq3, 2);
}

// === Default Impl Tests ===

#[test]
fn test_dedup_set_default_same_as_new() {
    let default_ds = DedupSet::default();
    let new_ds = DedupSet::new();
    assert_eq!(default_ds.len(), new_ds.len());
    assert!(default_ds.is_empty());
    assert!(new_ds.is_empty());
}

// === Scale Tests ===

#[test]
fn test_10k_unique_tokens_all_accepted() {
    let mut mt = new_memtable();

    for i in 0..10_000u64 {
        let token = make_token(i + 1);
        mt.insert(make_put_with_token("r1", &format!("k{i:06}"), "val", token))
            .unwrap();
    }

    assert_eq!(mt.entry_count(), 10_000);
}

#[test]
fn test_replay_same_10k_tokens_all_rejected() {
    let mut mt = new_memtable();

    let tokens: Vec<IdempotencyToken> = (1..=10_000u64).map(make_token).collect();

    for (i, &token) in tokens.iter().enumerate() {
        mt.insert(make_put_with_token("r1", &format!("k{i:06}"), "val", token))
            .unwrap();
    }

    for (i, &token) in tokens.iter().enumerate() {
        let result = mt.insert(make_put_with_token(
            "r1",
            &format!("replay{i:06}"),
            "dup",
            token,
        ));
        assert!(result.is_err(), "token {i} should be rejected on replay");
        match result.unwrap_err() {
            FlushError::DuplicateToken { .. } => {}
            other => panic!("expected DuplicateToken for token {i}, got: {other:?}"),
        }
    }
}
