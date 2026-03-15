use bytes::Bytes;
use flushdb_engine::memtable::MemtableConfig;
use flushdb_engine::{Memtable, MemtableList};
use flushdb_types::{CompositeKey, EntryType, FlushError, IdempotencyToken, MemtableEntry};

fn make_put(record: &str, key: &str, value: &str) -> MemtableEntry {
    MemtableEntry::new(
        CompositeKey::new(record.as_bytes(), key.as_bytes()).unwrap(),
        Bytes::from(value.to_string()),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Put,
    )
}

fn make_delete(record: &str, key: &str) -> MemtableEntry {
    MemtableEntry::new(
        CompositeKey::new(record.as_bytes(), key.as_bytes()).unwrap(),
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Delete,
    )
}

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

fn new_list() -> MemtableList {
    MemtableList::new(MemtableConfig::default(), 1)
}

fn small_list() -> MemtableList {
    MemtableList::new(
        MemtableConfig {
            size_threshold: 67_108_864,
            max_frozen_count: 2,
        },
        1,
    )
}

// === Freeze & Swap Tests ===

#[test]
fn test_freeze_creates_new_active() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    assert_eq!(list.active_entry_count(), 1);

    list.freeze_active().unwrap();

    assert_eq!(list.active_entry_count(), 0);
    assert_eq!(list.frozen_count(), 1);
}

#[test]
fn test_freeze_preserves_frozen_data() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = list.get(&key).expect("should find in frozen");
    assert_eq!(entry.value, Bytes::from("v1"));
}

#[test]
fn test_freeze_sequence_continuity() {
    let mut list = new_list();
    let seq1 = list.insert(make_put("r1", "k1", "v1")).unwrap();
    assert_eq!(seq1, 1);

    list.freeze_active().unwrap();

    let seq2 = list.insert(make_put("r1", "k2", "v2")).unwrap();
    assert_eq!(seq2, 2);
}

#[test]
fn test_freeze_backpressure() {
    let mut list = small_list();

    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();

    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();

    assert_eq!(list.frozen_count(), 2);

    list.insert(make_put("r1", "k3", "v3")).unwrap();
    let result = list.freeze_active();
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::ResourceExhausted { resource, message } => {
            assert_eq!(resource, "memtable");
            assert!(
                message.contains("2/2"),
                "message should contain frozen count info, got: {message}"
            );
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

#[test]
fn test_freeze_empty_memtable() {
    let mut list = new_list();
    list.freeze_active().unwrap();

    assert_eq!(list.frozen_count(), 1);
    assert_eq!(list.total_entry_count(), 0);
}

// === Cross-Memtable Read Tests ===

#[test]
fn test_get_checks_active_first() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "frozen_val")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k1", "active_val")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = list.get(&key).expect("should find entry");
    assert_eq!(entry.value, Bytes::from("active_val"));
}

#[test]
fn test_get_falls_through_to_frozen() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "frozen_val")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k2", "active_val")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = list.get(&key).expect("should find in frozen");
    assert_eq!(entry.value, Bytes::from("frozen_val"));
}

#[test]
fn test_get_checks_frozen_newest_first() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "older_frozen")).unwrap();
    list.freeze_active().unwrap();

    list.insert(make_put("r1", "k1", "newer_frozen")).unwrap();
    list.freeze_active().unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = list.get(&key).expect("should find in newest frozen");
    assert_eq!(entry.value, Bytes::from("newer_frozen"));
}

#[test]
fn test_get_not_found_anywhere() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();

    let key = CompositeKey::new(b"r1", b"missing").unwrap();
    assert!(list.get(&key).is_none());
}

#[test]
fn test_scan_merges_active_and_frozen() {
    let mut list = new_list();
    list.insert(make_put("r1", "a", "v_a")).unwrap();
    list.insert(make_put("r1", "c", "v_c")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "b", "v_b")).unwrap();
    list.insert(make_put("r1", "d", "v_d")).unwrap();

    let start = CompositeKey::new(b"r1", b"a").unwrap();
    let end = CompositeKey::new(b"r1", b"e").unwrap();
    let results = list.scan(&start, &end);

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].value, Bytes::from("v_a"));
    assert_eq!(results[1].value, Bytes::from("v_b"));
    assert_eq!(results[2].value, Bytes::from("v_c"));
    assert_eq!(results[3].value, Bytes::from("v_d"));
}

#[test]
fn test_scan_deduplicates_across_memtables() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "old_val")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k1", "new_val")).unwrap();

    let start = CompositeKey::new(b"r1", b"k1").unwrap();
    let end = CompositeKey::new(b"r1", b"k2").unwrap();
    let results = list.scan(&start, &end);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].value, Bytes::from("new_val"));
}

#[test]
fn test_scan_record_across_memtables() {
    let mut list = new_list();
    list.insert(make_put("r1", "a", "v1")).unwrap();
    list.insert(make_put("r2", "x", "other")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "b", "v2")).unwrap();

    let results = list.scan_record(b"r1");
    assert_eq!(results.len(), 2);
    for entry in &results {
        assert_eq!(entry.record_id(), b"r1");
    }
}

// === Cross-Memtable Dedup Tests ===

#[test]
fn test_dedup_rejects_token_in_active() {
    let mut list = new_list();
    let token = make_token(1);

    list.insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();
    let result = list.insert(make_put_with_token("r1", "k2", "v2", token));

    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::DuplicateToken { .. } => {}
        other => panic!("expected DuplicateToken, got: {other:?}"),
    }
}

#[test]
fn test_dedup_rejects_token_in_frozen() {
    let mut list = new_list();
    let token = make_token(1);

    list.insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();
    list.freeze_active().unwrap();

    let result = list.insert(make_put_with_token("r1", "k2", "v2", token));
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::DuplicateToken { .. } => {}
        other => panic!("expected DuplicateToken, got: {other:?}"),
    }
}

#[test]
fn test_dedup_all_zero_bypasses_across_memtables() {
    let mut list = new_list();
    let zero = IdempotencyToken::none();

    list.insert(make_put_with_token("r1", "k1", "v1", zero))
        .unwrap();
    list.freeze_active().unwrap();

    list.insert(make_put_with_token("r1", "k2", "v2", zero))
        .unwrap();
    list.insert(make_put_with_token("r1", "k3", "v3", zero))
        .unwrap();

    assert_eq!(list.total_entry_count(), 3);
}

// === Pop Frozen Tests ===

#[test]
fn test_pop_oldest_frozen() {
    let mut list = new_list();

    list.insert(make_put("r1", "k1", "first")).unwrap();
    list.freeze_active().unwrap();

    list.insert(make_put("r1", "k2", "second")).unwrap();
    list.freeze_active().unwrap();

    let oldest = list.pop_oldest_frozen().expect("should have frozen");
    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    assert!(oldest.get(&key).is_some(), "oldest should contain first entry");

    let next = list.pop_oldest_frozen().expect("should have another frozen");
    let key2 = CompositeKey::new(b"r1", b"k2").unwrap();
    assert!(next.get(&key2).is_some(), "next should contain second entry");
}

#[test]
fn test_pop_oldest_frozen_empty() {
    let mut list = new_list();
    assert!(list.pop_oldest_frozen().is_none());
}

#[test]
fn test_pop_reduces_frozen_count() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();

    assert_eq!(list.frozen_count(), 2);
    list.pop_oldest_frozen();
    assert_eq!(list.frozen_count(), 1);
    list.pop_oldest_frozen();
    assert_eq!(list.frozen_count(), 0);
}

#[test]
fn test_pop_relieves_backpressure() {
    let mut list = small_list();

    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();

    assert!(list.is_backpressured());

    list.pop_oldest_frozen();
    assert!(!list.is_backpressured());

    list.insert(make_put("r1", "k3", "v3")).unwrap();
    list.freeze_active().unwrap();
    assert!(list.is_backpressured());
}

// === Backpressure Tests ===

#[test]
fn test_is_backpressured() {
    let mut list = small_list();

    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();
    assert!(!list.is_backpressured());

    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();
    assert!(list.is_backpressured());
}

#[test]
fn test_not_backpressured_initially() {
    let list = new_list();
    assert!(!list.is_backpressured());
}

#[test]
fn test_backpressure_error_has_context() {
    let mut list = small_list();

    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();

    list.insert(make_put("r1", "k3", "v3")).unwrap();
    let err = list.freeze_active().unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("2/2"),
        "error should contain frozen count info, got: {msg}"
    );
}

// === State Query Tests ===

#[test]
fn test_active_memory_usage() {
    let mut list = new_list();
    let before = list.active_memory_usage();
    list.insert(make_put("r1", "k1", "some_value")).unwrap();
    assert!(list.active_memory_usage() > before);
}

#[test]
fn test_total_memory_usage() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    let active_usage = list.active_memory_usage();

    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();

    let total = list.total_memory_usage();
    assert!(
        total >= active_usage,
        "total should include frozen + active memory"
    );
    assert!(total > list.active_memory_usage());
}

#[test]
fn test_total_entry_count() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k3", "v3")).unwrap();

    assert_eq!(list.active_entry_count(), 1);
    assert_eq!(list.total_entry_count(), 3);
}

// === Next Sequence Number Tests ===

#[test]
fn test_next_sequence_number_across_freeze() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    assert_eq!(list.next_sequence_number(), 3);

    list.freeze_active().unwrap();
    assert_eq!(list.next_sequence_number(), 3);

    let seq = list.insert(make_put("r1", "k3", "v3")).unwrap();
    assert_eq!(seq, 3);
    assert_eq!(list.next_sequence_number(), 4);
}

// === Has Frozen Tests ===

#[test]
fn test_has_frozen_initially_false() {
    let list = new_list();
    assert!(!list.has_frozen());
}

#[test]
fn test_has_frozen_after_freeze() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();
    assert!(list.has_frozen());
}

#[test]
fn test_has_frozen_after_pop_all() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.freeze_active().unwrap();
    list.pop_oldest_frozen();
    assert!(!list.has_frozen());
}

// === Should Freeze Tests ===

#[test]
fn test_should_freeze_by_size() {
    let mut list = MemtableList::new(
        MemtableConfig {
            size_threshold: 100,
            max_frozen_count: 3,
        },
        1,
    );
    assert!(!list.should_freeze(std::time::Duration::from_secs(3600)));

    for i in 0..20 {
        list.insert(make_put("r1", &format!("key{i:04}"), "some_value_that_takes_space"))
            .unwrap();
    }
    assert!(list.should_freeze(std::time::Duration::from_secs(3600)));
}

#[test]
fn test_should_freeze_by_age() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    assert!(list.should_freeze(std::time::Duration::from_nanos(1)));
}

// === Delete Handling in Scan ===

#[test]
fn test_scan_record_dedup_across_memtables() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "old")).unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();
    list.insert(make_put("r1", "k1", "new")).unwrap();

    let results = list.scan_record(b"r1");
    assert_eq!(results.len(), 2);

    let k1_entry = results
        .iter()
        .find(|e| e.item_key() == b"k1")
        .expect("k1 should be in results");
    assert_eq!(k1_entry.value, Bytes::from("new"));
}

// === Send Bound Tests ===

#[test]
fn test_memtable_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Memtable>();
}

#[test]
fn test_memtable_list_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<MemtableList>();
}

// === Multiple Freeze Cycles ===

#[test]
fn test_multiple_freeze_cycles() {
    let mut list = new_list();

    for cycle in 0..3u32 {
        for i in 0..5u32 {
            let key = format!("c{cycle}_k{i}");
            let val = format!("c{cycle}_v{i}");
            list.insert(make_put("r1", &key, &val)).unwrap();
        }
        list.freeze_active().unwrap();
    }

    assert_eq!(list.frozen_count(), 3);
    assert_eq!(list.total_entry_count(), 15);

    for cycle in 0..3u32 {
        for i in 0..5u32 {
            let key = CompositeKey::new(b"r1", format!("c{cycle}_k{i}").as_bytes()).unwrap();
            assert!(list.get(&key).is_some(), "missing c{cycle}_k{i}");
        }
    }
}

// === Insert After Pop ===

#[test]
fn test_popped_frozen_not_in_reads() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "frozen_val")).unwrap();
    list.freeze_active().unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    assert!(list.get(&key).is_some());

    let _popped = list.pop_oldest_frozen();
    assert!(list.get(&key).is_none());
}

// === Scan Empty ===

#[test]
fn test_scan_empty_list() {
    let list = new_list();
    let start = CompositeKey::new(b"r1", b"a").unwrap();
    let end = CompositeKey::new(b"r1", b"z").unwrap();
    let results = list.scan(&start, &end);
    assert!(results.is_empty());
}

#[test]
fn test_scan_record_empty_list() {
    let list = new_list();
    let results = list.scan_record(b"r1");
    assert!(results.is_empty());
}

// === Dedup Across Multiple Frozen ===

#[test]
fn test_dedup_across_multiple_frozen() {
    let mut list = new_list();
    let token = make_token(99);

    list.insert(make_put_with_token("r1", "k1", "v1", token))
        .unwrap();
    list.freeze_active().unwrap();

    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();

    let result = list.insert(make_put_with_token("r1", "k3", "v3", token));
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::DuplicateToken { .. } => {}
        other => panic!("expected DuplicateToken, got: {other:?}"),
    }
}

// === Delete in Active, Put in Frozen (scan behavior) ===

#[test]
fn test_delete_in_active_shadows_put_in_frozen_scan() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();

    list.insert(make_delete("r1", "k1")).unwrap();

    let start = CompositeKey::new(b"r1", b"k1").unwrap();
    let end = CompositeKey::new(b"r1", b"k3").unwrap();
    let results = list.scan(&start, &end);

    // DELETE in active (higher seq) must shadow the PUT in frozen (lower seq)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].value, Bytes::from("v2"));
    assert!(!results.iter().any(|e| e.value == Bytes::from("v1")));
}

#[test]
fn test_delete_in_active_shadows_put_in_frozen_scan_record() {
    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    list.freeze_active().unwrap();

    list.insert(make_delete("r1", "k1")).unwrap();

    let results = list.scan_record(b"r1");

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].value, Bytes::from("v2"));
}
