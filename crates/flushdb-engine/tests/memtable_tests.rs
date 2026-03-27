use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::Duration;

use bytes::Bytes;
use flushdb_engine::memtable::{Memtable, MemtableConfig};
use flushdb_types::{CompositeKey, EntryType, IdempotencyToken, MemtableEntry};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

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

fn make_range_delete(record: &str, start_key: &str, end_key: &str) -> MemtableEntry {
    MemtableEntry::new(
        CompositeKey::range_tombstone_key(record.as_bytes(), start_key.as_bytes()).unwrap(),
        Bytes::from(end_key.to_string()),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::RangeDelete,
    )
}

fn new_memtable() -> Memtable {
    Memtable::new(MemtableConfig::default(), 1)
}

// === Insert Tests ===

#[test]
fn test_insert_assigns_sequence_number() {
    let mut mt = new_memtable();
    let seq1 = mt.insert(make_put("r1", "k1", "v1")).unwrap();
    let seq2 = mt.insert(make_put("r1", "k2", "v2")).unwrap();
    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);
}

#[test]
fn test_insert_put_entry() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "hello")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = mt.get(&key).expect("should find inserted entry");
    assert_eq!(entry.value, Bytes::from("hello"));
    assert_eq!(entry.entry_type, EntryType::Put);
}

#[test]
fn test_insert_delete_entry() {
    let mut mt = new_memtable();
    mt.insert(make_delete("r1", "k1")).unwrap();

    let mut found = false;
    for node in mt.iter() {
        if node.key == CompositeKey::new(b"r1", b"k1").unwrap() {
            assert_eq!(node.entry_type, EntryType::Delete);
            found = true;
        }
    }
    assert!(found, "delete entry should be in skiplist via iter");
}

#[test]
fn test_insert_range_delete_entry() {
    let mut mt = new_memtable();
    mt.insert(make_range_delete("r1", "a", "z")).unwrap();

    assert_eq!(mt.range_tombstone_count(), 1);
    assert_eq!(mt.entry_count(), 1);
}

#[test]
fn test_insert_frozen_memtable_returns_error() {
    let mut mt = new_memtable();
    mt.freeze();

    let result = mt.insert(make_put("r1", "k1", "v1"));
    assert!(result.is_err());
    match result.unwrap_err() {
        flushdb_types::FlushError::ResourceExhausted { resource, message } => {
            assert_eq!(resource, "memtable");
            assert_eq!(message, "memtable is frozen");
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

// === Point Lookup Tests ===

#[test]
fn test_get_existing_key() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "value1")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = mt.get(&key).expect("should find entry");
    assert_eq!(entry.value, Bytes::from("value1"));
    assert_eq!(entry.sequence_number, 1);
}

#[test]
fn test_get_nonexistent_key() {
    let mt = new_memtable();
    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    assert!(mt.get(&key).is_none());
}

#[test]
fn test_get_returns_latest_version() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "old")).unwrap();
    mt.insert(make_put("r1", "k1", "new")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = mt.get(&key).expect("should find entry");
    assert_eq!(entry.value, Bytes::from("new"));
    assert_eq!(entry.sequence_number, 2);
}

#[test]
fn test_get_deleted_key_returns_tombstone() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    mt.insert(make_delete("r1", "k1")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = mt.get(&key).expect("should find delete tombstone");
    assert_eq!(entry.entry_type, EntryType::Delete);
    assert_eq!(entry.sequence_number, 2);
}

#[test]
fn test_get_key_covered_by_range_tombstone() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "b", "v1")).unwrap();
    mt.insert(make_range_delete("r1", "a", "z")).unwrap();

    let key = CompositeKey::new(b"r1", b"b").unwrap();
    assert!(
        mt.get(&key).is_none(),
        "should be covered by range tombstone"
    );
}

#[test]
fn test_get_key_not_covered_by_range_tombstone_lower_seq() {
    let mut mt = new_memtable();
    mt.insert(make_range_delete("r1", "a", "z")).unwrap();
    mt.insert(make_put("r1", "b", "v1")).unwrap();

    let key = CompositeKey::new(b"r1", b"b").unwrap();
    let entry = mt
        .get(&key)
        .expect("range tombstone has lower seq, should not cover");
    assert_eq!(entry.value, Bytes::from("v1"));
}

// === Range Scan Tests ===

#[test]
fn test_scan_returns_entries_in_range() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();
    mt.insert(make_put("r1", "b", "v2")).unwrap();
    mt.insert(make_put("r1", "c", "v3")).unwrap();
    mt.insert(make_put("r1", "d", "v4")).unwrap();

    let start = CompositeKey::new(b"r1", b"b").unwrap();
    let end = CompositeKey::new(b"r1", b"d").unwrap();
    let results = mt.scan(&start, &end);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, Bytes::from("v2"));
    assert_eq!(results[1].value, Bytes::from("v3"));
}

#[test]
fn test_scan_deduplicates_by_key() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "old")).unwrap();
    mt.insert(make_put("r1", "a", "new")).unwrap();
    mt.insert(make_put("r1", "b", "v2")).unwrap();

    let start = CompositeKey::new(b"r1", b"a").unwrap();
    let end = CompositeKey::new(b"r1", b"c").unwrap();
    let results = mt.scan(&start, &end);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, Bytes::from("new"));
    assert_eq!(results[1].value, Bytes::from("v2"));
}

#[test]
fn test_scan_filters_point_tombstones() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();
    mt.insert(make_put("r1", "b", "v2")).unwrap();
    mt.insert(make_delete("r1", "b")).unwrap();

    let start = CompositeKey::new(b"r1", b"a").unwrap();
    let end = CompositeKey::new(b"r1", b"c").unwrap();
    let results = mt.scan(&start, &end);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].value, Bytes::from("v1"));
}

#[test]
fn test_scan_filters_range_tombstone_covered() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();
    mt.insert(make_put("r1", "b", "v2")).unwrap();
    mt.insert(make_put("r1", "c", "v3")).unwrap();
    mt.insert(make_range_delete("r1", "a", "c")).unwrap();

    let start = CompositeKey::new(b"r1", b"a").unwrap();
    let end = CompositeKey::new(b"r1", b"d").unwrap();
    let results = mt.scan(&start, &end);

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].value, Bytes::from("v3"));
}

#[test]
fn test_scan_empty_range() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();

    let start = CompositeKey::new(b"r1", b"z").unwrap();
    let end = CompositeKey::new(b"r1", b"a").unwrap();
    let results = mt.scan(&start, &end);

    assert!(results.is_empty());
}

// === Record Scan Tests ===

#[test]
fn test_scan_record_returns_all_items() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();
    mt.insert(make_put("r1", "b", "v2")).unwrap();
    mt.insert(make_put("r1", "c", "v3")).unwrap();

    let results = mt.scan_record(b"r1");
    assert_eq!(results.len(), 3);
}

#[test]
fn test_scan_record_excludes_other_records() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();
    mt.insert(make_put("r2", "a", "v2")).unwrap();
    mt.insert(make_put("r1", "b", "v3")).unwrap();

    let results = mt.scan_record(b"r1");
    assert_eq!(results.len(), 2);
    for entry in &results {
        assert_eq!(entry.record_id(), b"r1");
    }
}

#[test]
fn test_scan_record_filters_tombstones() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();
    mt.insert(make_put("r1", "b", "v2")).unwrap();
    mt.insert(make_delete("r1", "a")).unwrap();
    mt.insert(make_range_delete("r1", "b", "c")).unwrap();

    let results = mt.scan_record(b"r1");
    assert!(results.is_empty());
}

#[test]
fn test_scan_record_nonexistent() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "a", "v1")).unwrap();

    let results = mt.scan_record(b"r999");
    assert!(results.is_empty());
}

// === Sequence Number Tests ===

#[test]
fn test_sequence_monotonically_increasing() {
    let mut mt = new_memtable();
    let s1 = mt.insert(make_put("r1", "k1", "v1")).unwrap();
    let s2 = mt.insert(make_put("r1", "k2", "v2")).unwrap();
    let s3 = mt.insert(make_put("r1", "k3", "v3")).unwrap();
    assert_eq!(s2, s1 + 1);
    assert_eq!(s3, s2 + 1);
}

#[test]
fn test_starting_sequence_from_constructor() {
    let mut mt = Memtable::new(MemtableConfig::default(), 100);
    let seq = mt.insert(make_put("r1", "k1", "v1")).unwrap();
    assert_eq!(seq, 100);
}

#[test]
fn test_next_sequence_number_query() {
    let mut mt = Memtable::new(MemtableConfig::default(), 42);
    assert_eq!(mt.next_sequence_number(), 42);
    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    assert_eq!(mt.next_sequence_number(), 43);
}

// === Freeze & State Tests ===

#[test]
fn test_freeze_prevents_inserts() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    mt.freeze();

    let result = mt.insert(make_put("r1", "k2", "v2"));
    assert!(result.is_err());
}

#[test]
fn test_freeze_allows_reads() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    mt.insert(make_put("r1", "k2", "v2")).unwrap();
    mt.freeze();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    assert!(mt.get(&key).is_some());

    let start = CompositeKey::new(b"r1", b"k1").unwrap();
    let end = CompositeKey::new(b"r1", b"k3").unwrap();
    let results = mt.scan(&start, &end);
    assert_eq!(results.len(), 2);

    let record_results = mt.scan_record(b"r1");
    assert_eq!(record_results.len(), 2);
}

#[test]
fn test_should_freeze_by_size() {
    let config = MemtableConfig {
        size_threshold: 100,
        max_frozen_count: 3,
        ..Default::default()
    };
    let mut mt = Memtable::new(config, 1);

    assert!(!mt.should_freeze_by_size());

    for i in 0..20 {
        mt.insert(make_put(
            "r1",
            &format!("key{i:04}"),
            "some_value_that_takes_space",
        ))
        .unwrap();
    }

    assert!(mt.should_freeze_by_size());
}

#[test]
fn test_should_freeze_by_age() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "v1")).unwrap();

    assert!(!mt.should_freeze_by_age(Duration::from_secs(60)));
    assert!(mt.should_freeze_by_age(Duration::from_nanos(1)));
}

#[test]
fn test_is_empty() {
    let mut mt = new_memtable();
    assert!(mt.is_empty());

    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    assert!(!mt.is_empty());
}

#[test]
fn test_entry_count() {
    let mut mt = new_memtable();
    assert_eq!(mt.entry_count(), 0);

    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    assert_eq!(mt.entry_count(), 1);

    mt.insert(make_put("r1", "k2", "v2")).unwrap();
    assert_eq!(mt.entry_count(), 2);

    mt.insert(make_delete("r1", "k3")).unwrap();
    assert_eq!(mt.entry_count(), 3);
}

#[test]
fn test_is_frozen_before_and_after() {
    let mut mt = new_memtable();
    assert!(!mt.is_frozen());

    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    assert!(!mt.is_frozen());

    mt.freeze();
    assert!(mt.is_frozen());
}

#[test]
fn test_scan_does_not_filter_entries_with_higher_seq_than_range_tombstone() {
    let mut mt = new_memtable();
    // Range tombstone inserted first (lower seq = 1)
    mt.insert(make_range_delete("r1", "a", "z")).unwrap();
    // Puts inserted after (higher seq = 2, 3) should survive
    mt.insert(make_put("r1", "b", "v_b")).unwrap();
    mt.insert(make_put("r1", "c", "v_c")).unwrap();

    let start = CompositeKey::new(b"r1", b"a").unwrap();
    let end = CompositeKey::new(b"r1", b"z").unwrap();
    let results = mt.scan(&start, &end);

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, Bytes::from("v_b"));
    assert_eq!(results[1].value, Bytes::from("v_c"));
}

#[test]
fn test_scan_record_does_not_filter_entries_with_higher_seq_than_range_tombstone() {
    let mut mt = new_memtable();
    mt.insert(make_range_delete("r1", "a", "z")).unwrap();
    mt.insert(make_put("r1", "b", "v_b")).unwrap();
    mt.insert(make_put("r1", "c", "v_c")).unwrap();

    let results = mt.scan_record(b"r1");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].value, Bytes::from("v_b"));
    assert_eq!(results[1].value, Bytes::from("v_c"));
}

// === into_skiplist Tests ===

#[test]
fn test_into_skiplist_returns_skiplist_with_expected_entries() {
    let mut mt = new_memtable();
    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    mt.insert(make_put("r1", "k2", "v2")).unwrap();
    mt.insert(make_put("r2", "k1", "v3")).unwrap();
    mt.freeze();

    let sl = mt.into_skiplist();
    assert_eq!(sl.len(), 3);

    let key1 = CompositeKey::new(b"r1", b"k1").unwrap();
    let node1 = sl.get(&key1).expect("should find r1/k1");
    assert_eq!(node1.value, Bytes::from("v1"));

    let key2 = CompositeKey::new(b"r1", b"k2").unwrap();
    let node2 = sl.get(&key2).expect("should find r1/k2");
    assert_eq!(node2.value, Bytes::from("v2"));

    let key3 = CompositeKey::new(b"r2", b"k1").unwrap();
    let node3 = sl.get(&key3).expect("should find r2/k1");
    assert_eq!(node3.value, Bytes::from("v3"));
}

// === range_tombstones Accessor Tests ===

#[test]
fn test_range_tombstones_accessor_returns_inserted_tombstones() {
    let mut mt = new_memtable();
    mt.insert(make_range_delete("r1", "a", "m")).unwrap();

    let index = mt.range_tombstones();
    assert_eq!(index.len(), 1);

    let tombstones: Vec<_> = index.iter().collect();
    assert_eq!(tombstones[0].record_id.as_ref(), b"r1");
    assert_eq!(tombstones[0].start_key.as_ref(), b"a");
    assert_eq!(tombstones[0].end_key.as_ref(), b"m");
    assert_eq!(tombstones[0].sequence_number, 1);

    assert!(index.covers(b"r1", b"b", 0));
    assert!(!index.covers(b"r1", b"z", 0));
}

// === Oracle Test ===

#[test]
fn test_100k_entries_match_btreemap_oracle() {
    let mut rng = SmallRng::seed_from_u64(12345);
    let mut mt = Memtable::new(MemtableConfig::default(), 1);

    // Oracle: BTreeMap keyed by (composite_key_bytes, Reverse(seq))
    // Value is the MemtableEntry
    let mut oracle: BTreeMap<(Vec<u8>, Reverse<u64>), MemtableEntry> = BTreeMap::new();

    // Track which record_ids we used
    let mut record_ids: Vec<String> = Vec::new();
    for i in 0..100u32 {
        record_ids.push(format!("r{i:04}"));
    }

    for _ in 0..100_000 {
        let record = &record_ids[rng.random_range(0..record_ids.len())];
        let item_key = format!("k{:06}", rng.random_range(0..10000u32));
        let value = format!("v{}", rng.random_range(0..1_000_000u32));

        let entry = make_put(record, &item_key, &value);
        let seq = mt.insert(entry.clone()).unwrap();

        let ck_bytes = entry.composite_key.as_bytes().to_vec();
        let oracle_entry = MemtableEntry::with_sequence(
            entry.composite_key,
            entry.value,
            entry.metadata,
            entry.idempotency_key,
            seq,
            entry.entry_type,
        );
        oracle.insert((ck_bytes, Reverse(seq)), oracle_entry);
    }

    // Verify scan_record matches oracle for a sample of records
    for record in record_ids.iter().take(20) {
        let mt_results = mt.scan_record(record.as_bytes());

        // Build expected from oracle: group by key, take highest seq, filter non-delete
        let prefix = {
            let ck = CompositeKey::new(record.as_bytes(), b"").unwrap();
            ck.as_bytes().to_vec()
        };

        let mut expected: Vec<MemtableEntry> = Vec::new();
        let mut seen_keys: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

        for ((ck_bytes, _), entry) in oracle.range((prefix.clone(), Reverse(u64::MAX))..) {
            if !ck_bytes.starts_with(&prefix) || ck_bytes[prefix.len() - 1] != 0x00 {
                // Check that the record_id portion matches
                if entry.record_id() != record.as_bytes() {
                    break;
                }
            }
            if entry.record_id() != record.as_bytes() {
                break;
            }
            if seen_keys.contains(ck_bytes) {
                continue;
            }
            seen_keys.insert(ck_bytes.clone());

            if entry.entry_type != EntryType::Delete {
                expected.push(entry.clone());
            }
        }

        assert_eq!(
            mt_results.len(),
            expected.len(),
            "scan_record mismatch for record {record}: mt={} oracle={}",
            mt_results.len(),
            expected.len()
        );

        for (mt_entry, oracle_entry) in mt_results.iter().zip(expected.iter()) {
            assert_eq!(mt_entry.composite_key, oracle_entry.composite_key);
            assert_eq!(mt_entry.value, oracle_entry.value);
        }
    }

    // Verify random point lookups
    for _ in 0..1000 {
        let record = &record_ids[rng.random_range(0..record_ids.len())];
        let item_key = format!("k{:06}", rng.random_range(0..10000u32));
        let ck = CompositeKey::new(record.as_bytes(), item_key.as_bytes()).unwrap();

        let mt_result = mt.get(&ck);

        // Oracle: find highest seq entry for this key
        let ck_bytes = ck.as_bytes().to_vec();
        let oracle_result = oracle
            .range((ck_bytes.clone(), Reverse(u64::MAX))..)
            .next()
            .and_then(
                |((k, _), entry)| {
                    if k == &ck_bytes {
                        Some(entry)
                    } else {
                        None
                    }
                },
            );

        match (mt_result, oracle_result) {
            (Some(mt_entry), Some(oracle_entry)) => {
                assert_eq!(mt_entry.composite_key, oracle_entry.composite_key);
                assert_eq!(mt_entry.value, oracle_entry.value);
            }
            (None, None) => {}
            (Some(mt_entry), None) => {
                panic!(
                    "memtable returned entry but oracle did not for {record}/{item_key}: {:?}",
                    mt_entry.value
                );
            }
            (None, Some(oracle_entry)) => {
                // Oracle might have a delete tombstone that memtable.get returns
                // but in our case all entries are Put, so this would be a real mismatch
                if oracle_entry.entry_type == EntryType::Put {
                    panic!(
                        "oracle had entry but memtable returned None for {record}/{item_key}: {:?}",
                        oracle_entry.value
                    );
                }
            }
        }
    }
}
