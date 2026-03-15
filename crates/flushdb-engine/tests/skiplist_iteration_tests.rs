use std::cmp::Reverse;
use std::collections::BTreeMap;

use bytes::Bytes;
use flushdb_engine::skiplist::SkipList;
use flushdb_types::{CompositeKey, EntryType, IdempotencyToken, MemtableEntry};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

fn make_entry(
    record_id: &str,
    item_key: &str,
    value: &str,
    entry_type: EntryType,
) -> MemtableEntry {
    MemtableEntry::new(
        CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap(),
        Bytes::from(value.to_string()),
        Bytes::new(),
        IdempotencyToken::none(),
        entry_type,
    )
}

fn make_entry_with_seq(record_id: &str, item_key: &str, value: &str, seq: u64) -> MemtableEntry {
    MemtableEntry::with_sequence(
        CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap(),
        Bytes::from(value.to_string()),
        Bytes::new(),
        IdempotencyToken::none(),
        seq,
        EntryType::Put,
    )
}

// === Iteration Tests ===

#[test]
fn test_iter_empty_skiplist() {
    let sl = SkipList::new();
    let collected: Vec<_> = sl.iter().collect();
    assert!(collected.is_empty());
}

#[test]
fn test_iter_single_entry() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "key1", "val1", EntryType::Put));

    let collected: Vec<_> = sl.iter().collect();
    assert_eq!(collected.len(), 1);
    assert_eq!(collected[0].value, Bytes::from("val1"));
}

#[test]
fn test_iter_all_entries_in_order() {
    let mut sl = SkipList::new();
    for i in (0..100).rev() {
        let entry = make_entry(
            "rec1",
            &format!("key{:03}", i),
            &format!("val{}", i),
            EntryType::Put,
        );
        sl.insert(entry);
    }

    let keys: Vec<String> = sl
        .iter()
        .map(|n| String::from_utf8_lossy(n.key.item_key()).to_string())
        .collect();

    for i in 0..100 {
        assert_eq!(keys[i], format!("key{:03}", i));
    }
}

#[test]
fn test_iter_matches_btreemap_oracle() {
    let mut sl = SkipList::new();
    let mut oracle: BTreeMap<(Vec<u8>, Reverse<u64>), String> = BTreeMap::new();
    let mut rng = SmallRng::seed_from_u64(77);

    for i in 0u64..10_000 {
        let record_id = format!("r{:03}", rng.random_range(0..20u32));
        let item_key = format!("k{:03}", rng.random_range(0..100u32));
        let value = format!("v{}", i);
        let seq = i;

        let key = CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap();
        oracle.insert((key.as_bytes().to_vec(), Reverse(seq)), value.clone());

        let entry = MemtableEntry::with_sequence(
            key,
            Bytes::from(value),
            Bytes::new(),
            IdempotencyToken::none(),
            seq,
            EntryType::Put,
        );
        sl.insert(entry);
    }

    let sl_entries: Vec<(Vec<u8>, u64)> = sl
        .iter()
        .map(|n| (n.key.as_bytes().to_vec(), n.sequence_number))
        .collect();

    let oracle_entries: Vec<(Vec<u8>, u64)> = oracle
        .keys()
        .map(|(k, Reverse(s))| (k.clone(), *s))
        .collect();

    assert_eq!(sl_entries.len(), oracle_entries.len());
    for (a, b) in sl_entries.iter().zip(oracle_entries.iter()) {
        assert_eq!(a, b);
    }
}

// === Range Scan Tests ===

#[test]
fn test_range_returns_subset() {
    let mut sl = SkipList::new();
    for c in b'a'..=b'z' {
        let buf = [c];
        let key_str = std::str::from_utf8(&buf).unwrap();
        sl.insert(make_entry("rec1", key_str, "val", EntryType::Put));
    }

    let start = CompositeKey::new(b"rec1", b"d").unwrap();
    let end = CompositeKey::new(b"rec1", b"h").unwrap();
    let collected: Vec<u8> = sl
        .range(&start, &end)
        .map(|n| n.key.item_key()[0])
        .collect();

    assert_eq!(collected, vec![b'd', b'e', b'f', b'g']);
}

#[test]
fn test_range_start_equals_end() {
    let mut sl = SkipList::new();
    for c in b'a'..=b'z' {
        let buf = [c];
        let key_str = std::str::from_utf8(&buf).unwrap();
        sl.insert(make_entry("rec1", key_str, "val", EntryType::Put));
    }

    let key = CompositeKey::new(b"rec1", b"m").unwrap();
    let collected: Vec<_> = sl.range(&key, &key).collect();
    assert!(collected.is_empty());
}

#[test]
fn test_range_start_not_present() {
    let mut sl = SkipList::new();
    // Insert keys "b", "d", "f"
    sl.insert(make_entry("rec1", "b", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "d", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "f", "val", EntryType::Put));

    // Range from "c" (not present) to "g"
    let start = CompositeKey::new(b"rec1", b"c").unwrap();
    let end = CompositeKey::new(b"rec1", b"g").unwrap();
    let collected: Vec<String> = sl
        .range(&start, &end)
        .map(|n| String::from_utf8_lossy(n.key.item_key()).to_string())
        .collect();

    assert_eq!(collected, vec!["d", "f"]);
}

#[test]
fn test_range_end_not_present() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "b", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "d", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "f", "val", EntryType::Put));

    // Range from "a" to "e" (not present)
    let start = CompositeKey::new(b"rec1", b"a").unwrap();
    let end = CompositeKey::new(b"rec1", b"e").unwrap();
    let collected: Vec<String> = sl
        .range(&start, &end)
        .map(|n| String::from_utf8_lossy(n.key.item_key()).to_string())
        .collect();

    assert_eq!(collected, vec!["b", "d"]);
}

#[test]
fn test_range_covers_all_entries() {
    let mut sl = SkipList::new();
    for i in 0..50 {
        sl.insert(make_entry(
            "rec1",
            &format!("key{:03}", i),
            "val",
            EntryType::Put,
        ));
    }

    let min = CompositeKey::min_key_for_record(b"rec1").unwrap();
    // Use a key guaranteed to sort after all "rec1" entries
    let max = CompositeKey::new(b"rec2", b"").unwrap();

    let range_count = sl.range(&min, &max).count();
    let iter_count = sl.iter().count();
    assert_eq!(range_count, iter_count);
}

#[test]
fn test_range_within_single_record() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "apple", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "banana", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "cherry", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "date", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "elderberry", "val", EntryType::Put));

    let start = CompositeKey::new(b"rec1", b"banana").unwrap();
    let end = CompositeKey::new(b"rec1", b"elderberry").unwrap();
    let collected: Vec<String> = sl
        .range(&start, &end)
        .map(|n| String::from_utf8_lossy(n.key.item_key()).to_string())
        .collect();

    assert_eq!(collected, vec!["banana", "cherry", "date"]);
}

#[test]
fn test_range_across_records() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "x", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "y", "val", EntryType::Put));
    sl.insert(make_entry("rec2", "a", "val", EntryType::Put));
    sl.insert(make_entry("rec2", "b", "val", EntryType::Put));

    let start = CompositeKey::new(b"rec1", b"y").unwrap();
    let end = CompositeKey::new(b"rec2", b"b").unwrap();
    let collected: Vec<(&[u8], &[u8])> = sl
        .range(&start, &end)
        .map(|n| (n.key.record_id(), n.key.item_key()))
        .collect();

    assert_eq!(
        collected,
        vec![
            (b"rec1".as_slice(), b"y".as_slice()),
            (b"rec2".as_slice(), b"a".as_slice()),
        ]
    );
}

// === Range From Tests ===

#[test]
fn test_range_from_beginning() {
    let mut sl = SkipList::new();
    for i in 0..20 {
        sl.insert(make_entry(
            "rec1",
            &format!("key{:02}", i),
            "val",
            EntryType::Put,
        ));
    }

    let min = CompositeKey::min_key_for_record(b"rec1").unwrap();
    let count = sl.range_from(&min).count();
    assert_eq!(count, 20);
}

#[test]
fn test_range_from_middle() {
    let mut sl = SkipList::new();
    for i in 0..20 {
        sl.insert(make_entry(
            "rec1",
            &format!("key{:02}", i),
            "val",
            EntryType::Put,
        ));
    }

    let start = CompositeKey::new(b"rec1", b"key10").unwrap();
    let collected: Vec<String> = sl
        .range_from(&start)
        .map(|n| String::from_utf8_lossy(n.key.item_key()).to_string())
        .collect();

    assert_eq!(collected.len(), 10);
    assert_eq!(collected[0], "key10");
    assert_eq!(collected[9], "key19");
}

#[test]
fn test_range_from_past_end() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "a", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "b", "val", EntryType::Put));

    let start = CompositeKey::new(b"zzz", b"zzz").unwrap();
    let count = sl.range_from(&start).count();
    assert_eq!(count, 0);
}

// === Record Scan Tests ===

#[test]
fn test_scan_record_returns_all_items() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "a", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "b", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "c", "val", EntryType::Put));
    sl.insert(make_entry("rec2", "x", "val", EntryType::Put));

    let collected: Vec<String> = sl
        .scan_record(b"rec1")
        .map(|n| String::from_utf8_lossy(n.key.item_key()).to_string())
        .collect();

    assert_eq!(collected, vec!["a", "b", "c"]);
}

#[test]
fn test_scan_record_stops_at_boundary() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("alpha", "key1", "val", EntryType::Put));
    sl.insert(make_entry("alpha", "key2", "val", EntryType::Put));
    sl.insert(make_entry("beta", "key1", "val", EntryType::Put));
    sl.insert(make_entry("beta", "key2", "val", EntryType::Put));
    sl.insert(make_entry("gamma", "key1", "val", EntryType::Put));

    let alpha_items: Vec<_> = sl.scan_record(b"alpha").collect();
    assert_eq!(alpha_items.len(), 2);
    for node in &alpha_items {
        assert_eq!(node.key.record_id(), b"alpha");
    }

    let beta_items: Vec<_> = sl.scan_record(b"beta").collect();
    assert_eq!(beta_items.len(), 2);
    for node in &beta_items {
        assert_eq!(node.key.record_id(), b"beta");
    }
}

#[test]
fn test_scan_record_nonexistent() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "key1", "val", EntryType::Put));

    let collected: Vec<_> = sl.scan_record(b"no_such_record").collect();
    assert!(collected.is_empty());
}

#[test]
fn test_scan_record_with_multiple_versions() {
    let mut sl = SkipList::new();
    sl.insert(make_entry_with_seq("rec1", "key1", "old", 1));
    sl.insert(make_entry_with_seq("rec1", "key1", "mid", 3));
    sl.insert(make_entry_with_seq("rec1", "key1", "new", 5));

    let collected: Vec<(u64, &str)> = sl
        .scan_record(b"rec1")
        .map(|n| {
            (
                n.sequence_number,
                std::str::from_utf8(&n.value).unwrap(),
            )
        })
        .collect();

    // Should be sorted seq DESC: 5, 3, 1
    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0], (5, "new"));
    assert_eq!(collected[1], (3, "mid"));
    assert_eq!(collected[2], (1, "old"));
}

#[test]
fn test_scan_record_includes_tombstones() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "key1", "val", EntryType::Put));
    sl.insert(make_entry("rec1", "key2", "", EntryType::Delete));

    let entry = MemtableEntry::new(
        CompositeKey::range_tombstone_key(b"rec1", b"key3").unwrap(),
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::RangeDelete,
    );
    sl.insert(entry);

    let collected: Vec<EntryType> = sl
        .scan_record(b"rec1")
        .map(|n| n.entry_type)
        .collect();

    assert!(collected.contains(&EntryType::Put));
    assert!(collected.contains(&EntryType::Delete));
    assert!(collected.contains(&EntryType::RangeDelete));
}

// === Into Iterator Tests ===

#[test]
fn test_into_iter_yields_all_entries() {
    let mut sl = SkipList::new();
    for i in 0..50 {
        sl.insert(make_entry(
            "rec1",
            &format!("key{:03}", i),
            &format!("val{}", i),
            EntryType::Put,
        ));
    }

    // Collect borrow iter keys first
    let borrow_keys: Vec<Vec<u8>> = sl.iter().map(|n| n.key.as_bytes().to_vec()).collect();

    // Now consume
    let owned_keys: Vec<Vec<u8>> = sl.into_iter().map(|n| n.key.as_bytes().to_vec()).collect();

    assert_eq!(borrow_keys, owned_keys);
}

#[test]
fn test_into_iter_entries_are_owned() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "key1", "val1", EntryType::Put));
    sl.insert(make_entry("rec1", "key2", "val2", EntryType::Put));

    let owned_nodes: Vec<_> = sl.into_iter().collect();
    assert_eq!(owned_nodes.len(), 2);

    // Verify we have owned data
    assert_eq!(owned_nodes[0].value, Bytes::from("val1"));
    assert_eq!(owned_nodes[1].value, Bytes::from("val2"));
    assert_eq!(owned_nodes[0].key.item_key(), b"key1");
    assert_eq!(owned_nodes[1].key.item_key(), b"key2");
}

// === Edge Cases ===

#[test]
fn test_iter_with_duplicate_keys() {
    let mut sl = SkipList::new();
    sl.insert(make_entry_with_seq("rec1", "key1", "v1", 1));
    sl.insert(make_entry_with_seq("rec1", "key1", "v2", 2));
    sl.insert(make_entry_with_seq("rec1", "key1", "v3", 3));

    let collected: Vec<(u64, String)> = sl
        .iter()
        .map(|n| {
            (
                n.sequence_number,
                String::from_utf8_lossy(&n.value).to_string(),
            )
        })
        .collect();

    // All three yielded, seq DESC
    assert_eq!(collected.len(), 3);
    assert_eq!(collected[0].0, 3);
    assert_eq!(collected[1].0, 2);
    assert_eq!(collected[2].0, 1);
}

#[test]
fn test_range_with_empty_item_keys() {
    let mut sl = SkipList::new();
    sl.insert(make_entry("rec1", "", "empty_key", EntryType::Put));
    sl.insert(make_entry("rec1", "a", "a_key", EntryType::Put));
    sl.insert(make_entry("rec1", "b", "b_key", EntryType::Put));

    let start = CompositeKey::new(b"rec1", b"").unwrap();
    let end = CompositeKey::new(b"rec1", b"b").unwrap();
    let collected: Vec<String> = sl
        .range(&start, &end)
        .map(|n| String::from_utf8_lossy(&n.value).to_string())
        .collect();

    assert_eq!(collected, vec!["empty_key", "a_key"]);
}
