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

// === Insert & Lookup Tests ===

#[test]
fn test_insert_single_entry() {
    let mut sl = SkipList::new();
    let entry = make_entry("rec1", "key1", "val1", EntryType::Put);
    sl.insert(entry);

    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let node = sl.get(&key).expect("should find inserted entry");
    assert_eq!(node.value, Bytes::from("val1"));
    assert_eq!(node.entry_type, EntryType::Put);
}

#[test]
fn test_insert_multiple_entries() {
    let mut sl = SkipList::new();
    for i in 0..10 {
        let entry = make_entry(
            "rec1",
            &format!("key{:03}", i),
            &format!("val{}", i),
            EntryType::Put,
        );
        sl.insert(entry);
    }

    assert_eq!(sl.len(), 10);

    for i in 0..10 {
        let key = CompositeKey::new(b"rec1", format!("key{:03}", i).as_bytes()).unwrap();
        let node = sl.get(&key).expect("should find entry");
        assert_eq!(node.value, Bytes::from(format!("val{}", i)));
    }
}

#[test]
fn test_insert_duplicate_key_different_sequence() {
    let mut sl = SkipList::new();

    let entry1 = make_entry_with_seq("rec1", "key1", "old", 1);
    let entry2 = make_entry_with_seq("rec1", "key1", "new", 5);
    sl.insert(entry1);
    sl.insert(entry2);

    assert_eq!(sl.len(), 2);

    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let node = sl.get(&key).expect("should find entry with highest seq");
    assert_eq!(node.sequence_number, 5);
    assert_eq!(node.value, Bytes::from("new"));
}

#[test]
fn test_insert_overwrites_same_key_same_sequence() {
    let mut sl = SkipList::new();

    let entry1 = make_entry_with_seq("rec1", "key1", "original", 3);
    let entry2 = make_entry_with_seq("rec1", "key1", "replaced", 3);
    sl.insert(entry1);
    sl.insert(entry2);

    assert_eq!(sl.len(), 1);

    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let node = sl.get(&key).expect("should find entry");
    assert_eq!(node.value, Bytes::from("replaced"));
    assert_eq!(node.sequence_number, 3);
}

#[test]
fn test_get_nonexistent_key() {
    let mut sl = SkipList::new();
    let entry = make_entry("rec1", "key1", "val", EntryType::Put);
    sl.insert(entry);

    let key = CompositeKey::new(b"rec1", b"no_such_key").unwrap();
    assert!(sl.get(&key).is_none());
}

#[test]
fn test_contains_key() {
    let mut sl = SkipList::new();
    let key = CompositeKey::new(b"rec1", b"key1").unwrap();

    assert!(!sl.contains_key(&key));

    let entry = make_entry("rec1", "key1", "val", EntryType::Put);
    sl.insert(entry);

    assert!(sl.contains_key(&key));

    let absent = CompositeKey::new(b"rec1", b"absent").unwrap();
    assert!(!sl.contains_key(&absent));
}

// === Ordering Tests ===

#[test]
fn test_entries_sorted_by_composite_key() {
    let mut sl = SkipList::new();

    let keys = ["c", "a", "e", "b", "d"];
    for k in &keys {
        let entry = make_entry("rec1", k, "val", EntryType::Put);
        sl.insert(entry);
    }

    let collected: Vec<&[u8]> = sl.iter().map(|n| n.key.item_key()).collect();
    assert_eq!(
        collected,
        vec![b"a".as_slice(), b"b", b"c", b"d", b"e"]
    );
}

#[test]
fn test_same_key_sorted_by_sequence_desc() {
    let mut sl = SkipList::new();

    for seq in [3, 1, 5, 2, 4] {
        let entry = make_entry_with_seq("rec1", "key1", &format!("v{}", seq), seq);
        sl.insert(entry);
    }

    let seqs: Vec<u64> = sl.iter().map(|n| n.sequence_number).collect();
    assert_eq!(seqs, vec![5, 4, 3, 2, 1]);
}

#[test]
fn test_cross_record_ordering() {
    let mut sl = SkipList::new();

    sl.insert(make_entry("aab", "key1", "val", EntryType::Put));
    sl.insert(make_entry("aaa", "key2", "val", EntryType::Put));
    sl.insert(make_entry("aaa", "key1", "val", EntryType::Put));
    sl.insert(make_entry("aab", "key2", "val", EntryType::Put));

    let collected: Vec<(&[u8], &[u8])> = sl
        .iter()
        .map(|n| (n.key.record_id(), n.key.item_key()))
        .collect();

    assert_eq!(
        collected,
        vec![
            (b"aaa".as_slice(), b"key1".as_slice()),
            (b"aaa".as_slice(), b"key2".as_slice()),
            (b"aab".as_slice(), b"key1".as_slice()),
            (b"aab".as_slice(), b"key2".as_slice()),
        ]
    );
}

// === Size Tracking Tests ===

#[test]
fn test_len_increments() {
    let mut sl = SkipList::new();
    assert_eq!(sl.len(), 0);

    sl.insert(make_entry("rec1", "key1", "val", EntryType::Put));
    assert_eq!(sl.len(), 1);

    sl.insert(make_entry("rec1", "key2", "val", EntryType::Put));
    assert_eq!(sl.len(), 2);

    sl.insert(make_entry("rec2", "key1", "val", EntryType::Put));
    assert_eq!(sl.len(), 3);
}

#[test]
fn test_is_empty() {
    let mut sl = SkipList::new();
    assert!(sl.is_empty());

    sl.insert(make_entry("rec1", "key1", "val", EntryType::Put));
    assert!(!sl.is_empty());
}

#[test]
fn test_approximate_memory_usage_increases() {
    let mut sl = SkipList::new();
    assert_eq!(sl.approximate_memory_usage(), 0);

    sl.insert(make_entry("rec1", "key1", "val", EntryType::Put));
    let usage1 = sl.approximate_memory_usage();
    assert!(usage1 > 0);

    sl.insert(make_entry("rec1", "key2", "a longer value string", EntryType::Put));
    let usage2 = sl.approximate_memory_usage();
    assert!(usage2 > usage1);
}

// === Scale Tests ===

#[test]
fn test_100k_entries_sorted_correctly() {
    let mut sl = SkipList::new();
    let mut oracle: BTreeMap<(Vec<u8>, Reverse<u64>), String> = BTreeMap::new();
    let mut rng = SmallRng::seed_from_u64(42);

    for i in 0u64..100_000 {
        let record_id = format!("r{:05}", rng.random_range(0..100u32));
        let item_key = format!("k{:05}", rng.random_range(0..1000u32));
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

    let sl_keys: Vec<(Vec<u8>, u64)> = sl
        .iter()
        .map(|n| (n.key.as_bytes().to_vec(), n.sequence_number))
        .collect();

    let oracle_keys: Vec<(Vec<u8>, u64)> = oracle
        .keys()
        .map(|(k, Reverse(s))| (k.clone(), *s))
        .collect();

    assert_eq!(sl_keys.len(), oracle_keys.len());
    for (sl_entry, oracle_entry) in sl_keys.iter().zip(oracle_keys.iter()) {
        assert_eq!(sl_entry, oracle_entry);
    }
}

#[test]
fn test_100k_entries_point_lookup() {
    let mut sl = SkipList::new();
    let mut rng = SmallRng::seed_from_u64(42);
    let mut keys_and_seqs: Vec<(CompositeKey, u64, String)> = Vec::new();

    for i in 0u64..100_000 {
        let record_id = format!("r{:05}", rng.random_range(0..100u32));
        let item_key = format!("k{:05}", rng.random_range(0..1000u32));
        let value = format!("v{}", i);
        let seq = i;

        let key = CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap();
        keys_and_seqs.push((key.clone(), seq, value.clone()));

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

    // Lookup 1000 random entries
    let mut lookup_rng = SmallRng::seed_from_u64(999);
    for _ in 0..1000 {
        let idx = lookup_rng.random_range(0..keys_and_seqs.len());
        let (ref key, _seq, _) = keys_and_seqs[idx];
        assert!(
            sl.contains_key(key),
            "should find key at index {}",
            idx
        );
    }
}

#[test]
fn test_random_height_distribution() {
    use flushdb_engine::skiplist::MAX_HEIGHT;

    let mut rng = SmallRng::seed_from_u64(12345);
    let mut counts = vec![0usize; MAX_HEIGHT + 1];
    let total = 10_000;

    for _ in 0..total {
        let mut height = 1;
        while height < MAX_HEIGHT && rng.random_range(0..4u32) == 0 {
            height += 1;
        }
        counts[height] += 1;
    }

    // ~75% should be height 1 (probability = 3/4)
    let pct_height_1 = counts[1] as f64 / total as f64;
    assert!(
        pct_height_1 > 0.70 && pct_height_1 < 0.80,
        "expected ~75% height 1, got {:.1}%",
        pct_height_1 * 100.0
    );

    // Height 5+ should be <1% (probability = (1/4)^4 = 0.39%)
    let count_5_plus: usize = counts[5..].iter().sum();
    let pct_5_plus = count_5_plus as f64 / total as f64;
    assert!(
        pct_5_plus < 0.01,
        "expected <1% height 5+, got {:.2}%",
        pct_5_plus * 100.0
    );
}

// === Edge Cases ===

#[test]
fn test_insert_with_empty_value() {
    let mut sl = SkipList::new();
    let entry = make_entry("rec1", "key1", "", EntryType::Delete);
    sl.insert(entry);

    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let node = sl.get(&key).expect("should find delete entry");
    assert_eq!(node.value, Bytes::new());
    assert_eq!(node.entry_type, EntryType::Delete);
}

#[test]
fn test_insert_with_max_length_key() {
    let record_id = "x".repeat(256);
    let item_key = "y".repeat(4096);

    let mut sl = SkipList::new();
    let entry = MemtableEntry::new(
        CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap(),
        Bytes::from("val"),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Put,
    );
    sl.insert(entry);

    let key = CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap();
    let node = sl.get(&key).expect("should find max-length key");
    assert_eq!(node.value, Bytes::from("val"));
}

#[test]
fn test_single_entry_skiplist() {
    let mut sl = SkipList::new();
    let entry = make_entry("rec1", "only_key", "only_val", EntryType::Put);
    sl.insert(entry);

    assert_eq!(sl.len(), 1);
    assert!(!sl.is_empty());
    assert!(sl.approximate_memory_usage() > 0);

    let key = CompositeKey::new(b"rec1", b"only_key").unwrap();
    assert!(sl.contains_key(&key));

    let node = sl.get(&key).expect("should find the only entry");
    assert_eq!(node.value, Bytes::from("only_val"));

    let collected: Vec<&[u8]> = sl.iter().map(|n| n.key.item_key()).collect();
    assert_eq!(collected, vec![b"only_key".as_slice()]);
}

#[test]
fn test_skiplist_with_arena() {
    use flushdb_engine::arena::Arena;

    let arena = Arena::with_block_size(4096);
    let mut sl = SkipList::with_arena(arena);
    sl.insert(make_entry("rec1", "key1", "val1", EntryType::Put));

    assert_eq!(sl.len(), 1);
    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let node = sl.get(&key).expect("should find entry");
    assert_eq!(node.value, Bytes::from("val1"));
    assert!(sl.approximate_memory_usage() > 0);
}

#[test]
fn test_skiplist_default_same_as_new() {
    let default_sl = SkipList::default();
    let new_sl = SkipList::new();
    assert_eq!(default_sl.len(), new_sl.len());
    assert!(default_sl.is_empty());
    assert!(new_sl.is_empty());
}

#[test]
fn test_height_accessor() {
    let mut sl = SkipList::new();
    let entry = make_entry("rec1", "key1", "val", EntryType::Put);
    sl.insert(entry);

    let key = CompositeKey::new(b"rec1", b"key1").unwrap();
    let node = sl.get(&key).unwrap();
    assert!(node.height() >= 1);
    assert!(node.height() <= 12);
}
