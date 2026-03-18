use bytes::Bytes;
use flushdb_engine::{
    DedupSet, Memtable, MemtableConfig, MemtableList, RangeTombstone, RangeTombstoneIndex,
    SkipNode,
};
use flushdb_types::{CompositeKey, EntryType, IdempotencyToken, MemtableEntry};

fn make_put(record: &str, key: &str, value: &str) -> MemtableEntry {
    MemtableEntry::new(
        CompositeKey::new(record.as_bytes(), key.as_bytes()).unwrap(),
        Bytes::from(value.to_string()),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Put,
    )
}

#[test]
fn test_memtable_config_default() {
    let config = MemtableConfig::default();
    assert_eq!(config.size_threshold, 67_108_864);
    assert_eq!(config.max_frozen_count, 3);
}

#[test]
fn test_into_skiplist_on_frozen() {
    let mut mt = Memtable::new(MemtableConfig::default(), 1);
    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    mt.insert(make_put("r1", "k2", "v2")).unwrap();
    mt.freeze();

    let skiplist = mt.into_skiplist();
    let nodes: Vec<SkipNode> = skiplist.into_iter().collect();
    assert_eq!(nodes.len(), 2);
}

#[test]
#[cfg_attr(not(debug_assertions), ignore)]
#[should_panic]
fn test_into_skiplist_panics_on_active() {
    let mut mt = Memtable::new(MemtableConfig::default(), 1);
    mt.insert(make_put("r1", "k1", "v1")).unwrap();
    let _skiplist = mt.into_skiplist();
}

#[test]
fn test_range_tombstones_accessor() {
    let mut mt = Memtable::new(MemtableConfig::default(), 1);
    let rt_entry = MemtableEntry::new(
        CompositeKey::range_tombstone_key(b"r1", b"a").unwrap(),
        Bytes::from("z"),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::RangeDelete,
    );
    mt.insert(rt_entry).unwrap();

    let index: &RangeTombstoneIndex = mt.range_tombstones();
    assert_eq!(index.len(), 1);
}

#[test]
fn test_full_lifecycle() {
    let config = MemtableConfig {
        size_threshold: 67_108_864,
        max_frozen_count: 3,
        ..Default::default()
    };
    let mut list = MemtableList::new(config, 1);

    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.insert(make_put("r1", "k2", "v2")).unwrap();
    assert_eq!(list.active_entry_count(), 2);

    list.freeze_active().unwrap();
    assert_eq!(list.frozen_count(), 1);
    assert_eq!(list.active_entry_count(), 0);

    list.insert(make_put("r1", "k3", "v3")).unwrap();
    list.insert(make_put("r1", "k1", "v1_updated")).unwrap();
    assert_eq!(list.active_entry_count(), 2);

    let key1 = CompositeKey::new(b"r1", b"k1").unwrap();
    let entry = list.get(&key1).expect("should find k1 in active");
    assert_eq!(entry.value, Bytes::from("v1_updated"));

    let key2 = CompositeKey::new(b"r1", b"k2").unwrap();
    let entry = list.get(&key2).expect("should find k2 in frozen");
    assert_eq!(entry.value, Bytes::from("v2"));

    let start = CompositeKey::new(b"r1", b"k1").unwrap();
    let end = CompositeKey::new(b"r1", b"k4").unwrap();
    let results = list.scan(&start, &end);
    assert_eq!(results.len(), 3);

    let popped = list.pop_oldest_frozen().expect("should have frozen");
    assert!(popped.is_frozen());
    assert_eq!(list.frozen_count(), 0);

    let skiplist = popped.into_skiplist();
    let nodes: Vec<SkipNode> = skiplist.into_iter().collect();
    assert_eq!(nodes.len(), 2);
}

#[test]
fn test_flush_pipeline_simulation() {
    let config = MemtableConfig {
        size_threshold: 67_108_864,
        max_frozen_count: 3,
        ..Default::default()
    };
    let mut list = MemtableList::new(config, 1);

    for i in 0..1000u32 {
        let key = format!("k{i:06}");
        let val = format!("v{i:06}");
        list.insert(make_put("r1", &key, &val)).unwrap();
    }
    assert_eq!(list.active_entry_count(), 1000);

    list.freeze_active().unwrap();
    assert_eq!(list.active_entry_count(), 0);
    assert_eq!(list.total_entry_count(), 1000);

    let popped = list.pop_oldest_frozen().expect("should have frozen memtable");
    assert!(popped.is_frozen());
    assert_eq!(popped.entry_count(), 1000);

    let skiplist = popped.into_skiplist();
    let nodes: Vec<SkipNode> = skiplist.into_iter().collect();
    assert_eq!(nodes.len(), 1000);

    // Verify sorted order
    for window in nodes.windows(2) {
        assert!(
            window[0].key < window[1].key,
            "nodes should be sorted by key"
        );
    }

    // Verify completeness
    for (i, node) in nodes.iter().enumerate() {
        let expected_key =
            CompositeKey::new(b"r1", format!("k{i:06}").as_bytes()).unwrap();
        assert_eq!(node.key, expected_key);
        assert_eq!(node.value, Bytes::from(format!("v{i:06}")));
    }
}

#[test]
fn test_dedup_set_via_public_api() {
    let mut ds = DedupSet::new();
    assert!(ds.is_empty());
    assert_eq!(ds.len(), 0);

    let token = IdempotencyToken::from_parts(1000, {
        let mut buf = [0u8; 16];
        buf[0] = 1;
        buf
    });

    ds.insert(token);
    assert!(ds.contains(&token));
    assert_eq!(ds.len(), 1);
    assert!(!ds.is_empty());
}

#[test]
fn test_range_tombstone_public_fields() {
    let rt = RangeTombstone {
        record_id: Bytes::from("r1"),
        start_key: Bytes::from("a"),
        end_key: Bytes::from("z"),
        sequence_number: 42,
    };
    assert_eq!(rt.record_id.as_ref(), b"r1");
    assert_eq!(rt.start_key.as_ref(), b"a");
    assert_eq!(rt.end_key.as_ref(), b"z");
    assert_eq!(rt.sequence_number, 42);
}

#[test]
fn test_range_tombstone_index_via_public_api() {
    let mut index = RangeTombstoneIndex::new();
    assert!(index.is_empty());

    index.add(RangeTombstone {
        record_id: Bytes::from("r1"),
        start_key: Bytes::from("a"),
        end_key: Bytes::from("m"),
        sequence_number: 10,
    });
    assert_eq!(index.len(), 1);
    assert!(!index.is_empty());

    assert!(index.covers(b"r1", b"c", 5));
    assert!(!index.covers(b"r1", b"z", 5));
    assert!(!index.covers(b"r1", b"c", 15));
}

#[test]
fn test_skip_node_public_fields() {
    let mut mt = Memtable::new(MemtableConfig::default(), 1);
    mt.insert(make_put("r1", "k1", "hello")).unwrap();
    mt.freeze();

    let skiplist = mt.into_skiplist();
    let mut nodes: Vec<SkipNode> = skiplist.into_iter().collect();
    assert_eq!(nodes.len(), 1);

    let node = nodes.remove(0);
    assert_eq!(node.key, CompositeKey::new(b"r1", b"k1").unwrap());
    assert_eq!(node.value, Bytes::from("hello"));
    assert_eq!(node.entry_type, EntryType::Put);
    assert_eq!(node.sequence_number, 1);
    assert!(node.idempotency_key.is_none());
}
