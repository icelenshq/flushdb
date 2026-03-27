use flushdb_engine::sstable::index_block::{IndexBlock, IndexBlockBuilder, IndexEntry};
use flushdb_types::{CompositeKey, FlushError};

fn key(record_id: &[u8], item_key: &[u8]) -> CompositeKey {
    CompositeKey::new(record_id, item_key).unwrap()
}

fn build_three_entry_index() -> IndexBlock {
    let mut builder = IndexBlockBuilder::new();
    builder.add(key(b"aaa", b"001"), 0, 4096, 4096);
    builder.add(key(b"bbb", b"001"), 4096, 4096, 4096);
    builder.add(key(b"ccc", b"001"), 8192, 4096, 4096);
    builder.build()
}

// ─── Binary Search ──────────────────────────────────────────────────

#[test]
fn test_find_block_exact_match() {
    let index = build_three_entry_index();
    let key_b = key(b"bbb", b"001");

    let entry = index.find_block(&key_b).expect("exact match must be found");
    assert_eq!(entry.first_key.as_bytes(), key_b.as_bytes());
    assert_eq!(entry.block_offset, 4096);
}

#[test]
fn test_find_block_between_keys() {
    let index = build_three_entry_index();
    let between = key(b"abc", b"999");

    let entry = index
        .find_block(&between)
        .expect("key between blocks must find preceding block");
    let expected_first = key(b"aaa", b"001");
    assert_eq!(entry.first_key.as_bytes(), expected_first.as_bytes());
    assert_eq!(entry.block_offset, 0);
}

#[test]
fn test_find_block_before_all() {
    let index = build_three_entry_index();
    let before_all = key(b"\x01", b"\x01");

    assert!(
        index.find_block(&before_all).is_none(),
        "key before all blocks must return None"
    );
}

#[test]
fn test_find_block_after_last() {
    let index = build_three_entry_index();
    let after_last = key(b"zzz", b"999");

    let entry = index
        .find_block(&after_last)
        .expect("key after last block must return last block");
    let expected_first = key(b"ccc", b"001");
    assert_eq!(entry.first_key.as_bytes(), expected_first.as_bytes());
    assert_eq!(entry.block_offset, 8192);
}

#[test]
fn test_find_block_single_entry() {
    let mut builder = IndexBlockBuilder::new();
    builder.add(key(b"mmm", b"001"), 0, 1024, 1024);
    let index = builder.build();

    let at_key = key(b"mmm", b"001");
    let entry = index.find_block(&at_key).expect("exact key must be found");
    assert_eq!(entry.block_offset, 0);
    assert_eq!(entry.block_size, 1024);
    assert_eq!(entry.first_key.as_bytes(), at_key.as_bytes());

    let after_key = key(b"nnn", b"001");
    let entry = index
        .find_block(&after_key)
        .expect("key after single entry must find that entry");
    assert_eq!(entry.block_offset, 0, "should return the only block");

    let before_key = key(b"aaa", b"001");
    assert!(
        index.find_block(&before_key).is_none(),
        "key before single entry must return None"
    );
}

// ─── Key Range ──────────────────────────────────────────────────────

#[test]
fn test_key_range_multiple_blocks() {
    let index = build_three_entry_index();
    let (min, max) = index
        .key_range()
        .expect("non-empty index must have a key range");

    let expected_min = key(b"aaa", b"001");
    let expected_max = key(b"ccc", b"001");
    assert_eq!(min.as_bytes(), expected_min.as_bytes());
    assert_eq!(max.as_bytes(), expected_max.as_bytes());
}

#[test]
fn test_key_range_empty() {
    let builder = IndexBlockBuilder::new();
    let index = builder.build();
    assert!(index.key_range().is_none());
}

#[test]
fn test_overlaps_disjoint_before() {
    let index = build_three_entry_index();
    let start = key(b"\x01", b"\x01");
    let end = key(b"\x02", b"\x02");

    assert!(
        !index.overlaps(&start, &end),
        "range entirely before index must not overlap"
    );
}

#[test]
fn test_overlaps_disjoint_after() {
    let index = build_three_entry_index();
    let start = key(b"ddd", b"001");
    let end = key(b"zzz", b"999");

    assert!(
        !index.overlaps(&start, &end),
        "range entirely after index must not overlap"
    );
}

#[test]
fn test_overlaps_contained() {
    let index = build_three_entry_index();
    let start = key(b"aab", b"001");
    let end = key(b"bbc", b"001");

    assert!(
        index.overlaps(&start, &end),
        "range within index range must overlap"
    );
}

#[test]
fn test_overlaps_partial() {
    let index = build_three_entry_index();
    let start = key(b"bbb", b"500");
    let end = key(b"zzz", b"999");

    assert!(
        index.overlaps(&start, &end),
        "partially overlapping range must overlap"
    );
}

#[test]
fn test_overlaps_empty_index() {
    let builder = IndexBlockBuilder::new();
    let index = builder.build();

    let start = key(b"aaa", b"001");
    let end = key(b"zzz", b"999");
    assert!(
        !index.overlaps(&start, &end),
        "empty index must not overlap with any range"
    );
}

// ─── Serialization ──────────────────────────────────────────────────

#[test]
fn test_serialize_deserialize_round_trip() {
    let mut builder = IndexBlockBuilder::new();
    for i in 0..10u32 {
        let rid = format!("rec_{i:03}");
        let ik = format!("item_{i:03}");
        let ck = key(rid.as_bytes(), ik.as_bytes());
        builder.add(ck, (i as u64) * 4096, 4096, 4000 + i);
    }
    let index = builder.build();
    let serialized = index.serialize();
    let restored = IndexBlock::deserialize(&serialized).expect("deserialize must succeed");

    assert_eq!(index.block_count(), restored.block_count());

    for i in 0..index.block_count() {
        let orig = index.get(i).unwrap();
        let rest = restored.get(i).unwrap();
        assert_eq!(orig.first_key.as_bytes(), rest.first_key.as_bytes());
        assert_eq!(orig.block_offset, rest.block_offset);
        assert_eq!(orig.block_size, rest.block_size);
        assert_eq!(orig.uncompressed_size, rest.uncompressed_size);
    }
}

#[test]
fn test_serialize_empty_index() {
    let builder = IndexBlockBuilder::new();
    let index = builder.build();
    let serialized = index.serialize();

    assert_eq!(
        serialized.len(),
        4,
        "empty index must serialize as 4 bytes [count=0]"
    );
    assert_eq!(&serialized[..], &[0, 0, 0, 0]);

    let restored = IndexBlock::deserialize(&serialized).expect("deserialize must succeed");
    assert_eq!(restored.block_count(), 0);
}

#[test]
fn test_deserialize_rejects_truncated() {
    let data = [0u8; 3];
    let result = IndexBlock::deserialize(&data);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "truncated data must yield CorruptedData"
    );
}

#[test]
fn test_deserialize_rejects_truncated_entry() {
    // Header says 1 entry, but no entry data follows
    let mut data = vec![0u8; 4];
    data[0] = 1; // count = 1
    let result = IndexBlock::deserialize(&data);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "truncated entry data must yield CorruptedData"
    );
}

// ─── Ordering ───────────────────────────────────────────────────────

#[test]
fn test_entries_in_key_order() {
    let mut builder = IndexBlockBuilder::new();
    builder.add(key(b"alpha", b"001"), 0, 1024, 1024);
    builder.add(key(b"beta", b"001"), 1024, 1024, 1024);
    builder.add(key(b"gamma", b"001"), 2048, 1024, 1024);
    builder.add(key(b"zeta", b"001"), 3072, 1024, 1024);
    let index = builder.build();

    let entries: Vec<&IndexEntry> = index.iter().collect();
    for i in 0..entries.len() - 1 {
        assert!(
            entries[i].first_key.as_bytes() < entries[i + 1].first_key.as_bytes(),
            "entry {i} must be ordered before entry {}",
            i + 1
        );
    }
}
