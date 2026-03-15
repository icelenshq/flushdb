use bytes::Bytes;
use flushdb_engine::memtable::MemtableConfig;
use flushdb_engine::sstable::BlockEntry;
use flushdb_engine::{Memtable, MergeEntry, MergeIterator, MergeSource, VecSource};
use flushdb_types::{CompositeKey, EntryType, EntryValue, IdempotencyToken, MemtableEntry};

fn make_merge_entry(
    record_id: &str,
    item_key: &str,
    seq: u64,
    entry_type: EntryType,
) -> MergeEntry {
    MergeEntry {
        composite_key: CompositeKey::new(record_id.as_bytes(), item_key.as_bytes()).unwrap(),
        value: Bytes::from(format!("{record_id}:{item_key}:v{seq}")),
        metadata: Bytes::new(),
        entry_type,
        sequence_number: seq,
    }
}

fn make_put(record_id: &str, item_key: &str, seq: u64) -> MergeEntry {
    make_merge_entry(record_id, item_key, seq, EntryType::Put)
}

fn make_delete(record_id: &str, item_key: &str, seq: u64) -> MergeEntry {
    make_merge_entry(record_id, item_key, seq, EntryType::Delete)
}

fn make_range_delete(record_id: &str, item_key: &str, seq: u64) -> MergeEntry {
    make_merge_entry(record_id, item_key, seq, EntryType::RangeDelete)
}

fn collect_all(iter: &mut MergeIterator) -> Vec<MergeEntry> {
    let mut results = Vec::new();
    while let Some(entry) = iter.next_entry() {
        results.push(entry);
    }
    results
}

fn collect_deduped(iter: &mut MergeIterator) -> Vec<MergeEntry> {
    let mut results = Vec::new();
    while let Some(entry) = iter.next_deduped() {
        results.push(entry);
    }
    results
}

fn assert_key(entry: &MergeEntry, record_id: &str, item_key: &str) {
    assert_eq!(
        entry.composite_key.record_id(),
        record_id.as_bytes(),
        "expected record_id={record_id}, got {:?}",
        String::from_utf8_lossy(entry.composite_key.record_id())
    );
    assert_eq!(
        entry.composite_key.item_key(),
        item_key.as_bytes(),
        "expected item_key={item_key}, got {:?}",
        String::from_utf8_lossy(entry.composite_key.item_key())
    );
}

fn make_source(entries: Vec<MergeEntry>, source_id: usize) -> Box<dyn flushdb_engine::MergeSource> {
    Box::new(VecSource::new(entries, source_id))
}

// =============================================================================
// Basic Merge Tests
// =============================================================================

#[test]
fn test_merge_single_source() {
    let entries = vec![
        make_put("a", "k1", 1),
        make_put("a", "k2", 2),
        make_put("b", "k1", 3),
    ];
    let mut iter = MergeIterator::new(vec![make_source(entries, 0)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 3);
    assert_key(&results[0], "a", "k1");
    assert_key(&results[1], "a", "k2");
    assert_key(&results[2], "b", "k1");
    assert!(iter.is_exhausted());
}

#[test]
fn test_merge_two_sorted_sources() {
    let source0 = vec![make_put("a", "k1", 1), make_put("c", "k1", 3)];
    let source1 = vec![make_put("b", "k1", 2), make_put("d", "k1", 4)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 4);
    assert_key(&results[0], "a", "k1");
    assert_key(&results[1], "b", "k1");
    assert_key(&results[2], "c", "k1");
    assert_key(&results[3], "d", "k1");
}

#[test]
fn test_merge_interleaved_keys() {
    let source0 = vec![
        make_put("a", "k1", 1),
        make_put("a", "k3", 3),
        make_put("b", "k2", 5),
    ];
    let source1 = vec![
        make_put("a", "k2", 2),
        make_put("b", "k1", 4),
        make_put("b", "k3", 6),
    ];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 6);
    assert_key(&results[0], "a", "k1");
    assert_key(&results[1], "a", "k2");
    assert_key(&results[2], "a", "k3");
    assert_key(&results[3], "b", "k1");
    assert_key(&results[4], "b", "k2");
    assert_key(&results[5], "b", "k3");
}

#[test]
fn test_merge_empty_sources() {
    let mut iter = MergeIterator::new(vec![
        make_source(vec![], 0),
        make_source(vec![], 1),
        make_source(vec![], 2),
    ]);

    assert!(iter.is_exhausted());
    assert!(iter.next_entry().is_none());
    assert!(iter.next_deduped().is_none());
}

#[test]
fn test_merge_one_empty_one_full() {
    let entries = vec![make_put("x", "k1", 1), make_put("y", "k1", 2)];

    let mut iter = MergeIterator::new(vec![make_source(vec![], 0), make_source(entries, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 2);
    assert_key(&results[0], "x", "k1");
    assert_key(&results[1], "y", "k1");
}

// =============================================================================
// Deduplication Tests
// =============================================================================

#[test]
fn test_dedup_same_key_different_sequences() {
    // Source 0 (newer, lower source_id) has seq 10
    // Source 1 (older, higher source_id) has seq 5
    // Dedup should keep the entry from source 0 (newest data)
    let source0 = vec![make_put("rec", "item", 10)];
    let source1 = vec![make_put("rec", "item", 5)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_key(&results[0], "rec", "item");
    assert_eq!(results[0].sequence_number, 10);
    assert!(iter.is_exhausted());
}

#[test]
fn test_dedup_same_key_newer_source_wins() {
    // Same key, same sequence number — lower source_id should win
    let source0 = vec![make_put("rec", "item", 7)];
    let source1 = vec![make_put("rec", "item", 7)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_key(&results[0], "rec", "item");
    // Both have seq 7; the one from source 0 wins because lower source_id = newer
    assert_eq!(results[0].sequence_number, 7);
    // Verify value came from source 0
    assert_eq!(results[0].value.as_ref(), b"rec:item:v7");
}

#[test]
fn test_dedup_keeps_tombstone() {
    // Newest entry for the key is a Delete — it should still be returned
    let source0 = vec![make_delete("rec", "item", 10)];
    let source1 = vec![make_put("rec", "item", 5)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_key(&results[0], "rec", "item");
    assert_eq!(results[0].entry_type, EntryType::Delete);
    assert!(results[0].is_tombstone());
    assert_eq!(results[0].sequence_number, 10);
}

#[test]
fn test_dedup_skips_older_put_after_delete() {
    // Delete at seq 10 in source 0, Put at seq 5 in source 1
    // Only the Delete should be returned by dedup
    let source0 = vec![make_delete("rec", "item", 10)];
    let source1 = vec![make_put("rec", "item", 5)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entry_type, EntryType::Delete);
    assert_eq!(results[0].sequence_number, 10);
}

#[test]
fn test_dedup_multiple_duplicates() {
    // Same key in 5 sources with decreasing sequence numbers
    let sources: Vec<Box<dyn flushdb_engine::MergeSource>> = (0..5)
        .map(|i| {
            let seq = 50 - (i as u64 * 10); // 50, 40, 30, 20, 10
            make_source(vec![make_put("rec", "item", seq)], i)
        })
        .collect();

    let mut iter = MergeIterator::new(sources);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_key(&results[0], "rec", "item");
    assert_eq!(results[0].sequence_number, 50);
}

#[test]
fn test_dedup_range_delete_kept_as_winner() {
    // RangeDelete in source 0 should be kept, Put in source 1 skipped
    let source0 = vec![make_range_delete("rec", "item", 15)];
    let source1 = vec![make_put("rec", "item", 3)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entry_type, EntryType::RangeDelete);
    assert!(results[0].is_tombstone());
    assert!(!results[0].is_put());
}

// =============================================================================
// Sort Order Tests
// =============================================================================

#[test]
fn test_merge_preserves_composite_key_order() {
    let source0 = vec![
        make_put("alpha", "z", 1),
        make_put("gamma", "a", 3),
    ];
    let source1 = vec![
        make_put("beta", "m", 2),
        make_put("delta", "x", 4),
    ];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 4);

    // Verify strictly ascending CompositeKey order
    for i in 1..results.len() {
        assert!(
            results[i - 1].composite_key < results[i].composite_key,
            "entry {} should be < entry {}: {:?} vs {:?}",
            i - 1,
            i,
            results[i - 1].composite_key,
            results[i].composite_key
        );
    }

    assert_key(&results[0], "alpha", "z");
    assert_key(&results[1], "beta", "m");
    assert_key(&results[2], "delta", "x");
    assert_key(&results[3], "gamma", "a");
}

#[test]
fn test_merge_same_record_different_items() {
    // Items within the same record should be sorted by item_key
    let source0 = vec![
        make_put("rec", "apple", 1),
        make_put("rec", "cherry", 3),
    ];
    let source1 = vec![
        make_put("rec", "banana", 2),
        make_put("rec", "date", 4),
    ];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 4);
    assert_key(&results[0], "rec", "apple");
    assert_key(&results[1], "rec", "banana");
    assert_key(&results[2], "rec", "cherry");
    assert_key(&results[3], "rec", "date");
}

#[test]
fn test_merge_cross_record_ordering() {
    // All items for "aaa" must come before any item for "aab"
    let source0 = vec![
        make_put("aaa", "z", 1),
        make_put("aab", "a", 3),
    ];
    let source1 = vec![
        make_put("aaa", "m", 2),
        make_put("aab", "b", 4),
    ];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 4);
    assert_key(&results[0], "aaa", "m");
    assert_key(&results[1], "aaa", "z");
    assert_key(&results[2], "aab", "a");
    assert_key(&results[3], "aab", "b");
}

// =============================================================================
// Multi-Source Tests
// =============================================================================

#[test]
fn test_merge_five_sources() {
    // Simulates: active memtable (0), frozen memtable (1), 3 L0 SSTables (2,3,4)
    // Each source has unique keys and some overlapping keys
    let source0 = vec![
        make_put("user", "email", 50),
        make_put("user", "name", 48),
    ];
    let source1 = vec![
        make_put("user", "age", 40),
        make_put("user", "name", 38),
    ];
    let source2 = vec![
        make_put("order", "item1", 30),
        make_put("user", "name", 28),
    ];
    let source3 = vec![
        make_put("order", "item2", 20),
        make_put("user", "phone", 18),
    ];
    let source4 = vec![
        make_put("config", "setting1", 10),
        make_put("user", "name", 8),
    ];

    let mut iter = MergeIterator::new(vec![
        make_source(source0, 0),
        make_source(source1, 1),
        make_source(source2, 2),
        make_source(source3, 3),
        make_source(source4, 4),
    ]);

    let results = collect_deduped(&mut iter);

    // Unique keys: config/setting1, order/item1, order/item2,
    //              user/age, user/email, user/name, user/phone
    assert_eq!(results.len(), 7);

    assert_key(&results[0], "config", "setting1");
    assert_key(&results[1], "order", "item1");
    assert_key(&results[2], "order", "item2");
    assert_key(&results[3], "user", "age");
    assert_key(&results[4], "user", "email");
    assert_key(&results[5], "user", "name");
    assert_key(&results[6], "user", "phone");

    // user/name should come from source 0 (seq 48, highest)
    assert_eq!(results[5].sequence_number, 48);
}

#[test]
fn test_merge_large_dataset() {
    // 10 sources x 100 entries each = 1000 total entries, all unique keys
    let sources: Vec<Box<dyn flushdb_engine::MergeSource>> = (0..10)
        .map(|src_id| {
            let entries: Vec<MergeEntry> = (0..100)
                .map(|i| {
                    let record_id = format!("r{:04}", src_id * 100 + i);
                    make_put(&record_id, "data", (src_id * 100 + i) as u64)
                })
                .collect();
            make_source(entries, src_id)
        })
        .collect();

    let mut iter = MergeIterator::new(sources);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 1000);

    // Verify strict ordering
    for i in 1..results.len() {
        assert!(
            results[i - 1].composite_key < results[i].composite_key,
            "ordering violated at index {i}"
        );
    }
}

#[test]
fn test_merge_large_dataset_with_overlapping_keys() {
    // 10 sources x 100 entries, but all sources share the same key space
    // Dedup should reduce to 100 unique keys
    let sources: Vec<Box<dyn flushdb_engine::MergeSource>> = (0..10)
        .map(|src_id| {
            let entries: Vec<MergeEntry> = (0..100)
                .map(|i| {
                    let record_id = format!("r{i:04}");
                    let seq = (1000 - src_id * 100 + i) as u64; // source 0 has highest seqs
                    make_put(&record_id, "data", seq)
                })
                .collect();
            make_source(entries, src_id)
        })
        .collect();

    let mut iter = MergeIterator::new(sources);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 100);

    // All winners should come from source 0 (lowest source_id = newest)
    for i in 0..100 {
        let expected_record = format!("r{i:04}");
        assert_key(&results[i], &expected_record, "data");
    }
}

#[test]
fn test_merge_all_same_key() {
    // Every source has the exact same key
    let sources: Vec<Box<dyn flushdb_engine::MergeSource>> = (0..10)
        .map(|src_id| {
            let seq = (100 - src_id * 10) as u64;
            make_source(vec![make_put("only", "key", seq)], src_id)
        })
        .collect();

    let mut iter = MergeIterator::new(sources);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_key(&results[0], "only", "key");
    assert_eq!(results[0].sequence_number, 100);
    assert!(iter.is_exhausted());
}

#[test]
fn test_merge_all_same_key_raw() {
    // Without dedup, all 10 entries should be returned
    let sources: Vec<Box<dyn flushdb_engine::MergeSource>> = (0..10)
        .map(|src_id| {
            let seq = (100 - src_id * 10) as u64;
            make_source(vec![make_put("only", "key", seq)], src_id)
        })
        .collect();

    let mut iter = MergeIterator::new(sources);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 10);

    // All returned, lowest source_id first
    assert_eq!(results[0].sequence_number, 100); // source 0
    assert_eq!(results[9].sequence_number, 10);  // source 9
}

// =============================================================================
// Edge Cases
// =============================================================================

#[test]
fn test_merge_single_entry_per_source() {
    let sources: Vec<Box<dyn flushdb_engine::MergeSource>> = vec![
        make_source(vec![make_put("c", "k", 1)], 0),
        make_source(vec![make_put("a", "k", 2)], 1),
        make_source(vec![make_put("b", "k", 3)], 2),
    ];

    let mut iter = MergeIterator::new(sources);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 3);
    assert_key(&results[0], "a", "k");
    assert_key(&results[1], "b", "k");
    assert_key(&results[2], "c", "k");
}

#[test]
fn test_merge_iterator_next_after_exhaustion() {
    let mut iter = MergeIterator::new(vec![make_source(vec![make_put("a", "k", 1)], 0)]);

    assert!(iter.next_entry().is_some());
    assert!(iter.is_exhausted());

    // Repeated calls after exhaustion should return None
    assert!(iter.next_entry().is_none());
    assert!(iter.next_entry().is_none());
    assert!(iter.next_deduped().is_none());
    assert!(iter.next_deduped().is_none());
}

#[test]
fn test_merge_entry_from_memtable_entry() {
    let key = CompositeKey::new(b"record1", b"item1").unwrap();
    let memtable_entry = MemtableEntry::with_sequence(
        key.clone(),
        Bytes::from("hello world"),
        Bytes::from("meta123"),
        IdempotencyToken::none(),
        42,
        EntryType::Put,
    );

    let merge_entry = MergeEntry::from_memtable_entry(&memtable_entry);

    assert_eq!(merge_entry.composite_key, key);
    assert_eq!(merge_entry.value.as_ref(), b"hello world");
    assert_eq!(merge_entry.metadata.as_ref(), b"meta123");
    assert_eq!(merge_entry.sequence_number, 42);
    assert_eq!(merge_entry.entry_type, EntryType::Put);
    assert!(merge_entry.is_put());
    assert!(!merge_entry.is_tombstone());
}

#[test]
fn test_merge_entry_from_memtable_entry_delete() {
    let key = CompositeKey::new(b"rec", b"item").unwrap();
    let memtable_entry = MemtableEntry::with_sequence(
        key.clone(),
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::none(),
        99,
        EntryType::Delete,
    );

    let merge_entry = MergeEntry::from_memtable_entry(&memtable_entry);

    assert_eq!(merge_entry.entry_type, EntryType::Delete);
    assert!(merge_entry.is_tombstone());
    assert!(!merge_entry.is_put());
    assert_eq!(merge_entry.sequence_number, 99);
}

#[test]
fn test_merge_entry_from_block_entry_inline() {
    let key = CompositeKey::new(b"block_rec", b"block_item").unwrap();
    let block_entry = BlockEntry {
        composite_key: key.clone(),
        value: EntryValue::Inline(Bytes::from("inline_data")),
        metadata: Bytes::from("block_meta"),
        entry_type: EntryType::Put,
        sequence_number: 77,
    };

    let merge_entry = MergeEntry::from_block_entry(block_entry);

    assert_eq!(merge_entry.composite_key, key);
    assert_eq!(merge_entry.value.as_ref(), b"inline_data");
    assert_eq!(merge_entry.metadata.as_ref(), b"block_meta");
    assert_eq!(merge_entry.sequence_number, 77);
    assert_eq!(merge_entry.entry_type, EntryType::Put);
}

#[test]
fn test_merge_entry_from_block_entry_blob_ref() {
    let key = CompositeKey::new(b"blob_rec", b"blob_item").unwrap();
    let block_entry = BlockEntry {
        composite_key: key.clone(),
        value: EntryValue::BlobRef {
            blob_id: Bytes::from("blob123"),
            offset: 1024,
            size: 4096,
        },
        metadata: Bytes::from("blob_meta"),
        entry_type: EntryType::Put,
        sequence_number: 55,
    };

    let merge_entry = MergeEntry::from_block_entry(block_entry);

    assert_eq!(merge_entry.entry_type, EntryType::Put);
    assert_eq!(merge_entry.composite_key, key);
    // BlobRef conversion produces empty value bytes. This is intentional for
    // pre-blob-support phases; actual blob reads will be resolved in future phases.
    assert!(merge_entry.value.is_empty());
    assert_eq!(merge_entry.metadata.as_ref(), b"blob_meta");
    assert_eq!(merge_entry.sequence_number, 55);
}

// =============================================================================
// Dedup with Mixed Entry Types
// =============================================================================

#[test]
fn test_dedup_interleaved_puts_and_deletes() {
    // Multiple keys across two sources, mixing puts and deletes
    let source0 = vec![
        make_put("a", "k1", 10),
        make_delete("b", "k1", 20),
        make_put("c", "k1", 30),
    ];
    let source1 = vec![
        make_delete("a", "k1", 5),
        make_put("b", "k1", 15),
        make_delete("c", "k1", 25),
    ];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 3);

    // a/k1: source 0 wins (seq 10, Put)
    assert_key(&results[0], "a", "k1");
    assert_eq!(results[0].entry_type, EntryType::Put);
    assert_eq!(results[0].sequence_number, 10);

    // b/k1: source 0 wins (seq 20, Delete)
    assert_key(&results[1], "b", "k1");
    assert_eq!(results[1].entry_type, EntryType::Delete);
    assert_eq!(results[1].sequence_number, 20);

    // c/k1: source 0 wins (seq 30, Put)
    assert_key(&results[2], "c", "k1");
    assert_eq!(results[2].entry_type, EntryType::Put);
    assert_eq!(results[2].sequence_number, 30);
}

#[test]
fn test_dedup_preserves_non_overlapping_keys() {
    // Dedup should not affect keys that only appear in one source
    let source0 = vec![
        make_put("a", "k1", 10),
        make_put("c", "k1", 30),
    ];
    let source1 = vec![
        make_put("b", "k1", 20),
        make_put("d", "k1", 40),
    ];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 4);
    assert_key(&results[0], "a", "k1");
    assert_key(&results[1], "b", "k1");
    assert_key(&results[2], "c", "k1");
    assert_key(&results[3], "d", "k1");
}

// =============================================================================
// next_entry vs next_deduped Behavior
// =============================================================================

#[test]
fn test_next_entry_returns_all_duplicates() {
    // next_entry should return ALL entries, including duplicates
    let source0 = vec![make_put("rec", "item", 10)];
    let source1 = vec![make_put("rec", "item", 5)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 2);
    // Source 0 (lower source_id) comes first
    assert_eq!(results[0].sequence_number, 10);
    assert_eq!(results[1].sequence_number, 5);
}

#[test]
fn test_next_entry_ordering_for_same_key() {
    // For same key, lower source_id entries come first in next_entry
    let source0 = vec![make_put("rec", "item", 100)];
    let source1 = vec![make_put("rec", "item", 200)];
    let source2 = vec![make_put("rec", "item", 50)];

    let mut iter = MergeIterator::new(vec![
        make_source(source0, 0),
        make_source(source1, 1),
        make_source(source2, 2),
    ]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 3);
    // Ordered by source_id when key is equal
    assert_eq!(results[0].sequence_number, 100); // source 0
    assert_eq!(results[1].sequence_number, 200); // source 1
    assert_eq!(results[2].sequence_number, 50);  // source 2
}

// =============================================================================
// is_exhausted behavior
// =============================================================================

#[test]
fn test_is_exhausted_empty_iterator() {
    let iter = MergeIterator::new(vec![]);
    assert!(iter.is_exhausted());
}

#[test]
fn test_is_exhausted_transitions() {
    let mut iter = MergeIterator::new(vec![
        make_source(vec![make_put("a", "k", 1)], 0),
        make_source(vec![make_put("b", "k", 2)], 1),
    ]);

    assert!(!iter.is_exhausted());
    iter.next_entry();
    assert!(!iter.is_exhausted());
    iter.next_entry();
    assert!(iter.is_exhausted());
}

// =============================================================================
// Complex Multi-Source Scenarios
// =============================================================================

#[test]
fn test_dedup_with_gaps_between_sources() {
    // Source 0: keys a, c, e
    // Source 1: keys b, c, d
    // Source 2: keys a, d, f
    // After dedup: a(s0), b(s1), c(s0), d(s1), e(s0), f(s2)
    let source0 = vec![
        make_put("a", "k", 30),
        make_put("c", "k", 28),
        make_put("e", "k", 26),
    ];
    let source1 = vec![
        make_put("b", "k", 20),
        make_put("c", "k", 18),
        make_put("d", "k", 16),
    ];
    let source2 = vec![
        make_put("a", "k", 10),
        make_put("d", "k", 8),
        make_put("f", "k", 6),
    ];

    let mut iter = MergeIterator::new(vec![
        make_source(source0, 0),
        make_source(source1, 1),
        make_source(source2, 2),
    ]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 6);

    assert_key(&results[0], "a", "k");
    assert_eq!(results[0].sequence_number, 30); // from source 0

    assert_key(&results[1], "b", "k");
    assert_eq!(results[1].sequence_number, 20); // only in source 1

    assert_key(&results[2], "c", "k");
    assert_eq!(results[2].sequence_number, 28); // from source 0

    assert_key(&results[3], "d", "k");
    assert_eq!(results[3].sequence_number, 16); // from source 1

    assert_key(&results[4], "e", "k");
    assert_eq!(results[4].sequence_number, 26); // only in source 0

    assert_key(&results[5], "f", "k");
    assert_eq!(results[5].sequence_number, 6); // only in source 2
}

#[test]
fn test_merge_five_sources_mixed_entry_types() {
    // Realistic scenario: active memtable has a delete, frozen memtable has a put,
    // older SSTables have puts — dedup should keep the delete from source 0
    let source0 = vec![make_delete("user", "session", 50)];
    let source1 = vec![make_put("user", "session", 40)];
    let source2 = vec![make_put("user", "session", 30)];
    let source3 = vec![make_put("user", "session", 20)];
    let source4 = vec![make_put("user", "session", 10)];

    let mut iter = MergeIterator::new(vec![
        make_source(source0, 0),
        make_source(source1, 1),
        make_source(source2, 2),
        make_source(source3, 3),
        make_source(source4, 4),
    ]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].entry_type, EntryType::Delete);
    assert_eq!(results[0].sequence_number, 50);
}

// =============================================================================
// Empty Item Key and Various Key Shapes
// =============================================================================

#[test]
fn test_merge_with_empty_item_key() {
    let source0 = vec![MergeEntry {
        composite_key: CompositeKey::from_record_only(b"rec").unwrap(),
        value: Bytes::from("val"),
        metadata: Bytes::new(),
        entry_type: EntryType::Put,
        sequence_number: 1,
    }];
    let source1 = vec![make_put("rec", "item", 2)];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_all(&mut iter);
    assert_eq!(results.len(), 2);
    // Empty item_key sorts before "item" (empty byte slice < non-empty)
    assert!(results[0].composite_key.is_empty_item_key());
    assert_key(&results[1], "rec", "item");
}

#[test]
fn test_merge_no_sources() {
    let mut iter = MergeIterator::new(vec![]);
    assert!(iter.is_exhausted());
    assert!(iter.next_entry().is_none());
    assert!(iter.next_deduped().is_none());
}

#[test]
fn test_merge_alternating_dedup_and_unique() {
    // Pattern: shared key, unique, shared key, unique, shared key
    let source0 = vec![
        make_put("a", "k", 10),
        make_put("c", "k", 8),
        make_put("e", "k", 6),
    ];
    let source1 = vec![
        make_put("a", "k", 5),
        make_put("b", "k", 9),
        make_put("c", "k", 3),
        make_put("d", "k", 7),
        make_put("e", "k", 1),
    ];

    let mut iter = MergeIterator::new(vec![make_source(source0, 0), make_source(source1, 1)]);

    let results = collect_deduped(&mut iter);
    assert_eq!(results.len(), 5);
    assert_key(&results[0], "a", "k");
    assert_eq!(results[0].sequence_number, 10);
    assert_key(&results[1], "b", "k");
    assert_eq!(results[1].sequence_number, 9);
    assert_key(&results[2], "c", "k");
    assert_eq!(results[2].sequence_number, 8);
    assert_key(&results[3], "d", "k");
    assert_eq!(results[3].sequence_number, 7);
    assert_key(&results[4], "e", "k");
    assert_eq!(results[4].sequence_number, 6);
}

// === VecSource Direct Tests ===

#[test]
fn test_vec_source_peek_on_empty_returns_none() {
    let source = VecSource::new(vec![], 42);
    assert!(source.peek().is_none());
}

#[test]
fn test_vec_source_advance_past_end_does_not_panic() {
    let mut source = VecSource::new(vec![make_put("a", "k", 1)], 0);
    source.advance();
    assert!(source.peek().is_none());
    // Advancing again past the end should not panic
    source.advance();
    source.advance();
    assert!(source.peek().is_none());
}

#[test]
fn test_vec_source_preserves_source_id() {
    let source = VecSource::new(vec![], 99);
    assert_eq!(source.source_id(), 99);

    let source2 = VecSource::new(vec![make_put("a", "k", 1)], 7);
    assert_eq!(source2.source_id(), 7);
}

// === MergeEntry::from_skip_node Tests ===

#[test]
fn test_merge_entry_from_skip_node() {
    let mut mt = Memtable::new(MemtableConfig::default(), 1);
    mt.insert(MemtableEntry::new(
        CompositeKey::new(b"rec1", b"key1").unwrap(),
        Bytes::from("value1"),
        Bytes::from("meta1"),
        IdempotencyToken::none(),
        EntryType::Put,
    ))
    .unwrap();
    mt.freeze();

    let skiplist = mt.into_skiplist();
    let node = skiplist.iter().next().expect("should have one node");
    let merge_entry = MergeEntry::from_skip_node(node);

    assert_eq!(merge_entry.composite_key, CompositeKey::new(b"rec1", b"key1").unwrap());
    assert_eq!(merge_entry.value, Bytes::from("value1"));
    assert_eq!(merge_entry.metadata, Bytes::from("meta1"));
    assert_eq!(merge_entry.entry_type, EntryType::Put);
    assert_eq!(merge_entry.sequence_number, 1);
}

#[test]
fn test_merge_entry_from_skip_node_delete() {
    let mut mt = Memtable::new(MemtableConfig::default(), 1);
    mt.insert(MemtableEntry::new(
        CompositeKey::new(b"rec1", b"key1").unwrap(),
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Delete,
    ))
    .unwrap();
    mt.freeze();

    let skiplist = mt.into_skiplist();
    let node = skiplist.iter().next().unwrap();
    let merge_entry = MergeEntry::from_skip_node(node);

    assert_eq!(merge_entry.entry_type, EntryType::Delete);
    assert!(merge_entry.is_tombstone());
}
