use bytes::Bytes;
use flushdb_engine::memtable::MemtableConfig;
use flushdb_engine::read_path::{
    GetResult, PageToken, RangeReadOptions, RangeTombstoneCollector, ReadPath,
};
use flushdb_engine::{DirectBlockFetcher, MemtableList, MergeEntry};
use flushdb_types::{CompositeKey, EntryType, IdempotencyToken, LocalFsBackend, MemtableEntry};

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
    let key = CompositeKey::range_tombstone_key(record.as_bytes(), start_key.as_bytes()).unwrap();
    MemtableEntry::new(
        key,
        Bytes::from(end_key.to_string()),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::RangeDelete,
    )
}

fn new_list() -> MemtableList {
    MemtableList::new(MemtableConfig::default(), 1)
}

fn test_fetcher() -> (tempfile::TempDir, DirectBlockFetcher<LocalFsBackend>) {
    let dir = tempfile::TempDir::new().unwrap();
    let backend = LocalFsBackend::new(dir.path().to_path_buf());
    (dir, DirectBlockFetcher::new(backend))
}

// === PageToken encode/decode Tests ===

#[test]
fn test_page_token_encode_decode_round_trip() {
    let key = CompositeKey::new(b"rec1", b"item42").unwrap();
    let token = PageToken {
        last_composite_key: key.clone(),
        last_sequence_number: 12345,
        avg_item_size_bytes: None,
    };

    let encoded = token.encode();
    let decoded = PageToken::decode(&encoded).unwrap();

    assert_eq!(decoded.last_composite_key, key);
    assert_eq!(decoded.last_sequence_number, 12345);
}

#[test]
fn test_page_token_decode_too_short_errors() {
    let short_data = vec![0u8; 4]; // less than 12 bytes
    let result = PageToken::decode(&short_data);
    assert!(result.is_err());
}

#[test]
fn test_page_token_decode_truncated_errors() {
    // key_len says 100 but only a few bytes follow
    let mut data = Vec::new();
    data.extend_from_slice(&100u32.to_le_bytes()); // key_len = 100
    data.extend_from_slice(&[0u8; 10]); // only 10 bytes, not 100 + 8
    let result = PageToken::decode(&data);
    assert!(result.is_err());
}

#[test]
fn test_page_token_base64_round_trip() {
    let key = CompositeKey::new(b"rec1", b"item42").unwrap();
    let token = PageToken {
        last_composite_key: key.clone(),
        last_sequence_number: 999,
        avg_item_size_bytes: None,
    };

    let b64 = token.to_base64();
    let decoded = PageToken::from_base64(&b64).unwrap();

    assert_eq!(decoded.last_composite_key, key);
    assert_eq!(decoded.last_sequence_number, 999);
}

#[test]
fn test_page_token_from_base64_invalid_length_errors() {
    let result = PageToken::from_base64("abc"); // not multiple of 4
    assert!(result.is_err());
}

// === RangeTombstoneCollector Tests ===

#[test]
fn test_collector_covers_key_in_range() {
    let mut collector = RangeTombstoneCollector::new();
    collector.add(b"rec1", b"a", b"m", 10);

    assert!(collector.covers(b"rec1", b"b", 5));
    assert!(collector.covers(b"rec1", b"a", 5));
}

#[test]
fn test_collector_does_not_cover_newer_entry() {
    let mut collector = RangeTombstoneCollector::new();
    collector.add(b"rec1", b"a", b"m", 10);

    // Entry with seq=15 is newer than tombstone seq=10
    assert!(!collector.covers(b"rec1", b"b", 15));
}

#[test]
fn test_collector_does_not_cover_outside_range() {
    let mut collector = RangeTombstoneCollector::new();
    collector.add(b"rec1", b"a", b"m", 10);

    assert!(!collector.covers(b"rec1", b"z", 5));
}

#[test]
fn test_collector_does_not_cover_different_record() {
    let mut collector = RangeTombstoneCollector::new();
    collector.add(b"rec1", b"a", b"z", 10);

    assert!(!collector.covers(b"rec2", b"b", 5));
}

#[test]
fn test_collector_add_from_memtable_list() {
    let mut list = new_list();
    list.insert(make_range_delete("r1", "a", "z")).unwrap();

    let mut collector = RangeTombstoneCollector::new();
    collector.add_from_memtable_list(&list);

    assert!(collector.covers(b"r1", b"m", 0));
}

// === GetResult Tests ===

#[test]
fn test_get_result_from_merge_entry() {
    let key = CompositeKey::new(b"rec1", b"item1").unwrap();
    let entry = MergeEntry {
        composite_key: key.clone(),
        value: Bytes::from("val"),
        metadata: Bytes::from("meta"),
        sequence_number: 42,
        entry_type: EntryType::Put,
    };

    let result = GetResult::from_merge_entry(&entry);
    assert_eq!(result.key, key);
    assert_eq!(result.value, Bytes::from("val"));
    assert_eq!(result.metadata, Bytes::from("meta"));
    assert_eq!(result.sequence_number, 42);
}

// === ReadPath point_read Tests (memtable only, no SSTables) ===

#[tokio::test]
async fn test_point_read_hit_from_memtable() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let result = read_path.point_read(&key, &list, &[]).await.unwrap();

    assert!(result.is_some());
    assert_eq!(result.unwrap().value, Bytes::from("v1"));
}

#[tokio::test]
async fn test_point_read_miss_returns_none() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let list = new_list();
    let key = CompositeKey::new(b"r1", b"missing").unwrap();
    let result = read_path.point_read(&key, &list, &[]).await.unwrap();

    assert!(result.is_none());
}

#[tokio::test]
async fn test_point_read_tombstone_returns_none() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    list.insert(make_put("r1", "k1", "v1")).unwrap();
    list.insert(make_delete("r1", "k1")).unwrap();

    let key = CompositeKey::new(b"r1", b"k1").unwrap();
    let result = read_path.point_read(&key, &list, &[]).await.unwrap();

    assert!(result.is_none());
}

#[tokio::test]
async fn test_point_read_range_tombstone_suppresses_result() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    list.insert(make_put("r1", "b", "v")).unwrap();
    list.insert(make_range_delete("r1", "a", "z")).unwrap();

    let key = CompositeKey::new(b"r1", b"b").unwrap();
    let result = read_path.point_read(&key, &list, &[]).await.unwrap();

    assert!(result.is_none());
}

// === ReadPath range_read Tests (memtable only) ===

#[tokio::test]
async fn test_range_read_basic() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    for i in 0..5 {
        list.insert(make_put("r1", &format!("k{i}"), &format!("v{i}")))
            .unwrap();
    }

    let result = read_path
        .range_read(b"r1", None, None, RangeReadOptions::default(), &list, &[])
        .await
        .unwrap();

    assert_eq!(result.entries.len(), 5);
}

#[tokio::test]
async fn test_range_read_bounded() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    for i in 0..10 {
        list.insert(make_put("r1", &format!("k{i:02}"), &format!("v{i}")))
            .unwrap();
    }

    let result = read_path
        .range_read(
            b"r1",
            Some(b"k02"),
            Some(b"k05"),
            RangeReadOptions::default(),
            &list,
            &[],
        )
        .await
        .unwrap();

    assert_eq!(result.entries.len(), 3); // k02, k03, k04
}

#[tokio::test]
async fn test_range_read_item_limit() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    for i in 0..10 {
        list.insert(make_put("r1", &format!("k{i:02}"), &format!("v{i}")))
            .unwrap();
    }

    let options = RangeReadOptions {
        page_size_bytes: usize::MAX,
        item_limit: Some(3),
        resume_from: None,
    };

    let result = read_path
        .range_read(b"r1", None, None, options, &list, &[])
        .await
        .unwrap();

    assert_eq!(result.entries.len(), 3);
    assert!(result.next_page_token.is_some());
    assert!(
        result.is_partial,
        "paginated result should be marked partial"
    );
}

#[tokio::test]
async fn test_range_read_is_partial_false_when_complete() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    for i in 0..3 {
        list.insert(make_put("r1", &format!("k{i:02}"), &format!("v{i}")))
            .unwrap();
    }

    let result = read_path
        .range_read(b"r1", None, None, RangeReadOptions::default(), &list, &[])
        .await
        .unwrap();

    assert_eq!(result.entries.len(), 3);
    assert!(result.next_page_token.is_none());
    assert!(
        !result.is_partial,
        "complete result should not be marked partial"
    );
}

#[tokio::test]
async fn test_range_read_tombstone_excluded() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    list.insert(make_put("r1", "a", "v1")).unwrap();
    list.insert(make_put("r1", "b", "v2")).unwrap();
    list.insert(make_delete("r1", "a")).unwrap();

    let result = read_path
        .range_read(b"r1", None, None, RangeReadOptions::default(), &list, &[])
        .await
        .unwrap();

    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].value, Bytes::from("v2"));
}

#[tokio::test]
async fn test_range_read_range_tombstone_excluded() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    list.insert(make_put("r1", "a", "v1")).unwrap();
    list.insert(make_put("r1", "b", "v2")).unwrap();
    list.insert(make_put("r1", "c", "v3")).unwrap();
    list.insert(make_range_delete("r1", "a", "c")).unwrap();

    let result = read_path
        .range_read(b"r1", None, None, RangeReadOptions::default(), &list, &[])
        .await
        .unwrap();

    // Only "c" should remain (range delete covers [a, c) )
    assert_eq!(result.entries.len(), 1);
    assert_eq!(result.entries[0].value, Bytes::from("v3"));
}

// === ReadPath multi_get Tests ===

#[tokio::test]
async fn test_multi_get_mixed_hits_and_misses() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    list.insert(make_put("r1", "a", "va")).unwrap();
    list.insert(make_put("r1", "c", "vc")).unwrap();

    let keys: Vec<&[u8]> = vec![b"a", b"b", b"c"];
    let results = read_path.multi_get(b"r1", &keys, &list, &[]).await.unwrap();

    assert_eq!(results.len(), 3);
    assert!(results[0].is_some());
    assert_eq!(results[0].as_ref().unwrap().value, Bytes::from("va"));
    assert!(results[1].is_none());
    assert!(results[2].is_some());
    assert_eq!(results[2].as_ref().unwrap().value, Bytes::from("vc"));
}

// === ReadPath pagination resume Tests ===

#[tokio::test]
async fn test_range_read_pagination_resume() {
    let (_dir, fetcher) = test_fetcher();
    let read_path = ReadPath::new(&fetcher);

    let mut list = new_list();
    for i in 0..10 {
        list.insert(make_put("r1", &format!("k{i:02}"), &format!("v{i}")))
            .unwrap();
    }

    // First page: 3 items
    let options = RangeReadOptions {
        page_size_bytes: usize::MAX,
        item_limit: Some(3),
        resume_from: None,
    };
    let page1 = read_path
        .range_read(b"r1", None, None, options, &list, &[])
        .await
        .unwrap();
    assert_eq!(page1.entries.len(), 3);
    assert!(page1.next_page_token.is_some());

    // Second page: resume
    let options2 = RangeReadOptions {
        page_size_bytes: usize::MAX,
        item_limit: Some(3),
        resume_from: page1.next_page_token,
    };
    let page2 = read_path
        .range_read(b"r1", None, None, options2, &list, &[])
        .await
        .unwrap();
    assert_eq!(page2.entries.len(), 3);

    // Verify no overlap between pages
    let page1_keys: Vec<_> = page1
        .entries
        .iter()
        .map(|e| e.composite_key.item_key().to_vec())
        .collect();
    for entry in &page2.entries {
        assert!(
            !page1_keys.contains(&entry.composite_key.item_key().to_vec()),
            "page2 should not contain keys from page1"
        );
    }
}
