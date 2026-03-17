use flushdb_engine::manifest::types::{
    l0_sst_path, manifest_path, run_fragment_path, Level, Manifest, ManifestConfig, ManifestId,
    ManifestUpdate, ManifestUpdateTrigger, SSTableMeta,
};
use flushdb_engine::sstable::SstInfo;
use flushdb_engine::sstable::types::CompressionType;
use flushdb_types::CompositeKey;

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

fn make_composite_key(record_id: &[u8], item_key: &[u8]) -> CompositeKey {
    CompositeKey::new(record_id, item_key).expect("valid composite key")
}

fn make_sstable_meta(id: &str, min_key: &CompositeKey, max_key: &CompositeKey) -> SSTableMeta {
    SSTableMeta {
        id: id.to_string(),
        size_bytes: 4096,
        entry_count: 100,
        min_key: min_key.as_bytes().to_vec(),
        max_key: max_key.as_bytes().to_vec(),
        bloom_filter_offset: 2048,
        bloom_filter_size: 256,
        index_offset: 2304,
        index_size: 128,
        created_at_ms: 1700000000000,
        sequence_range: (1, 100),
        record_id_count: 50,
        run_id: None,
        fragment_index: None,
        dedup_block_size: 64,
    }
}

fn make_sstable_meta_with_size(
    id: &str,
    min_key: &CompositeKey,
    max_key: &CompositeKey,
    size_bytes: u64,
) -> SSTableMeta {
    let mut meta = make_sstable_meta(id, min_key, max_key);
    meta.size_bytes = size_bytes;
    meta
}

fn make_run_fragment_meta(
    id: &str,
    min_key: &CompositeKey,
    max_key: &CompositeKey,
    run_id: &str,
    fragment_index: u32,
) -> SSTableMeta {
    let mut meta = make_sstable_meta(id, min_key, max_key);
    meta.run_id = Some(run_id.to_string());
    meta.fragment_index = Some(fragment_index);
    meta
}

// ---------------------------------------------------------------------------
// ManifestId tests
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_id_to_path_string() {
    let id = ManifestId::new(42);
    assert_eq!(id.to_path_string(), "00000000000000000042");
}

#[test]
fn test_manifest_id_to_path_string_zero() {
    assert_eq!(ManifestId::ZERO.to_path_string(), "00000000000000000000");
}

#[test]
fn test_manifest_id_to_path_string_large() {
    let id = ManifestId::new(18446744073709551615); // u64::MAX
    assert_eq!(id.to_path_string(), "18446744073709551615");
}

#[test]
fn test_manifest_id_from_path_string() {
    let id = ManifestId::from_path_string("00000000000000000042").expect("valid path string");
    assert_eq!(id.as_u64(), 42);
}

#[test]
fn test_manifest_id_from_path_string_round_trip() {
    let original = ManifestId::new(12345);
    let path_str = original.to_path_string();
    let recovered = ManifestId::from_path_string(&path_str).expect("round-trip");
    assert_eq!(original, recovered);
}

#[test]
fn test_manifest_id_from_path_string_too_short() {
    let result = ManifestId::from_path_string("123");
    assert!(result.is_err());
}

#[test]
fn test_manifest_id_from_path_string_too_long() {
    let result = ManifestId::from_path_string("000000000000000000001");
    assert!(result.is_err());
}

#[test]
fn test_manifest_id_from_path_string_non_numeric() {
    let result = ManifestId::from_path_string("0000000000000000abcd");
    assert!(result.is_err());
}

#[test]
fn test_manifest_id_from_path_string_empty() {
    let result = ManifestId::from_path_string("");
    assert!(result.is_err());
}

#[test]
fn test_manifest_id_next() {
    let id = ManifestId::new(5);
    let next = id.next();
    assert_eq!(next.as_u64(), 6);
}

#[test]
fn test_manifest_id_next_from_zero() {
    let next = ManifestId::ZERO.next();
    assert_eq!(next.as_u64(), 1);
}

#[test]
fn test_manifest_id_ordering() {
    let a = ManifestId::new(1);
    let b = ManifestId::new(2);
    let c = ManifestId::new(100);
    assert!(a < b);
    assert!(b < c);
    assert!(a < c);
}

#[test]
fn test_manifest_id_ordering_matches_path_string_lexicographic_order() {
    let a = ManifestId::new(1);
    let b = ManifestId::new(2);
    let c = ManifestId::new(100);

    assert!(a.to_path_string() < b.to_path_string());
    assert!(b.to_path_string() < c.to_path_string());
    assert!(a.to_path_string() < c.to_path_string());
}

#[test]
fn test_manifest_id_equality() {
    let a = ManifestId::new(42);
    let b = ManifestId::new(42);
    assert_eq!(a, b);
}

#[test]
fn test_manifest_id_inequality() {
    let a = ManifestId::new(1);
    let b = ManifestId::new(2);
    assert_ne!(a, b);
}

#[test]
fn test_manifest_id_serde_roundtrip() {
    let id = ManifestId::new(42);
    let json = serde_json::to_string(&id).expect("serialize");
    assert_eq!(json, r#""00000000000000000042""#);

    let deserialized: ManifestId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(deserialized, id);
}

#[test]
fn test_manifest_id_serde_zero() {
    let id = ManifestId::ZERO;
    let json = serde_json::to_string(&id).expect("serialize");
    assert_eq!(json, r#""00000000000000000000""#);

    let deserialized: ManifestId = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(deserialized, id);
}

#[test]
fn test_manifest_id_serde_rejects_raw_integer() {
    let result = serde_json::from_str::<ManifestId>("42");
    assert!(result.is_err());
}

#[test]
fn test_manifest_id_zero_sentinel() {
    assert_eq!(ManifestId::ZERO.as_u64(), 0);
    assert_eq!(ManifestId::ZERO, ManifestId::new(0));
}

#[test]
fn test_manifest_id_display() {
    let id = ManifestId::new(7);
    let display = format!("{id}");
    assert_eq!(display, "00000000000000000007");
}

// ---------------------------------------------------------------------------
// Level tests
// ---------------------------------------------------------------------------

#[test]
fn test_level_ordering() {
    assert!(Level::L0 < Level::L1);
    assert!(Level::L1 < Level::L2);
    assert!(Level::L2 < Level::L3);
}

#[test]
fn test_level_next_l0() {
    assert_eq!(Level::L0.next(), Some(Level::L1));
}

#[test]
fn test_level_next_l1() {
    assert_eq!(Level::L1.next(), Some(Level::L2));
}

#[test]
fn test_level_next_l2() {
    assert_eq!(Level::L2.next(), Some(Level::L3));
}

#[test]
fn test_level_next_l3_is_none() {
    assert_eq!(Level::L3.next(), None);
}

#[test]
fn test_level_is_bottom() {
    assert!(!Level::L0.is_bottom());
    assert!(!Level::L1.is_bottom());
    assert!(!Level::L2.is_bottom());
    assert!(Level::L3.is_bottom());
}

#[test]
fn test_level_is_overlapping() {
    assert!(Level::L0.is_overlapping());
    assert!(!Level::L1.is_overlapping());
    assert!(!Level::L2.is_overlapping());
    assert!(!Level::L3.is_overlapping());
}

#[test]
fn test_level_max_size_bytes() {
    assert_eq!(Level::L0.max_size_bytes(), 0);
    assert_eq!(Level::L1.max_size_bytes(), 256 * 1024 * 1024);
    assert_eq!(Level::L2.max_size_bytes(), 2560 * 1024 * 1024);
    assert_eq!(Level::L3.max_size_bytes(), 25600 * 1024 * 1024);
}

#[test]
fn test_level_as_str() {
    assert_eq!(Level::L0.as_str(), "L0");
    assert_eq!(Level::L1.as_str(), "L1");
    assert_eq!(Level::L2.as_str(), "L2");
    assert_eq!(Level::L3.as_str(), "L3");
}

#[test]
fn test_level_from_str() {
    assert_eq!(Level::parse("L0").expect("L0"), Level::L0);
    assert_eq!(Level::parse("L1").expect("L1"), Level::L1);
    assert_eq!(Level::parse("L2").expect("L2"), Level::L2);
    assert_eq!(Level::parse("L3").expect("L3"), Level::L3);
}

#[test]
fn test_level_from_str_invalid() {
    assert!(Level::parse("L4").is_err());
    assert!(Level::parse("l0").is_err());
    assert!(Level::parse("").is_err());
    assert!(Level::parse("garbage").is_err());
}

#[test]
fn test_level_as_u8() {
    assert_eq!(Level::L0.as_u8(), 0);
    assert_eq!(Level::L1.as_u8(), 1);
    assert_eq!(Level::L2.as_u8(), 2);
    assert_eq!(Level::L3.as_u8(), 3);
}

#[test]
fn test_level_serde_roundtrip() {
    for level in Level::all() {
        let json = serde_json::to_string(level).expect("serialize");
        let expected = format!("\"{}\"", level.as_str());
        assert_eq!(json, expected, "level {level} should serialize as string");

        let deserialized: Level = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(deserialized, *level);
    }
}

#[test]
fn test_level_serde_rejects_integer() {
    let result = serde_json::from_str::<Level>("0");
    assert!(result.is_err());
}

#[test]
fn test_level_all() {
    let all = Level::all();
    assert_eq!(all.len(), 4);
    assert_eq!(all[0], Level::L0);
    assert_eq!(all[1], Level::L1);
    assert_eq!(all[2], Level::L2);
    assert_eq!(all[3], Level::L3);
}

#[test]
fn test_level_display() {
    assert_eq!(format!("{}", Level::L0), "L0");
    assert_eq!(format!("{}", Level::L3), "L3");
}

// ---------------------------------------------------------------------------
// SSTableMeta tests
// ---------------------------------------------------------------------------

#[test]
fn test_sstable_meta_serde_roundtrip() {
    let min_key = make_composite_key(b"aaa", b"key1");
    let max_key = make_composite_key(b"zzz", b"key9");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    let json = serde_json::to_string_pretty(&meta).expect("serialize");
    let deserialized: SSTableMeta = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(deserialized, meta);
    assert_eq!(deserialized.min_key, min_key.as_bytes());
    assert_eq!(deserialized.max_key, max_key.as_bytes());
}

#[test]
fn test_sstable_meta_serde_binary_keys_as_base64() {
    let min_key = make_composite_key(b"rec", b"\x01\x02\x03");
    let max_key = make_composite_key(b"rec", b"\xFE\xFF");
    let meta = make_sstable_meta("sst-bin", &min_key, &max_key);

    let json = serde_json::to_string(&meta).expect("serialize");
    // After deserialization, binary keys must survive intact
    let deserialized: SSTableMeta = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(deserialized.min_key, min_key.as_bytes());
    assert_eq!(deserialized.max_key, max_key.as_bytes());
}

#[test]
fn test_sstable_meta_serde_with_run_fragment() {
    let min_key = make_composite_key(b"a", b"");
    let max_key = make_composite_key(b"z", b"");
    let meta = make_run_fragment_meta("frag-001", &min_key, &max_key, "run-abc", 3);

    let json = serde_json::to_string(&meta).expect("serialize");
    let deserialized: SSTableMeta = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(deserialized.run_id, Some("run-abc".to_string()));
    assert_eq!(deserialized.fragment_index, Some(3));
    assert_eq!(deserialized, meta);
}

#[test]
fn test_sstable_meta_contains_key_within_range() {
    let min_key = make_composite_key(b"bbb", b"");
    let max_key = make_composite_key(b"ddd", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    let query = make_composite_key(b"ccc", b"");
    assert!(meta.contains_key(&query));
}

#[test]
fn test_sstable_meta_contains_key_at_min_boundary() {
    let min_key = make_composite_key(b"bbb", b"");
    let max_key = make_composite_key(b"ddd", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    assert!(meta.contains_key(&min_key));
}

#[test]
fn test_sstable_meta_contains_key_at_max_boundary() {
    let min_key = make_composite_key(b"bbb", b"");
    let max_key = make_composite_key(b"ddd", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    assert!(meta.contains_key(&max_key));
}

#[test]
fn test_sstable_meta_contains_key_below_range() {
    let min_key = make_composite_key(b"bbb", b"");
    let max_key = make_composite_key(b"ddd", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    let query = make_composite_key(b"aaa", b"");
    assert!(!meta.contains_key(&query));
}

#[test]
fn test_sstable_meta_contains_key_above_range() {
    let min_key = make_composite_key(b"bbb", b"");
    let max_key = make_composite_key(b"ddd", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    let query = make_composite_key(b"eee", b"");
    assert!(!meta.contains_key(&query));
}

#[test]
fn test_sstable_meta_overlaps_true() {
    let a_min = make_composite_key(b"aaa", b"");
    let a_max = make_composite_key(b"ccc", b"");
    let a = make_sstable_meta("sst-a", &a_min, &a_max);

    let b_min = make_composite_key(b"bbb", b"");
    let b_max = make_composite_key(b"ddd", b"");
    let b = make_sstable_meta("sst-b", &b_min, &b_max);

    assert!(a.overlaps(&b));
    assert!(b.overlaps(&a));
}

#[test]
fn test_sstable_meta_overlaps_disjoint() {
    let a_min = make_composite_key(b"aaa", b"");
    let a_max = make_composite_key(b"bbb", b"");
    let a = make_sstable_meta("sst-a", &a_min, &a_max);

    let b_min = make_composite_key(b"ddd", b"");
    let b_max = make_composite_key(b"eee", b"");
    let b = make_sstable_meta("sst-b", &b_min, &b_max);

    assert!(!a.overlaps(&b));
    assert!(!b.overlaps(&a));
}

#[test]
fn test_sstable_meta_overlaps_adjacent_touching() {
    // a's max == b's min: these should overlap since ranges are inclusive
    let a_min = make_composite_key(b"aaa", b"");
    let a_max = make_composite_key(b"bbb", b"");
    let a = make_sstable_meta("sst-a", &a_min, &a_max);

    let b_min = make_composite_key(b"bbb", b"");
    let b_max = make_composite_key(b"ccc", b"");
    let b = make_sstable_meta("sst-b", &b_min, &b_max);

    assert!(a.overlaps(&b));
    assert!(b.overlaps(&a));
}

#[test]
fn test_sstable_meta_overlaps_range() {
    let min_key = make_composite_key(b"bbb", b"");
    let max_key = make_composite_key(b"ddd", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    let start = make_composite_key(b"ccc", b"");
    let end = make_composite_key(b"eee", b"");
    assert!(meta.overlaps_range(start.as_bytes(), end.as_bytes()));
}

#[test]
fn test_sstable_meta_overlaps_range_no_overlap() {
    let min_key = make_composite_key(b"bbb", b"");
    let max_key = make_composite_key(b"ccc", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    let start = make_composite_key(b"ddd", b"");
    let end = make_composite_key(b"eee", b"");
    assert!(!meta.overlaps_range(start.as_bytes(), end.as_bytes()));
}

#[test]
fn test_sstable_meta_overlaps_range_contained() {
    let min_key = make_composite_key(b"aaa", b"");
    let max_key = make_composite_key(b"zzz", b"");
    let meta = make_sstable_meta("sst-001", &min_key, &max_key);

    let start = make_composite_key(b"ccc", b"");
    let end = make_composite_key(b"ddd", b"");
    assert!(meta.overlaps_range(start.as_bytes(), end.as_bytes()));
}

#[test]
fn test_sstable_meta_sst_path_l0() {
    let min_key = make_composite_key(b"a", b"");
    let max_key = make_composite_key(b"z", b"");
    let meta = make_sstable_meta("my-sst-id", &min_key, &max_key);

    let path = meta.sst_path("test-ns", Level::L0);
    assert_eq!(path, "flushdb/test-ns/sstables/L0/my-sst-id.sst");
}

#[test]
fn test_sstable_meta_sst_path_l1_no_fragment() {
    let min_key = make_composite_key(b"a", b"");
    let max_key = make_composite_key(b"z", b"");
    let meta = make_sstable_meta("plain-sst", &min_key, &max_key);

    let path = meta.sst_path("ns", Level::L1);
    assert_eq!(path, "flushdb/ns/sstables/L1/plain-sst.sst");
}

#[test]
fn test_sstable_meta_sst_path_l1_fragment() {
    let min_key = make_composite_key(b"a", b"");
    let max_key = make_composite_key(b"z", b"");
    let meta = make_run_fragment_meta("frag-001", &min_key, &max_key, "run-xyz", 0);

    let path = meta.sst_path("ns", Level::L1);
    assert_eq!(path, "flushdb/ns/sstables/L1/run-run-xyz/frag-0000.sst");
}

#[test]
fn test_sstable_meta_sst_path_l1_fragment_high_index() {
    let min_key = make_composite_key(b"a", b"");
    let max_key = make_composite_key(b"z", b"");
    let meta = make_run_fragment_meta("frag-999", &min_key, &max_key, "run-abc", 42);

    let path = meta.sst_path("prod", Level::L1);
    assert_eq!(path, "flushdb/prod/sstables/L1/run-run-abc/frag-0042.sst");
}

#[test]
fn test_sstable_meta_sst_path_l2() {
    let min_key = make_composite_key(b"a", b"");
    let max_key = make_composite_key(b"z", b"");
    let meta = make_sstable_meta("sst-l2", &min_key, &max_key);

    let path = meta.sst_path("ns", Level::L2);
    assert_eq!(path, "flushdb/ns/sstables/L2/sst-l2.sst");
}

#[test]
fn test_sstable_meta_from_sst_info() {
    let min_key = make_composite_key(b"rec-a", b"k1");
    let max_key = make_composite_key(b"rec-z", b"k9");

    let sst_info = SstInfo {
        path: "flushdb/ns/sstables/L0/my-ulid-id.sst".to_string(),
        entry_count: 500,
        file_size: 65536,
        min_key: min_key.clone(),
        max_key: max_key.clone(),
        bloom_filter_offset: 50000,
        bloom_filter_size: 1024,
        index_block_offset: 51024,
        index_block_size: 512,
        dedup_block_size: 128,
        compression: CompressionType::Snappy,
    };

    let meta = SSTableMeta::from_sst_info(&sst_info, (10, 200), 75, 1700000000000);

    assert_eq!(meta.id, "my-ulid-id");
    assert_eq!(meta.size_bytes, 65536);
    assert_eq!(meta.entry_count, 500);
    assert_eq!(meta.min_key, min_key.as_bytes());
    assert_eq!(meta.max_key, max_key.as_bytes());
    assert_eq!(meta.bloom_filter_offset, 50000);
    assert_eq!(meta.bloom_filter_size, 1024);
    assert_eq!(meta.index_offset, 51024);
    assert_eq!(meta.index_size, 512);
    assert_eq!(meta.created_at_ms, 1700000000000);
    assert_eq!(meta.sequence_range, (10, 200));
    assert_eq!(meta.record_id_count, 75);
    assert_eq!(meta.dedup_block_size, 128);
    assert!(meta.run_id.is_none());
    assert!(meta.fragment_index.is_none());
}

#[test]
fn test_sstable_meta_from_sst_info_extracts_id_from_path() {
    let key = make_composite_key(b"a", b"");
    let sst_info = SstInfo {
        path: "deep/nested/path/some-id.sst".to_string(),
        entry_count: 1,
        file_size: 100,
        min_key: key.clone(),
        max_key: key.clone(),
        bloom_filter_offset: 0,
        bloom_filter_size: 0,
        index_block_offset: 0,
        index_block_size: 0,
        dedup_block_size: 0,
        compression: CompressionType::None,
    };

    let meta = SSTableMeta::from_sst_info(&sst_info, (1, 1), 1, 0);
    assert_eq!(meta.id, "some-id");
}

#[test]
fn test_sstable_meta_from_sst_info_bare_filename() {
    let key = make_composite_key(b"a", b"");
    let sst_info = SstInfo {
        path: "bare-name.sst".to_string(),
        entry_count: 1,
        file_size: 100,
        min_key: key.clone(),
        max_key: key.clone(),
        bloom_filter_offset: 0,
        bloom_filter_size: 0,
        index_block_offset: 0,
        index_block_size: 0,
        dedup_block_size: 0,
        compression: CompressionType::None,
    };

    let meta = SSTableMeta::from_sst_info(&sst_info, (1, 1), 1, 0);
    assert_eq!(meta.id, "bare-name");
}

#[test]
fn test_sstable_meta_from_sst_info_fragment_includes_run_dir() {
    let key = make_composite_key(b"a", b"");
    let sst_info = SstInfo {
        path: "flushdb/ns/sstables/L1/run-01ABC/frag-0000.sst".to_string(),
        entry_count: 1,
        file_size: 100,
        min_key: key.clone(),
        max_key: key.clone(),
        bloom_filter_offset: 0,
        bloom_filter_size: 0,
        index_block_offset: 0,
        index_block_size: 0,
        dedup_block_size: 0,
        compression: CompressionType::None,
    };

    let meta = SSTableMeta::from_sst_info(&sst_info, (1, 1), 1, 0);
    assert_eq!(meta.id, "run-01ABC/frag-0000");
}

// ---------------------------------------------------------------------------
// Manifest tests
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_new_empty() {
    let m = Manifest::new_empty("test-ns");

    assert_eq!(m.format_version, 1);
    assert_eq!(m.manifest_id, ManifestId::ZERO);
    assert_eq!(m.namespace, "test-ns");
    assert_eq!(m.writer_epoch, 0);
    assert_eq!(m.compactor_epoch, 0);
    assert_eq!(m.last_flushed_sequence, 0);
    assert!(!m.is_snapshot);
    assert_eq!(m.previous_manifest_id, ManifestId::ZERO);

    for level in Level::all() {
        assert!(m.sstables_at_level(*level).is_empty());
    }
    assert!(m.blob_files.is_empty());
    assert!(m.tombstone_compaction_watermarks.is_empty());
}

#[test]
fn test_manifest_new_empty_has_all_levels() {
    let m = Manifest::new_empty("ns");
    assert_eq!(m.levels.len(), 4);
    assert!(m.levels.contains_key(&Level::L0));
    assert!(m.levels.contains_key(&Level::L1));
    assert!(m.levels.contains_key(&Level::L2));
    assert!(m.levels.contains_key(&Level::L3));
}

#[test]
fn test_manifest_serde_roundtrip_empty() {
    let original = Manifest::new_empty("roundtrip-ns");
    let bytes = original.serialize().expect("serialize");
    let recovered = Manifest::deserialize(&bytes).expect("deserialize");

    assert_eq!(recovered.format_version, original.format_version);
    assert_eq!(recovered.manifest_id, original.manifest_id);
    assert_eq!(recovered.namespace, original.namespace);
    assert_eq!(recovered.writer_epoch, original.writer_epoch);
    assert_eq!(recovered.last_flushed_sequence, original.last_flushed_sequence);
    assert_eq!(recovered.levels.len(), original.levels.len());
    assert_eq!(recovered.is_snapshot, original.is_snapshot);
}

#[test]
fn test_manifest_serde_roundtrip_with_sstables() {
    let mut m = Manifest::new_empty("ns");
    m.manifest_id = ManifestId::new(5);
    m.writer_epoch = 10;
    m.last_flushed_sequence = 42;

    let min_l0 = make_composite_key(b"aaa", b"k1");
    let max_l0 = make_composite_key(b"bbb", b"k2");
    m.levels
        .get_mut(&Level::L0)
        .expect("L0 exists")
        .push(make_sstable_meta("l0-sst-1", &min_l0, &max_l0));

    let min_l1 = make_composite_key(b"ccc", b"k3");
    let max_l1 = make_composite_key(b"ddd", b"k4");
    m.levels
        .get_mut(&Level::L1)
        .expect("L1 exists")
        .push(make_sstable_meta("l1-sst-1", &min_l1, &max_l1));

    let bytes = m.serialize().expect("serialize");
    let recovered = Manifest::deserialize(&bytes).expect("deserialize");

    assert_eq!(recovered, m);
}

#[test]
fn test_manifest_serde_roundtrip_with_run_fragments() {
    let mut m = Manifest::new_empty("ns");

    let min = make_composite_key(b"a", b"");
    let max = make_composite_key(b"m", b"");
    let frag = make_run_fragment_meta("frag-0", &min, &max, "run-1", 0);
    m.levels.get_mut(&Level::L1).expect("L1").push(frag);

    let bytes = m.serialize().expect("serialize");
    let recovered = Manifest::deserialize(&bytes).expect("deserialize");
    assert_eq!(recovered, m);

    let sst = &recovered.sstables_at_level(Level::L1)[0];
    assert_eq!(sst.run_id, Some("run-1".to_string()));
    assert_eq!(sst.fragment_index, Some(0));
}

#[test]
fn test_manifest_l0_count_empty() {
    let m = Manifest::new_empty("ns");
    assert_eq!(m.l0_count(), 0);
}

#[test]
fn test_manifest_l0_count_with_sstables() {
    let mut m = Manifest::new_empty("ns");
    let k1 = make_composite_key(b"a", b"");
    let k2 = make_composite_key(b"b", b"");
    let k3 = make_composite_key(b"c", b"");
    let k4 = make_composite_key(b"d", b"");

    let l0 = m.levels.get_mut(&Level::L0).expect("L0");
    l0.push(make_sstable_meta("sst-1", &k1, &k2));
    l0.push(make_sstable_meta("sst-2", &k3, &k4));

    assert_eq!(m.l0_count(), 2);
}

#[test]
fn test_manifest_l0_count_ignores_other_levels() {
    let mut m = Manifest::new_empty("ns");
    let k1 = make_composite_key(b"a", b"");
    let k2 = make_composite_key(b"z", b"");

    m.levels
        .get_mut(&Level::L1)
        .expect("L1")
        .push(make_sstable_meta("l1-sst", &k1, &k2));

    assert_eq!(m.l0_count(), 0);
}

#[test]
fn test_manifest_level_size_bytes_empty() {
    let m = Manifest::new_empty("ns");
    assert_eq!(m.level_size_bytes(Level::L0), 0);
    assert_eq!(m.level_size_bytes(Level::L1), 0);
}

#[test]
fn test_manifest_level_size_bytes_sums_correctly() {
    let mut m = Manifest::new_empty("ns");
    let k1 = make_composite_key(b"a", b"");
    let k2 = make_composite_key(b"m", b"");
    let k3 = make_composite_key(b"n", b"");
    let k4 = make_composite_key(b"z", b"");

    let l1 = m.levels.get_mut(&Level::L1).expect("L1");
    l1.push(make_sstable_meta_with_size("sst-1", &k1, &k2, 1000));
    l1.push(make_sstable_meta_with_size("sst-2", &k3, &k4, 2000));

    assert_eq!(m.level_size_bytes(Level::L1), 3000);
}

#[test]
fn test_manifest_total_sstable_count() {
    let mut m = Manifest::new_empty("ns");
    let k = make_composite_key(b"a", b"");

    m.levels
        .get_mut(&Level::L0)
        .expect("L0")
        .push(make_sstable_meta("l0-1", &k, &k));
    m.levels
        .get_mut(&Level::L0)
        .expect("L0")
        .push(make_sstable_meta("l0-2", &k, &k));
    m.levels
        .get_mut(&Level::L1)
        .expect("L1")
        .push(make_sstable_meta("l1-1", &k, &k));

    assert_eq!(m.total_sstable_count(), 3);
}

#[test]
fn test_manifest_all_sstable_ids() {
    let mut m = Manifest::new_empty("ns");
    let k = make_composite_key(b"a", b"");

    m.levels
        .get_mut(&Level::L0)
        .expect("L0")
        .push(make_sstable_meta("l0-sst", &k, &k));
    m.levels
        .get_mut(&Level::L1)
        .expect("L1")
        .push(make_sstable_meta("l1-sst", &k, &k));
    m.levels
        .get_mut(&Level::L2)
        .expect("L2")
        .push(make_sstable_meta("l2-sst", &k, &k));

    let ids = m.all_sstable_ids();
    assert_eq!(ids.len(), 3);
    assert!(ids.contains(&"l0-sst"));
    assert!(ids.contains(&"l1-sst"));
    assert!(ids.contains(&"l2-sst"));
}

#[test]
fn test_manifest_all_sstable_ids_empty() {
    let m = Manifest::new_empty("ns");
    assert!(m.all_sstable_ids().is_empty());
}

#[test]
fn test_manifest_find_overlapping_returns_matching() {
    let mut m = Manifest::new_empty("ns");

    let sst1_min = make_composite_key(b"aaa", b"");
    let sst1_max = make_composite_key(b"ccc", b"");
    let sst2_min = make_composite_key(b"ddd", b"");
    let sst2_max = make_composite_key(b"fff", b"");
    let sst3_min = make_composite_key(b"ggg", b"");
    let sst3_max = make_composite_key(b"iii", b"");

    let l1 = m.levels.get_mut(&Level::L1).expect("L1");
    l1.push(make_sstable_meta("sst-1", &sst1_min, &sst1_max));
    l1.push(make_sstable_meta("sst-2", &sst2_min, &sst2_max));
    l1.push(make_sstable_meta("sst-3", &sst3_min, &sst3_max));

    let query_start = make_composite_key(b"bbb", b"");
    let query_end = make_composite_key(b"eee", b"");

    let overlapping = m.find_overlapping(Level::L1, query_start.as_bytes(), query_end.as_bytes());
    assert_eq!(overlapping.len(), 2);
    assert_eq!(overlapping[0].id, "sst-1");
    assert_eq!(overlapping[1].id, "sst-2");
}

#[test]
fn test_manifest_find_overlapping_returns_empty_for_no_match() {
    let mut m = Manifest::new_empty("ns");

    let sst_min = make_composite_key(b"aaa", b"");
    let sst_max = make_composite_key(b"bbb", b"");
    m.levels
        .get_mut(&Level::L1)
        .expect("L1")
        .push(make_sstable_meta("sst-1", &sst_min, &sst_max));

    let query_start = make_composite_key(b"ddd", b"");
    let query_end = make_composite_key(b"eee", b"");

    let overlapping = m.find_overlapping(Level::L1, query_start.as_bytes(), query_end.as_bytes());
    assert!(overlapping.is_empty());
}

#[test]
fn test_manifest_find_overlapping_on_empty_level() {
    let m = Manifest::new_empty("ns");

    let start = make_composite_key(b"a", b"");
    let end = make_composite_key(b"z", b"");

    let overlapping = m.find_overlapping(Level::L1, start.as_bytes(), end.as_bytes());
    assert!(overlapping.is_empty());
}

#[test]
fn test_manifest_find_sstable_for_key_l0_linear_scan() {
    let mut m = Manifest::new_empty("ns");

    let sst1_min = make_composite_key(b"aaa", b"");
    let sst1_max = make_composite_key(b"ccc", b"");
    let sst2_min = make_composite_key(b"bbb", b"");
    let sst2_max = make_composite_key(b"ddd", b"");

    let l0 = m.levels.get_mut(&Level::L0).expect("L0");
    l0.push(make_sstable_meta("sst-1", &sst1_min, &sst1_max));
    l0.push(make_sstable_meta("sst-2", &sst2_min, &sst2_max));

    // Key in both ranges: L0 returns the first one found (linear scan)
    let query = make_composite_key(b"bbb", b"x");
    let result = m.find_sstable_for_key(Level::L0, &query);
    assert!(result.is_some());
    assert_eq!(result.expect("found").id, "sst-1");
}

#[test]
fn test_manifest_find_sstable_for_key_binary_search_l1() {
    let mut m = Manifest::new_empty("ns");

    // L1 SSTables sorted by min_key, non-overlapping
    let sst1_min = make_composite_key(b"aaa", b"");
    let sst1_max = make_composite_key(b"ccc", b"");
    let sst2_min = make_composite_key(b"ddd", b"");
    let sst2_max = make_composite_key(b"fff", b"");
    let sst3_min = make_composite_key(b"ggg", b"");
    let sst3_max = make_composite_key(b"iii", b"");

    let l1 = m.levels.get_mut(&Level::L1).expect("L1");
    l1.push(make_sstable_meta("sst-1", &sst1_min, &sst1_max));
    l1.push(make_sstable_meta("sst-2", &sst2_min, &sst2_max));
    l1.push(make_sstable_meta("sst-3", &sst3_min, &sst3_max));

    let query = make_composite_key(b"eee", b"");
    let result = m.find_sstable_for_key(Level::L1, &query);
    assert!(result.is_some());
    assert_eq!(result.expect("found").id, "sst-2");
}

#[test]
fn test_manifest_find_sstable_for_key_binary_search_first_sst() {
    let mut m = Manifest::new_empty("ns");

    let sst1_min = make_composite_key(b"aaa", b"");
    let sst1_max = make_composite_key(b"ccc", b"");
    let sst2_min = make_composite_key(b"ddd", b"");
    let sst2_max = make_composite_key(b"fff", b"");

    let l1 = m.levels.get_mut(&Level::L1).expect("L1");
    l1.push(make_sstable_meta("sst-1", &sst1_min, &sst1_max));
    l1.push(make_sstable_meta("sst-2", &sst2_min, &sst2_max));

    let query = make_composite_key(b"bbb", b"");
    let result = m.find_sstable_for_key(Level::L1, &query);
    assert!(result.is_some());
    assert_eq!(result.expect("found").id, "sst-1");
}

#[test]
fn test_manifest_find_sstable_for_key_binary_search_last_sst() {
    let mut m = Manifest::new_empty("ns");

    let sst1_min = make_composite_key(b"aaa", b"");
    let sst1_max = make_composite_key(b"ccc", b"");
    let sst2_min = make_composite_key(b"ddd", b"");
    let sst2_max = make_composite_key(b"fff", b"");

    let l1 = m.levels.get_mut(&Level::L1).expect("L1");
    l1.push(make_sstable_meta("sst-1", &sst1_min, &sst1_max));
    l1.push(make_sstable_meta("sst-2", &sst2_min, &sst2_max));

    let query = make_composite_key(b"eee", b"");
    let result = m.find_sstable_for_key(Level::L1, &query);
    assert!(result.is_some());
    assert_eq!(result.expect("found").id, "sst-2");
}

#[test]
fn test_manifest_find_sstable_for_key_miss_between_gaps() {
    let mut m = Manifest::new_empty("ns");

    let sst1_min = make_composite_key(b"aaa", b"");
    let sst1_max = make_composite_key(b"bbb", b"");
    let sst2_min = make_composite_key(b"ddd", b"");
    let sst2_max = make_composite_key(b"eee", b"");

    let l1 = m.levels.get_mut(&Level::L1).expect("L1");
    l1.push(make_sstable_meta("sst-1", &sst1_min, &sst1_max));
    l1.push(make_sstable_meta("sst-2", &sst2_min, &sst2_max));

    // "ccc" falls in the gap between sst-1 and sst-2
    let query = make_composite_key(b"ccc", b"");
    let result = m.find_sstable_for_key(Level::L1, &query);
    assert!(result.is_none());
}

#[test]
fn test_manifest_find_sstable_for_key_miss_before_all() {
    let mut m = Manifest::new_empty("ns");

    let sst_min = make_composite_key(b"ddd", b"");
    let sst_max = make_composite_key(b"fff", b"");
    m.levels
        .get_mut(&Level::L1)
        .expect("L1")
        .push(make_sstable_meta("sst-1", &sst_min, &sst_max));

    let query = make_composite_key(b"aaa", b"");
    let result = m.find_sstable_for_key(Level::L1, &query);
    assert!(result.is_none());
}

#[test]
fn test_manifest_find_sstable_for_key_miss_after_all() {
    let mut m = Manifest::new_empty("ns");

    let sst_min = make_composite_key(b"aaa", b"");
    let sst_max = make_composite_key(b"ccc", b"");
    m.levels
        .get_mut(&Level::L1)
        .expect("L1")
        .push(make_sstable_meta("sst-1", &sst_min, &sst_max));

    let query = make_composite_key(b"zzz", b"");
    let result = m.find_sstable_for_key(Level::L1, &query);
    assert!(result.is_none());
}

#[test]
fn test_manifest_find_sstable_for_key_empty_level() {
    let m = Manifest::new_empty("ns");
    let query = make_composite_key(b"anything", b"");
    assert!(m.find_sstable_for_key(Level::L1, &query).is_none());
}

#[test]
fn test_manifest_serialize_produces_valid_json() {
    let m = Manifest::new_empty("json-ns");
    let bytes = m.serialize().expect("serialize");

    // Verify it's valid JSON
    let _value: serde_json::Value =
        serde_json::from_slice(&bytes).expect("should be valid JSON");
}

#[test]
fn test_manifest_deserialize_invalid_json() {
    let result = Manifest::deserialize(b"not json at all");
    assert!(result.is_err());
}

// ---------------------------------------------------------------------------
// ManifestUpdate tests
// ---------------------------------------------------------------------------

fn make_flush_update(add: Vec<(Level, SSTableMeta)>, seq: Option<u64>) -> ManifestUpdate {
    ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: add,
        remove_sstables: Vec::new(),
        new_last_flushed_sequence: seq,
        writer_epoch: 1,
        compactor_epoch: 0,
    }
}

fn make_compaction_update(
    add: Vec<(Level, SSTableMeta)>,
    remove: Vec<(Level, String)>,
) -> ManifestUpdate {
    ManifestUpdate {
        trigger: ManifestUpdateTrigger::Compaction,
        add_sstables: add,
        remove_sstables: remove,
        new_last_flushed_sequence: None,
        writer_epoch: 1,
        compactor_epoch: 1,
    }
}

#[test]
fn test_manifest_update_add_sstables() {
    let m = Manifest::new_empty("ns");

    let min_key = make_composite_key(b"aaa", b"");
    let max_key = make_composite_key(b"zzz", b"");
    let meta = make_sstable_meta("new-sst", &min_key, &max_key);

    let update = make_flush_update(vec![(Level::L0, meta.clone())], Some(100));
    let new_m = update.apply(&m).expect("apply");

    assert_eq!(new_m.l0_count(), 1);
    assert_eq!(new_m.sstables_at_level(Level::L0)[0].id, "new-sst");
    assert_eq!(new_m.manifest_id, ManifestId::new(1));
}

#[test]
fn test_manifest_update_add_multiple_sstables() {
    let m = Manifest::new_empty("ns");

    let k1 = make_composite_key(b"aaa", b"");
    let k2 = make_composite_key(b"bbb", b"");
    let k3 = make_composite_key(b"ccc", b"");
    let k4 = make_composite_key(b"ddd", b"");

    let meta1 = make_sstable_meta("sst-1", &k1, &k2);
    let meta2 = make_sstable_meta("sst-2", &k3, &k4);

    let update = make_flush_update(
        vec![(Level::L0, meta1), (Level::L0, meta2)],
        Some(200),
    );
    let new_m = update.apply(&m).expect("apply");

    assert_eq!(new_m.l0_count(), 2);
}

#[test]
fn test_manifest_update_add_to_l1_keeps_sorted() {
    let m = Manifest::new_empty("ns");

    // Add them out of order; L1 should sort by min_key
    let k_c = make_composite_key(b"ccc", b"");
    let k_d = make_composite_key(b"ddd", b"");
    let k_a = make_composite_key(b"aaa", b"");
    let k_b = make_composite_key(b"bbb", b"");

    let meta_cd = make_sstable_meta("sst-cd", &k_c, &k_d);
    let meta_ab = make_sstable_meta("sst-ab", &k_a, &k_b);

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Compaction,
        add_sstables: vec![(Level::L1, meta_cd), (Level::L1, meta_ab)],
        remove_sstables: Vec::new(),
        new_last_flushed_sequence: None,
        writer_epoch: 1,
        compactor_epoch: 1,
    };

    let new_m = update.apply(&m).expect("apply");
    let l1 = new_m.sstables_at_level(Level::L1);
    assert_eq!(l1.len(), 2);
    assert_eq!(l1[0].id, "sst-ab");
    assert_eq!(l1[1].id, "sst-cd");
}

#[test]
fn test_manifest_update_remove_sstables() {
    let mut m = Manifest::new_empty("ns");
    let k1 = make_composite_key(b"a", b"");
    let k2 = make_composite_key(b"z", b"");

    m.levels
        .get_mut(&Level::L0)
        .expect("L0")
        .push(make_sstable_meta("sst-to-remove", &k1, &k2));

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Compaction,
        add_sstables: Vec::new(),
        remove_sstables: vec![(Level::L0, "sst-to-remove".to_string())],
        new_last_flushed_sequence: None,
        writer_epoch: 1,
        compactor_epoch: 1,
    };

    let new_m = update.apply(&m).expect("apply");
    assert_eq!(new_m.l0_count(), 0);
}

#[test]
fn test_manifest_update_remove_nonexistent_errors() {
    let m = Manifest::new_empty("ns");

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Compaction,
        add_sstables: Vec::new(),
        remove_sstables: vec![(Level::L0, "does-not-exist".to_string())],
        new_last_flushed_sequence: None,
        writer_epoch: 1,
        compactor_epoch: 1,
    };

    let result = update.apply(&m);
    assert!(result.is_err());
}

#[test]
fn test_manifest_update_sets_previous_id() {
    let mut m = Manifest::new_empty("ns");
    m.manifest_id = ManifestId::new(10);

    let update = make_flush_update(Vec::new(), None);
    let new_m = update.apply(&m).expect("apply");

    assert_eq!(new_m.previous_manifest_id, ManifestId::new(10));
    assert_eq!(new_m.manifest_id, ManifestId::new(11));
}

#[test]
fn test_manifest_update_increments_manifest_id() {
    let m = Manifest::new_empty("ns");
    assert_eq!(m.manifest_id, ManifestId::ZERO);

    let update1 = make_flush_update(Vec::new(), None);
    let m1 = update1.apply(&m).expect("apply");
    assert_eq!(m1.manifest_id, ManifestId::new(1));

    let update2 = make_flush_update(Vec::new(), None);
    let m2 = update2.apply(&m1).expect("apply");
    assert_eq!(m2.manifest_id, ManifestId::new(2));
}

#[test]
fn test_manifest_update_flush_sequence_update() {
    let m = Manifest::new_empty("ns");

    let update = make_flush_update(Vec::new(), Some(42));
    let new_m = update.apply(&m).expect("apply");

    assert_eq!(new_m.last_flushed_sequence, 42);
}

#[test]
fn test_manifest_update_flush_sequence_must_not_decrease() {
    let mut m = Manifest::new_empty("ns");
    m.last_flushed_sequence = 100;

    let update = make_flush_update(Vec::new(), Some(50));
    let result = update.apply(&m);
    assert!(result.is_err());
}

#[test]
fn test_manifest_update_flush_sequence_can_stay_same() {
    let mut m = Manifest::new_empty("ns");
    m.last_flushed_sequence = 100;

    let update = make_flush_update(Vec::new(), Some(100));
    let new_m = update.apply(&m).expect("apply");
    assert_eq!(new_m.last_flushed_sequence, 100);
}

#[test]
fn test_manifest_update_no_sequence_preserves_current() {
    let mut m = Manifest::new_empty("ns");
    m.last_flushed_sequence = 77;

    let update = make_flush_update(Vec::new(), None);
    let new_m = update.apply(&m).expect("apply");
    assert_eq!(new_m.last_flushed_sequence, 77);
}

#[test]
fn test_manifest_update_combined_add_and_remove() {
    let mut m = Manifest::new_empty("ns");

    // Pre-populate L0 with two SSTables
    let k_a = make_composite_key(b"aaa", b"");
    let k_b = make_composite_key(b"bbb", b"");
    let k_c = make_composite_key(b"ccc", b"");
    let k_d = make_composite_key(b"ddd", b"");

    m.levels
        .get_mut(&Level::L0)
        .expect("L0")
        .push(make_sstable_meta("l0-old-1", &k_a, &k_b));
    m.levels
        .get_mut(&Level::L0)
        .expect("L0")
        .push(make_sstable_meta("l0-old-2", &k_c, &k_d));

    // Compaction: remove L0 SSTables, add merged L1 SSTable
    let merged = make_sstable_meta("l1-merged", &k_a, &k_d);
    let update = make_compaction_update(
        vec![(Level::L1, merged)],
        vec![
            (Level::L0, "l0-old-1".to_string()),
            (Level::L0, "l0-old-2".to_string()),
        ],
    );

    let new_m = update.apply(&m).expect("apply");
    assert_eq!(new_m.l0_count(), 0);
    assert_eq!(new_m.sstables_at_level(Level::L1).len(), 1);
    assert_eq!(new_m.sstables_at_level(Level::L1)[0].id, "l1-merged");
}

#[test]
fn test_manifest_update_updates_epochs() {
    let m = Manifest::new_empty("ns");

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: Vec::new(),
        remove_sstables: Vec::new(),
        new_last_flushed_sequence: None,
        writer_epoch: 42,
        compactor_epoch: 99,
    };

    let new_m = update.apply(&m).expect("apply");
    assert_eq!(new_m.writer_epoch, 42);
    assert_eq!(new_m.compactor_epoch, 99);
}

#[test]
fn test_manifest_update_preserves_namespace() {
    let m = Manifest::new_empty("my-namespace");
    let update = make_flush_update(Vec::new(), None);
    let new_m = update.apply(&m).expect("apply");

    assert_eq!(new_m.namespace, "my-namespace");
}

#[test]
fn test_manifest_update_preserves_existing_sstables() {
    let mut m = Manifest::new_empty("ns");

    let k1 = make_composite_key(b"aaa", b"");
    let k2 = make_composite_key(b"bbb", b"");
    m.levels
        .get_mut(&Level::L1)
        .expect("L1")
        .push(make_sstable_meta("existing-l1", &k1, &k2));

    // Add to L0, existing L1 should remain
    let k3 = make_composite_key(b"ccc", b"");
    let k4 = make_composite_key(b"ddd", b"");
    let new_meta = make_sstable_meta("new-l0", &k3, &k4);

    let update = make_flush_update(vec![(Level::L0, new_meta)], Some(10));
    let new_m = update.apply(&m).expect("apply");

    assert_eq!(new_m.l0_count(), 1);
    assert_eq!(new_m.sstables_at_level(Level::L1).len(), 1);
    assert_eq!(new_m.sstables_at_level(Level::L1)[0].id, "existing-l1");
}

// ---------------------------------------------------------------------------
// ManifestUpdateTrigger tests
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_update_trigger_serde_roundtrip() {
    let triggers = [
        ManifestUpdateTrigger::Flush,
        ManifestUpdateTrigger::Compaction,
        ManifestUpdateTrigger::GC,
    ];

    for trigger in &triggers {
        let json = serde_json::to_string(trigger).expect("serialize");
        let deserialized: ManifestUpdateTrigger =
            serde_json::from_str(&json).expect("deserialize");
        assert_eq!(&deserialized, trigger);
    }
}

// ---------------------------------------------------------------------------
// ManifestConfig tests
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_config_defaults() {
    let config = ManifestConfig::default();
    assert_eq!(config.snapshot_interval, 100);
    assert_eq!(config.max_manifest_size, 16 * 1024 * 1024);
    assert_eq!(config.pruning_batch_size, 50);
    assert_eq!(config.base_path, "flushdb");
}

// ---------------------------------------------------------------------------
// Path helper tests
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_path_format() {
    let id = ManifestId::new(42);
    let path = manifest_path("flushdb", "my-ns", &id);
    assert_eq!(path, "flushdb/my-ns/manifests/00000000000000000042");
}

#[test]
fn test_manifest_path_format_zero() {
    let path = manifest_path("flushdb", "ns", &ManifestId::ZERO);
    assert_eq!(path, "flushdb/ns/manifests/00000000000000000000");
}

#[test]
fn test_manifest_path_custom_base() {
    let id = ManifestId::new(1);
    let path = manifest_path("custom/base", "ns", &id);
    assert_eq!(path, "custom/base/ns/manifests/00000000000000000001");
}

#[test]
fn test_l0_sst_path_format() {
    let path = l0_sst_path("flushdb", "my-ns", "01ABCDEF-sst");
    assert_eq!(path, "flushdb/my-ns/sstables/L0/01ABCDEF-sst.sst");
}

#[test]
fn test_l0_sst_path_custom_base() {
    let path = l0_sst_path("custom", "ns", "the-id");
    assert_eq!(path, "custom/ns/sstables/L0/the-id.sst");
}

#[test]
fn test_run_fragment_path_format() {
    let path = run_fragment_path("flushdb", "my-ns", Level::L1, "run-abc", 0);
    assert_eq!(
        path,
        "flushdb/my-ns/sstables/L1/run-run-abc/frag-0000.sst"
    );
}

#[test]
fn test_run_fragment_path_high_index() {
    let path = run_fragment_path("flushdb", "ns", Level::L2, "xyz", 42);
    assert_eq!(path, "flushdb/ns/sstables/L2/run-xyz/frag-0042.sst");
}

#[test]
fn test_run_fragment_path_l3() {
    let path = run_fragment_path("base", "ns", Level::L3, "run-id", 9999);
    assert_eq!(path, "base/ns/sstables/L3/run-run-id/frag-9999.sst");
}

#[test]
fn test_run_fragment_path_custom_base() {
    let path = run_fragment_path("my/base", "prod", Level::L1, "r1", 1);
    assert_eq!(path, "my/base/prod/sstables/L1/run-r1/frag-0001.sst");
}

// ---------------------------------------------------------------------------
// Edge case and integration-style tests
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_full_lifecycle_flush_then_compaction() {
    let m = Manifest::new_empty("lifecycle-ns");

    // Flush: add 3 L0 SSTables
    let k1 = make_composite_key(b"aaa", b"");
    let k2 = make_composite_key(b"ccc", b"");
    let k3 = make_composite_key(b"ddd", b"");
    let k4 = make_composite_key(b"fff", b"");
    let k5 = make_composite_key(b"ggg", b"");
    let k6 = make_composite_key(b"iii", b"");

    let flush1 = make_flush_update(
        vec![(Level::L0, make_sstable_meta("l0-1", &k1, &k2))],
        Some(10),
    );
    let m = flush1.apply(&m).expect("flush 1");

    let flush2 = make_flush_update(
        vec![(Level::L0, make_sstable_meta("l0-2", &k3, &k4))],
        Some(20),
    );
    let m = flush2.apply(&m).expect("flush 2");

    let flush3 = make_flush_update(
        vec![(Level::L0, make_sstable_meta("l0-3", &k5, &k6))],
        Some(30),
    );
    let m = flush3.apply(&m).expect("flush 3");

    assert_eq!(m.l0_count(), 3);
    assert_eq!(m.manifest_id, ManifestId::new(3));
    assert_eq!(m.last_flushed_sequence, 30);

    // Compaction: merge all L0 into one L1
    let merged = make_sstable_meta("l1-merged", &k1, &k6);
    let compaction = make_compaction_update(
        vec![(Level::L1, merged)],
        vec![
            (Level::L0, "l0-1".to_string()),
            (Level::L0, "l0-2".to_string()),
            (Level::L0, "l0-3".to_string()),
        ],
    );

    let m = compaction.apply(&m).expect("compaction");
    assert_eq!(m.l0_count(), 0);
    assert_eq!(m.sstables_at_level(Level::L1).len(), 1);
    assert_eq!(m.manifest_id, ManifestId::new(4));
    assert_eq!(m.previous_manifest_id, ManifestId::new(3));
    assert_eq!(m.last_flushed_sequence, 30);
}

#[test]
fn test_manifest_serialize_deserialize_preserves_sstable_binary_keys() {
    let mut m = Manifest::new_empty("ns");

    // Use keys with non-ASCII bytes to stress base64 encoding
    let min_key = make_composite_key(b"rec", b"\x01\x80\xFF");
    let max_key = make_composite_key(b"rec", b"\xFE\xFF");
    m.levels
        .get_mut(&Level::L0)
        .expect("L0")
        .push(make_sstable_meta("binary-sst", &min_key, &max_key));

    let bytes = m.serialize().expect("serialize");
    let recovered = Manifest::deserialize(&bytes).expect("deserialize");

    let sst = &recovered.sstables_at_level(Level::L0)[0];
    assert_eq!(sst.min_key, min_key.as_bytes());
    assert_eq!(sst.max_key, max_key.as_bytes());
}

#[test]
fn test_sstable_meta_overlaps_range_symmetric() {
    let min = make_composite_key(b"bbb", b"");
    let max = make_composite_key(b"ddd", b"");
    let meta = make_sstable_meta("sst", &min, &max);

    // Exact same range
    assert!(meta.overlaps_range(min.as_bytes(), max.as_bytes()));

    // Range contains the SSTable
    let wide_start = make_composite_key(b"aaa", b"");
    let wide_end = make_composite_key(b"zzz", b"");
    assert!(meta.overlaps_range(wide_start.as_bytes(), wide_end.as_bytes()));

    // SSTable contains the range
    let narrow_start = make_composite_key(b"ccc", b"");
    let narrow_end = make_composite_key(b"ccc", b"z");
    assert!(meta.overlaps_range(narrow_start.as_bytes(), narrow_end.as_bytes()));
}

#[test]
fn test_manifest_sstables_at_level_returns_empty_for_missing_level() {
    // Even though new_empty initializes all levels, test the fallback
    let m = Manifest::new_empty("ns");
    let ssts = m.sstables_at_level(Level::L3);
    assert!(ssts.is_empty());
}

// ---------------------------------------------------------------------------
// total_sstable_count tests
// ---------------------------------------------------------------------------

#[test]
fn test_total_sstable_count_empty() {
    let m = Manifest::new_empty("ns");
    assert_eq!(m.total_sstable_count(), 0);
}

#[test]
fn test_total_sstable_count_across_levels() {
    let m = Manifest::new_empty("ns");
    let k1 = make_composite_key(b"a", b"");
    let k2 = make_composite_key(b"m", b"");
    let k3 = make_composite_key(b"n", b"");
    let k4 = make_composite_key(b"z", b"");

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![
            (Level::L0, make_sstable_meta("s1", &k1, &k2)),
            (Level::L0, make_sstable_meta("s2", &k3, &k4)),
            (Level::L1, make_sstable_meta("s3", &k1, &k4)),
        ],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: 0,
        compactor_epoch: 0,
    };

    let m2 = update.apply(&m).unwrap();
    assert_eq!(m2.total_sstable_count(), 3);
}

// ---------------------------------------------------------------------------
// all_sstable_ids tests
// ---------------------------------------------------------------------------

#[test]
fn test_all_sstable_ids_empty() {
    let m = Manifest::new_empty("ns");
    assert!(m.all_sstable_ids().is_empty());
}

#[test]
fn test_all_sstable_ids_returns_all() {
    let m = Manifest::new_empty("ns");
    let k1 = make_composite_key(b"a", b"");
    let k2 = make_composite_key(b"z", b"");

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![
            (Level::L0, make_sstable_meta("alpha", &k1, &k2)),
            (Level::L1, make_sstable_meta("beta", &k1, &k2)),
        ],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: 0,
        compactor_epoch: 0,
    };

    let m2 = update.apply(&m).unwrap();
    let ids = m2.all_sstable_ids();
    assert_eq!(ids.len(), 2);
    assert!(ids.contains(&"alpha"));
    assert!(ids.contains(&"beta"));
}

// ---------------------------------------------------------------------------
// find_sstable_for_key tests
// ---------------------------------------------------------------------------

#[test]
fn test_find_sstable_for_key_l0_linear_scan() {
    let m = Manifest::new_empty("ns");
    let k_a = make_composite_key(b"a", b"");
    let k_m = make_composite_key(b"m", b"");
    let k_n = make_composite_key(b"n", b"");
    let k_z = make_composite_key(b"z", b"");

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![
            (Level::L0, make_sstable_meta("s1", &k_a, &k_m)),
            (Level::L0, make_sstable_meta("s2", &k_n, &k_z)),
        ],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: 0,
        compactor_epoch: 0,
    };
    let m2 = update.apply(&m).unwrap();

    let target = make_composite_key(b"b", b"");
    let found = m2.find_sstable_for_key(Level::L0, &target);
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, "s1");
}

#[test]
fn test_find_sstable_for_key_l1_binary_search() {
    let m = Manifest::new_empty("ns");
    let k_a = make_composite_key(b"a", b"");
    let k_m = make_composite_key(b"m", b"");
    let k_n = make_composite_key(b"n", b"");
    let k_z = make_composite_key(b"z", b"");

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![
            (Level::L1, make_sstable_meta("s1", &k_a, &k_m)),
            (Level::L1, make_sstable_meta("s2", &k_n, &k_z)),
        ],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: 0,
        compactor_epoch: 0,
    };
    let m2 = update.apply(&m).unwrap();

    let target = make_composite_key(b"p", b"");
    let found = m2.find_sstable_for_key(Level::L1, &target);
    assert!(found.is_some());
    assert_eq!(found.unwrap().id, "s2");
}

#[test]
fn test_find_sstable_for_key_miss() {
    let m = Manifest::new_empty("ns");
    let k_b = make_composite_key(b"b", b"");
    let k_d = make_composite_key(b"d", b"");

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L1, make_sstable_meta("s1", &k_b, &k_d))],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: 0,
        compactor_epoch: 0,
    };
    let m2 = update.apply(&m).unwrap();

    let target_before = make_composite_key(b"a", b"");
    assert!(m2.find_sstable_for_key(Level::L1, &target_before).is_none());

    let target_after = make_composite_key(b"z", b"");
    assert!(m2.find_sstable_for_key(Level::L1, &target_after).is_none());
}

#[test]
fn test_find_sstable_for_key_empty_level() {
    let m = Manifest::new_empty("ns");
    let target = make_composite_key(b"a", b"");
    assert!(m.find_sstable_for_key(Level::L0, &target).is_none());
    assert!(m.find_sstable_for_key(Level::L1, &target).is_none());
}

// ---------------------------------------------------------------------------
// SSTableMeta::overlaps tests
// ---------------------------------------------------------------------------

#[test]
fn test_sstable_meta_overlaps_two_metas() {
    let k_a = make_composite_key(b"a", b"");
    let k_m = make_composite_key(b"m", b"");
    let k_d = make_composite_key(b"d", b"");
    let k_z = make_composite_key(b"z", b"");

    let meta1 = make_sstable_meta("s1", &k_a, &k_m);
    let meta2 = make_sstable_meta("s2", &k_d, &k_z);

    assert!(meta1.overlaps(&meta2));
    assert!(meta2.overlaps(&meta1));
}

#[test]
fn test_sstable_meta_no_overlap() {
    let k_a = make_composite_key(b"a", b"");
    let k_c = make_composite_key(b"c", b"");
    let k_d = make_composite_key(b"d", b"");
    let k_z = make_composite_key(b"z", b"");

    let meta1 = make_sstable_meta("s1", &k_a, &k_c);
    let meta2 = make_sstable_meta("s2", &k_d, &k_z);

    assert!(!meta1.overlaps(&meta2));
    assert!(!meta2.overlaps(&meta1));
}

// ---------------------------------------------------------------------------
// ManifestUpdate::apply decreasing sequence error test
// ---------------------------------------------------------------------------

#[test]
fn test_manifest_update_apply_decreasing_sequence_errors() {
    let mut m = Manifest::new_empty("ns");
    m.last_flushed_sequence = 100;

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(50),
        writer_epoch: 0,
        compactor_epoch: 0,
    };

    let result = update.apply(&m);
    assert!(result.is_err());
}
