use bytes::Bytes;
use tempfile::TempDir;

use flushdb_engine::{
    FlushConfig, FlushPipeline, ManifestConfig, ManifestManager,
    Memtable, MemtableConfig,
};
use flushdb_types::{
    CompositeKey, EntryType, IdempotencyToken, LocalFsBackend, MemtableEntry,
};

fn setup() -> (TempDir, LocalFsBackend) {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let backend = LocalFsBackend::new(storage_dir);
    (dir, backend)
}

fn make_entry(record_id: &[u8], key: &[u8], value: &[u8]) -> MemtableEntry {
    MemtableEntry::new(
        CompositeKey::new(record_id, key).unwrap(),
        Bytes::copy_from_slice(value),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Put,
    )
}

fn populated_memtable(count: usize) -> Memtable {
    let config = MemtableConfig {
        size_threshold: 64 * 1024 * 1024,
        max_frozen_count: 3,
        ..Default::default()
    };
    let mut mt = Memtable::new(config, 1);
    for i in 0..count {
        let key = format!("key{:06}", i);
        let value = format!("value{}", i);
        mt.insert(make_entry(b"rec1", key.as_bytes(), value.as_bytes()))
            .unwrap();
    }
    mt.freeze();
    mt
}

#[tokio::test]
async fn test_flush_creates_sstable_and_updates_manifest() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let pipeline = FlushPipeline::new(
        FlushConfig::default(),
        "test-ns".into(),
        "flushdb".into(),
    );

    let frozen = populated_memtable(10);
    let result = pipeline
        .flush(frozen, &mut manager, &backend, 0)
        .await
        .unwrap();

    assert_eq!(result.l0_count_after, 1);
    assert!(result.sst_info.entry_count > 0);
    assert_eq!(manager.current().l0_count(), 1);
}

#[tokio::test]
async fn test_flush_updates_last_flushed_sequence() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let pipeline = FlushPipeline::new(FlushConfig::default(), "test-ns".into(), "flushdb".into());

    let frozen = populated_memtable(5);
    let result = pipeline
        .flush(frozen, &mut manager, &backend, 0)
        .await
        .unwrap();

    assert_eq!(
        manager.current().last_flushed_sequence,
        result.flushed_sequence_range.1
    );
    // Verify sequence range is valid (min <= max, both non-zero)
    let (min_seq, max_seq) = result.flushed_sequence_range;
    assert!(min_seq > 0, "min sequence should be positive");
    assert!(max_seq >= min_seq, "max_seq should be >= min_seq");
    assert_eq!(result.generation_id, 0, "generation should match passed value");
}

#[tokio::test]
async fn test_multiple_flushes_increment_manifest() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let pipeline = FlushPipeline::new(FlushConfig::default(), "test-ns".into(), "flushdb".into());

    let f1 = populated_memtable(5);
    pipeline.flush(f1, &mut manager, &backend, 0).await.unwrap();

    let f2 = populated_memtable(5);
    pipeline.flush(f2, &mut manager, &backend, 1).await.unwrap();

    assert_eq!(manager.current().l0_count(), 2);
}

#[tokio::test]
async fn test_flush_includes_tombstones() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let pipeline = FlushPipeline::new(FlushConfig::default(), "test-ns".into(), "flushdb".into());

    let mt_config = MemtableConfig {
        size_threshold: 64 * 1024 * 1024,
        max_frozen_count: 3,
        ..Default::default()
    };
    let mut mt = Memtable::new(mt_config, 1);
    mt.insert(make_entry(b"rec1", b"key1", b"val1")).unwrap();
    mt.insert(MemtableEntry::new(
        CompositeKey::new(b"rec1", b"key2").unwrap(),
        Bytes::new(),
        Bytes::new(),
        IdempotencyToken::none(),
        EntryType::Delete,
    ))
    .unwrap();
    mt.freeze();

    let result = pipeline.flush(mt, &mut manager, &backend, 0).await.unwrap();
    // Both entries should be in the SSTable (put + tombstone)
    assert_eq!(result.sst_info.entry_count, 2);
}

#[tokio::test]
async fn test_flush_empty_memtable_errors() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let pipeline = FlushPipeline::new(FlushConfig::default(), "test-ns".into(), "flushdb".into());

    let mt_config = MemtableConfig::default();
    let mt = Memtable::new(mt_config, 1);
    // Freeze without inserting
    let mut mt = mt;
    mt.freeze();

    let result = pipeline.flush(mt, &mut manager, &backend, 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_backpressure_rejects_at_limit() {
    let pipeline = FlushPipeline::new(
        FlushConfig {
            max_frozen_count: 3,
            ..FlushConfig::default()
        },
        "ns".into(),
        "flushdb".into(),
    );

    assert!(pipeline.check_backpressure(2).is_ok());
    assert!(pipeline.check_backpressure(3).is_err());
    assert!(pipeline.check_backpressure(4).is_err());
}

#[test]
fn test_should_freeze_by_size_threshold() {
    let pipeline = FlushPipeline::new(
        FlushConfig {
            flush_trigger_size: 100,
            ..FlushConfig::default()
        },
        "ns".into(),
        "flushdb".into(),
    );

    let config = MemtableConfig {
        size_threshold: 100,
        max_frozen_count: 3,
        ..Default::default()
    };
    let mut mt = Memtable::new(config, 1);

    assert!(
        !pipeline.should_freeze(&mt, std::time::Duration::from_secs(3600)),
        "empty memtable should not trigger freeze"
    );

    // Fill memtable past threshold
    for i in 0..20 {
        let key = format!("key{i:04}");
        mt.insert(make_entry(b"rec1", key.as_bytes(), b"some_value_that_takes_space"))
            .unwrap();
    }

    assert!(
        pipeline.should_freeze(&mt, std::time::Duration::from_secs(3600)),
        "memtable past size threshold should trigger freeze"
    );
}

#[test]
fn test_should_freeze_by_age() {
    let pipeline = FlushPipeline::new(FlushConfig::default(), "ns".into(), "flushdb".into());

    let config = MemtableConfig::default();
    let mut mt = Memtable::new(config, 1);
    mt.insert(make_entry(b"rec1", b"key1", b"val1")).unwrap();

    // With Duration::ZERO, any non-empty memtable is "old enough"
    assert!(pipeline.should_freeze(&mt, std::time::Duration::from_nanos(1)));
}

#[test]
fn test_should_freeze_below_threshold_returns_false() {
    let pipeline = FlushPipeline::new(FlushConfig::default(), "ns".into(), "flushdb".into());

    let config = MemtableConfig::default();
    let mut mt = Memtable::new(config, 1);
    mt.insert(make_entry(b"rec1", b"key1", b"val1")).unwrap();

    // High age threshold + small memtable = no freeze
    assert!(!pipeline.should_freeze(&mt, std::time::Duration::from_secs(3600)));
}
