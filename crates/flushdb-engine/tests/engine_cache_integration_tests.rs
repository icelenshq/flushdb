use bytes::Bytes;
use tempfile::TempDir;

use flushdb_engine::{
    CacheConfig, Engine, EngineConfig, FlushConfig, ManifestConfig, MemtableConfig,
    CompactionConfig, RangeReadOptions,
};
use flushdb_types::LocalFsBackend;
use flushdb_wal::WalConfig;

fn test_config(dir: &TempDir, namespace: &str) -> (EngineConfig, LocalFsBackend) {
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let backend = LocalFsBackend::new(storage_dir);
    let config = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 4096,
            max_frozen_count: 3,
        },
        wal_config: WalConfig::default(),
        flush_config: FlushConfig {
            sst_config: flushdb_engine::sstable::types::SstConfig::default(),
            max_frozen_count: 3,
            flush_trigger_size: 4096,
            flush_trigger_age: std::time::Duration::from_secs(3600),
        },
        compaction_config: CompactionConfig::default(),
        manifest_config: ManifestConfig {
            base_path: "flushdb".to_string(),
            ..ManifestConfig::default()
        },
        cache_config: CacheConfig::default(),
        namespace: namespace.to_string(),
        local_dir: dir.path().to_path_buf(),
    };
    (config, backend)
}

#[tokio::test]
async fn test_engine_opens_with_cache() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");

    let engine = Engine::open(backend, config).await.unwrap();
    assert_eq!(engine.pinned_metadata_count(), 0);
    assert_eq!(engine.continuity_tracked_records(), 0);
    let stats = engine.cache_stats();
    assert_eq!(stats.entry_count, 0);
}

#[tokio::test]
async fn test_repeated_get_uses_cache() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..100u32 {
        let key = format!("key{:04}", i);
        let value = format!("value_{}", i);
        engine
            .put(b"rec1", key.as_bytes(), Bytes::from(value), Bytes::new(), None)
            .await
            .unwrap();
    }

    engine.close().await.unwrap();

    let (config2, backend2) = test_config(&dir, "test-ns");
    let engine2 = Engine::open(backend2, config2).await.unwrap();

    assert!(engine2.l0_count() > 0, "expected L0 SSTables after close");

    let result1 = engine2.get(b"rec1", b"key0010").await.unwrap();
    assert!(result1.is_some());
    assert_eq!(result1.unwrap().value, Bytes::from("value_10"));

    let result2 = engine2.get(b"rec1", b"key0010").await.unwrap();
    assert!(result2.is_some());
    assert_eq!(result2.unwrap().value, Bytes::from("value_10"));
}

#[tokio::test]
async fn test_scan_populates_cache() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..100u32 {
        let key = format!("key{:04}", i);
        let value = format!("value_{}", i);
        engine
            .put(b"rec1", key.as_bytes(), Bytes::from(value), Bytes::new(), None)
            .await
            .unwrap();
    }

    engine.close().await.unwrap();

    let (config2, backend2) = test_config(&dir, "test-ns");
    let engine2 = Engine::open(backend2, config2).await.unwrap();

    let scan_result = engine2
        .scan(b"rec1", None, None, RangeReadOptions::default())
        .await
        .unwrap();
    assert_eq!(scan_result.entries.len(), 100);

    for i in 0..100u32 {
        let key = format!("key{:04}", i);
        let expected = format!("value_{}", i);
        let result = engine2.get(b"rec1", key.as_bytes()).await.unwrap();
        assert!(result.is_some(), "key {} should be found after scan", key);
        assert_eq!(result.unwrap().value, Bytes::from(expected));
    }
}

#[tokio::test]
async fn test_write_invalidates_continuity() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    engine
        .put(b"rec1", b"key_a", Bytes::from("va"), Bytes::new(), None)
        .await
        .unwrap();

    let missing = engine.get(b"rec1", b"nonexistent").await.unwrap();
    assert!(missing.is_none());

    engine
        .put(b"rec1", b"key_b", Bytes::from("vb"), Bytes::new(), None)
        .await
        .unwrap();

    let still_missing = engine.get(b"rec1", b"nonexistent").await.unwrap();
    assert!(still_missing.is_none());

    let found = engine.get(b"rec1", b"key_b").await.unwrap();
    assert!(found.is_some());
    assert_eq!(found.unwrap().value, Bytes::from("vb"));
}

#[tokio::test]
async fn test_full_lifecycle_with_cache() {
    let dir = TempDir::new().unwrap();
    let namespace = "cache-lifecycle";

    {
        let (config, backend) = test_config(&dir, namespace);
        let mut engine = Engine::open(backend, config).await.unwrap();

        for i in 0..30u32 {
            let key = format!("key{:04}", i);
            let val = format!("val{}", i);
            engine
                .put(b"rec1", key.as_bytes(), Bytes::from(val), Bytes::new(), None)
                .await
                .unwrap();
        }

        let scan = engine
            .scan(b"rec1", None, None, RangeReadOptions::default())
            .await
            .unwrap();
        assert_eq!(scan.entries.len(), 30);

        let result = engine.get(b"rec1", b"key0015").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("val15"));

        engine.delete(b"rec1", b"key0015").await.unwrap();

        let deleted = engine.get(b"rec1", b"key0015").await.unwrap();
        assert!(deleted.is_none());

        let scan2 = engine
            .scan(b"rec1", None, None, RangeReadOptions::default())
            .await
            .unwrap();
        assert_eq!(scan2.entries.len(), 29);

        engine.close().await.unwrap();
    }

    {
        let (config, backend) = test_config(&dir, namespace);
        let engine = Engine::open(backend, config).await.unwrap();

        let deleted = engine.get(b"rec1", b"key0015").await.unwrap();
        assert!(deleted.is_none());

        let scan = engine
            .scan(b"rec1", None, None, RangeReadOptions::default())
            .await
            .unwrap();
        assert_eq!(scan.entries.len(), 29);

        let result = engine.get(b"rec1", b"key0010").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("val10"));
    }
}
