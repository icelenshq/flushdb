use bytes::Bytes;
use tempfile::TempDir;

use flushdb_engine::{
    CacheConfig, Engine, EngineConfig, FlushConfig, Level, ManifestConfig, MemtableConfig,
    CompactionConfig, RangeReadOptions, WriteStallStatus,
};
use flushdb_types::{IdempotencyToken, LocalFsBackend};
use flushdb_wal::WalConfig;

fn test_config(dir: &TempDir, namespace: &str) -> (EngineConfig, LocalFsBackend) {
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let backend = LocalFsBackend::new(storage_dir);
    let config = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 4096,
            max_frozen_count: 3,
            ..Default::default()
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
async fn test_engine_open_fresh() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");

    let engine = Engine::open(backend, config).await.unwrap();
    assert_eq!(engine.l0_count(), 0);
    assert_eq!(engine.frozen_memtable_count(), 0);
}

#[tokio::test]
async fn test_put_single_entry() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    let seq = engine
        .put(b"rec1", b"key1", Bytes::from("value1"), Bytes::new(), None)
        .await
        .unwrap();
    assert!(seq > 0);

    let result = engine.get(b"rec1", b"key1").await.unwrap();
    assert!(result.is_some());
    let get_result = result.unwrap();
    assert_eq!(get_result.value, Bytes::from("value1"));
}

#[tokio::test]
async fn test_put_multiple_entries() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..10u32 {
        let key = format!("key{:04}", i);
        let value = format!("value{}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from(value),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    for i in 0..10u32 {
        let key = format!("key{:04}", i);
        let expected_value = format!("value{}", i);
        let result = engine.get(b"rec1", key.as_bytes()).await.unwrap();
        assert!(result.is_some(), "key {} not found", key);
        assert_eq!(result.unwrap().value, Bytes::from(expected_value));
    }
}

#[tokio::test]
async fn test_put_overwrites_value() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    engine
        .put(b"rec1", b"key1", Bytes::from("v1"), Bytes::new(), None)
        .await
        .unwrap();
    engine
        .put(b"rec1", b"key1", Bytes::from("v2"), Bytes::new(), None)
        .await
        .unwrap();

    let result = engine.get(b"rec1", b"key1").await.unwrap();
    assert_eq!(result.unwrap().value, Bytes::from("v2"));
}

#[tokio::test]
async fn test_delete_entry() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    engine
        .put(b"rec1", b"key1", Bytes::from("v1"), Bytes::new(), None)
        .await
        .unwrap();
    engine.delete(b"rec1", b"key1").await.unwrap();

    let result = engine.get(b"rec1", b"key1").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_delete_range() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    // Put 10 entries
    for i in 0..10u32 {
        let key = format!("key{:04}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from("val"),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    // Delete range [key0003, key0007)
    engine
        .delete_range(b"rec1", b"key0003", b"key0007")
        .await
        .unwrap();

    // Keys outside range still exist
    assert!(engine.get(b"rec1", b"key0002").await.unwrap().is_some());
    assert!(engine.get(b"rec1", b"key0007").await.unwrap().is_some());

    // Keys inside range are deleted
    assert!(engine.get(b"rec1", b"key0003").await.unwrap().is_none());
    assert!(engine.get(b"rec1", b"key0005").await.unwrap().is_none());
    assert!(engine.get(b"rec1", b"key0006").await.unwrap().is_none());
}

#[tokio::test]
async fn test_scan_after_delete_range() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..10u32 {
        let key = format!("key{:04}", i);
        engine
            .put(b"rec1", key.as_bytes(), Bytes::from("val"), Bytes::new(), None)
            .await
            .unwrap();
    }

    engine.delete_range(b"rec1", b"key0003", b"key0007").await.unwrap();

    let result = engine
        .scan(b"rec1", None, None, RangeReadOptions::default())
        .await
        .unwrap();

    // Range-deleted keys should be excluded from scan as well
    let keys: Vec<Vec<u8>> = result.entries.iter().map(|e| e.composite_key.item_key().to_vec()).collect();
    assert!(!keys.contains(&b"key0003".to_vec()));
    assert!(!keys.contains(&b"key0005".to_vec()));
    assert!(!keys.contains(&b"key0006".to_vec()));
    assert!(keys.contains(&b"key0002".to_vec()));
    assert!(keys.contains(&b"key0007".to_vec()));
}

#[tokio::test]
async fn test_put_returns_monotonic_sequence() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    let seq1 = engine
        .put(b"r", b"k1", Bytes::from("v"), Bytes::new(), None)
        .await
        .unwrap();
    let seq2 = engine
        .put(b"r", b"k2", Bytes::from("v"), Bytes::new(), None)
        .await
        .unwrap();
    let seq3 = engine
        .put(b"r", b"k3", Bytes::from("v"), Bytes::new(), None)
        .await
        .unwrap();

    assert!(seq2 > seq1);
    assert!(seq3 > seq2);
}

#[tokio::test]
async fn test_dedup_rejects_duplicate_token() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    let token = IdempotencyToken::new(1);
    engine
        .put(
            b"rec1",
            b"key1",
            Bytes::from("v1"),
            Bytes::new(),
            Some(token),
        )
        .await
        .unwrap();

    let result = engine
        .put(
            b"rec1",
            b"key1",
            Bytes::from("v2"),
            Bytes::new(),
            Some(token),
        )
        .await;

    assert!(result.is_err());
    match result.unwrap_err() {
        flushdb_types::FlushError::DuplicateToken { .. } => {}
        e => panic!("expected DuplicateToken, got {:?}", e),
    }
}

#[tokio::test]
async fn test_scan_single_record() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..5u32 {
        let key = format!("key{:04}", i);
        let value = format!("value{}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from(value),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    let result = engine
        .scan(b"rec1", None, None, RangeReadOptions::default())
        .await
        .unwrap();

    assert_eq!(result.entries.len(), 5);
    // Verify sorted order
    for i in 0..5u32 {
        let key = format!("key{:04}", i);
        assert_eq!(
            result.entries[i as usize].composite_key.item_key(),
            key.as_bytes()
        );
    }
}

#[tokio::test]
async fn test_scan_bounded_range() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..10u32 {
        let key = format!("key{:04}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from("val"),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    let result = engine
        .scan(
            b"rec1",
            Some(b"key0003"),
            Some(b"key0007"),
            RangeReadOptions::default(),
        )
        .await
        .unwrap();

    assert_eq!(result.entries.len(), 4); // key0003, key0004, key0005, key0006
}

#[tokio::test]
async fn test_scan_pagination() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    for i in 0..20u32 {
        let key = format!("key{:04}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from("val"),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    let mut all_entries = Vec::new();
    let mut options = RangeReadOptions {
        page_size_bytes: 50, // Very small to force pagination
        item_limit: Some(5),
        resume_from: None,
    };

    loop {
        let result = engine
            .scan(b"rec1", None, None, options.clone())
            .await
            .unwrap();

        all_entries.extend(result.entries);

        match result.next_page_token {
            Some(token) => {
                options.resume_from = Some(token);
            }
            None => break,
        }
    }

    assert_eq!(all_entries.len(), 20);

    // Verify no duplicates
    let mut seen_keys: Vec<Vec<u8>> = Vec::new();
    for entry in &all_entries {
        let key = entry.composite_key.item_key().to_vec();
        assert!(!seen_keys.contains(&key), "duplicate key found");
        seen_keys.push(key);
    }
}

#[tokio::test]
async fn test_multi_get() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    engine
        .put(b"rec1", b"a", Bytes::from("va"), Bytes::new(), None)
        .await
        .unwrap();
    engine
        .put(b"rec1", b"b", Bytes::from("vb"), Bytes::new(), None)
        .await
        .unwrap();
    engine
        .put(b"rec1", b"c", Bytes::from("vc"), Bytes::new(), None)
        .await
        .unwrap();

    let keys: Vec<&[u8]> = vec![b"a", b"c", b"missing", b"b"];
    let results = engine.multi_get(b"rec1", &keys).await.unwrap();

    assert_eq!(results.len(), 4);
    assert_eq!(results[0].as_ref().unwrap().value, Bytes::from("va"));
    assert_eq!(results[1].as_ref().unwrap().value, Bytes::from("vc"));
    assert!(results[2].is_none());
    assert_eq!(results[3].as_ref().unwrap().value, Bytes::from("vb"));
}

#[tokio::test]
async fn test_get_nonexistent_key_returns_none() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let engine = Engine::open(backend, config).await.unwrap();

    let result = engine.get(b"rec1", b"nonexistent").await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_write_crash_recover_read() {
    let dir = TempDir::new().unwrap();
    let namespace = "test-ns";

    // Phase 1: Write data then drop engine (simulate crash)
    {
        let (config, backend) = test_config(&dir, namespace);
        let mut engine = Engine::open(backend, config).await.unwrap();

        for i in 0..50u32 {
            let key = format!("key{:04}", i);
            let value = format!("value{}", i);
            engine
                .put(
                    b"rec1",
                    key.as_bytes(),
                    Bytes::from(value),
                    Bytes::new(),
                    None,
                )
                .await
                .unwrap();
        }
        // Drop without close — simulates crash
    }

    // Phase 2: Reopen and verify all data is readable
    {
        let (config, backend) = test_config(&dir, namespace);
        let engine = Engine::open(backend, config).await.unwrap();

        for i in 0..50u32 {
            let key = format!("key{:04}", i);
            let expected = format!("value{}", i);
            let result = engine.get(b"rec1", key.as_bytes()).await.unwrap();
            assert!(result.is_some(), "key {} not found after recovery", key);
            assert_eq!(result.unwrap().value, Bytes::from(expected));
        }
    }
}

#[tokio::test]
async fn test_flush_then_read() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine: Engine<LocalFsBackend> = Engine::open(backend, config).await.unwrap();

    // Write enough to trigger a flush (memtable threshold is 4096 bytes)
    for i in 0..100u32 {
        let key = format!("key{:04}", i);
        let value = format!("value_{}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from(value),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    // Explicitly close to flush active memtable
    engine.close().await.unwrap();

    // Reopen - data should be in SSTables
    let (config2, backend2) = test_config(&dir, "test-ns");
    let engine = Engine::open(backend2, config2).await.unwrap();
    assert!(engine.l0_count() > 0, "expected L0 SSTables after close");

    // All entries still readable from SSTables
    for i in 0..100u32 {
        let key = format!("key{:04}", i);
        let expected = format!("value_{}", i);
        let result = engine.get(b"rec1", key.as_bytes()).await.unwrap();
        assert!(result.is_some(), "key {} not found after flush", key);
        assert_eq!(result.unwrap().value, Bytes::from(expected));
    }
}

#[tokio::test]
async fn test_close_flushes_active() {
    let dir = TempDir::new().unwrap();
    let namespace = "test-ns";

    // Write some data and close properly
    {
        let (config, backend) = test_config(&dir, namespace);
        let mut engine = Engine::open(backend, config).await.unwrap();

        engine
            .put(b"rec1", b"key1", Bytes::from("v1"), Bytes::new(), None)
            .await
            .unwrap();
        engine.close().await.unwrap();
    }

    // Reopen — data should be in SSTable (not just WAL)
    {
        let (config, backend) = test_config(&dir, namespace);
        let engine = Engine::open(backend, config).await.unwrap();

        let result = engine.get(b"rec1", b"key1").await.unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().value, Bytes::from("v1"));
        assert!(engine.l0_count() > 0, "expected flush to SSTable on close");
    }
}

#[tokio::test]
async fn test_mixed_puts_and_deletes() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    // Put 10 entries
    for i in 0..10u32 {
        engine
            .put(
                b"rec1",
                format!("k{:02}", i).as_bytes(),
                Bytes::from("v"),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    // Delete even keys
    for i in (0..10u32).step_by(2) {
        engine
            .delete(b"rec1", format!("k{:02}", i).as_bytes())
            .await
            .unwrap();
    }

    // Verify: odd keys exist, even keys don't
    for i in 0..10u32 {
        let result = engine
            .get(b"rec1", format!("k{:02}", i).as_bytes())
            .await
            .unwrap();
        if i % 2 == 0 {
            assert!(result.is_none(), "k{:02} should be deleted", i);
        } else {
            assert!(result.is_some(), "k{:02} should exist", i);
        }
    }
}

// === Status Method Tests ===

#[tokio::test]
async fn test_write_stall_normal_initially() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let engine = Engine::open(backend, config).await.unwrap();

    assert!(matches!(engine.write_stall_status(), WriteStallStatus::Normal));
}

#[tokio::test]
async fn test_manifest_accessor_returns_current() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let engine = Engine::open(backend, config).await.unwrap();

    let manifest = engine.manifest();
    assert_eq!(manifest.namespace, "test-ns");
}

#[tokio::test]
async fn test_level_sizes_returns_all_levels() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let engine = Engine::open(backend, config).await.unwrap();

    let sizes = engine.level_sizes();
    assert_eq!(sizes.len(), 4); // L0, L1, L2, L3
    for (_, size) in &sizes {
        assert_eq!(*size, 0);
    }
}

#[tokio::test]
async fn test_manifest_version_advances_after_flush() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    let version_before = engine.manifest_version();

    // Write enough to trigger flush (threshold is 4096)
    for i in 0..100u32 {
        engine
            .put(
                b"rec1",
                format!("key{:04}", i).as_bytes(),
                Bytes::from("value"),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }
    engine.close().await.unwrap();

    // Reopen and check
    let (config2, backend2) = test_config(&dir, "test-ns");
    let engine2 = Engine::open(backend2, config2).await.unwrap();
    assert!(engine2.manifest_version() > version_before);
}

#[tokio::test]
async fn test_consumed_sstable_ids_initially_empty() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let engine = Engine::open(backend, config).await.unwrap();
    assert!(engine.consumed_sstable_ids().is_empty());
}

#[tokio::test]
async fn test_schedule_and_run_deletion() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    // Schedule deletion of a nonexistent path — should tolerate not-found
    engine.schedule_deletion(vec!["nonexistent/path.sst".to_string()]);
    assert_eq!(engine.consumed_sstable_ids().len(), 1);

    let deleted = engine.run_deletion().await.unwrap();
    assert_eq!(deleted, 1); // NotFound is counted as deleted
    assert!(engine.consumed_sstable_ids().is_empty());
}

#[tokio::test]
async fn test_maybe_flush_when_no_frozen() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    engine
        .put(b"r", b"k", Bytes::from("v"), Bytes::new(), None)
        .await
        .unwrap();

    let result = engine.maybe_flush().await.unwrap();
    assert!(result.is_none());
}

#[tokio::test]
async fn test_full_lifecycle() {
    let dir = TempDir::new().unwrap();
    let namespace = "lifecycle";

    // Phase 1: Write entries across multiple records
    {
        let (config, backend) = test_config(&dir, namespace);
        let mut engine = Engine::open(backend, config).await.unwrap();

        for r in 0..5u32 {
            for k in 0..20u32 {
                let rec = format!("rec{:02}", r);
                let key = format!("key{:04}", k);
                let val = format!("r{}k{}", r, k);
                engine
                    .put(
                        rec.as_bytes(),
                        key.as_bytes(),
                        Bytes::from(val),
                        Bytes::new(),
                        None,
                    )
                    .await
                    .unwrap();
            }
        }

        engine.close().await.unwrap();
    }

    // Phase 2: Reopen, read, verify
    {
        let (config, backend) = test_config(&dir, namespace);
        let engine = Engine::open(backend, config).await.unwrap();

        for r in 0..5u32 {
            let rec = format!("rec{:02}", r);

            // Point reads
            for k in 0..20u32 {
                let key = format!("key{:04}", k);
                let expected = format!("r{}k{}", r, k);
                let result = engine.get(rec.as_bytes(), key.as_bytes()).await.unwrap();
                assert!(result.is_some());
                assert_eq!(result.unwrap().value, Bytes::from(expected));
            }

            // Range reads
            let scan = engine
                .scan(rec.as_bytes(), None, None, RangeReadOptions::default())
                .await
                .unwrap();
            assert_eq!(scan.entries.len(), 20, "record {} should have 20 items", rec);
        }
    }
}

#[tokio::test]
async fn test_maybe_compact_triggers_l0_to_l1() {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let backend = LocalFsBackend::new(storage_dir);
    let config = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 512,
            max_frozen_count: 3,
            ..Default::default()
        },
        wal_config: WalConfig::default(),
        flush_config: FlushConfig {
            sst_config: flushdb_engine::sstable::types::SstConfig::default(),
            max_frozen_count: 3,
            flush_trigger_size: 512,
            flush_trigger_age: std::time::Duration::from_secs(3600),
        },
        compaction_config: CompactionConfig {
            l0_compaction_trigger: 3,
            ..CompactionConfig::default()
        },
        manifest_config: ManifestConfig {
            base_path: "flushdb".to_string(),
            ..ManifestConfig::default()
        },
        cache_config: CacheConfig::default(),
        namespace: "compact-ns".to_string(),
        local_dir: dir.path().to_path_buf(),
    };

    let mut engine = Engine::open(backend, config).await.unwrap();

    // Write enough data to trigger multiple flushes (each at 512 bytes).
    // With l0_compaction_trigger = 3, the 4th L0 SSTable triggers L0->L1 compaction
    // inside flush_frozen -> maybe_compact.
    for i in 0..200u32 {
        let key = format!("key{:04}", i);
        let value = format!("value_{:04}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from(value),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    // Flush any remaining active memtable
    engine.close().await.unwrap();

    // Reopen to verify persistent state
    let storage_dir = dir.path().join("storage");
    let backend2 = LocalFsBackend::new(storage_dir);
    let config2 = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 512,
            max_frozen_count: 3,
            ..Default::default()
        },
        wal_config: WalConfig::default(),
        flush_config: FlushConfig {
            sst_config: flushdb_engine::sstable::types::SstConfig::default(),
            max_frozen_count: 3,
            flush_trigger_size: 512,
            flush_trigger_age: std::time::Duration::from_secs(3600),
        },
        compaction_config: CompactionConfig {
            l0_compaction_trigger: 3,
            ..CompactionConfig::default()
        },
        manifest_config: ManifestConfig {
            base_path: "flushdb".to_string(),
            ..ManifestConfig::default()
        },
        cache_config: CacheConfig::default(),
        namespace: "compact-ns".to_string(),
        local_dir: dir.path().to_path_buf(),
    };

    let mut engine2 = Engine::open(backend2, config2).await.unwrap();

    // Compaction should have moved data from L0 to L1
    let l1_size = engine2.manifest().level_size_bytes(Level::L1);
    assert!(
        l1_size > 0,
        "L1 should have data after compaction, but size is {}",
        l1_size
    );

    // Calling maybe_compact on a clean engine should succeed and return empty
    let results = engine2.maybe_compact().await.unwrap();
    assert!(
        results.is_empty(),
        "no pending compaction after L0->L1 was already done"
    );

    // Verify all data is still readable after compaction
    for i in 0..200u32 {
        let key = format!("key{:04}", i);
        let expected = format!("value_{:04}", i);
        let result = engine2.get(b"rec1", key.as_bytes()).await.unwrap();
        assert!(result.is_some(), "key {} not found after compaction", key);
        assert_eq!(result.unwrap().value, Bytes::from(expected));
    }
}

#[tokio::test]
async fn test_scan_spans_memtable_and_sstable() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    // Write k00–k04 into the engine
    for i in 0..5u32 {
        let key = format!("k{:02}", i);
        let value = format!("val{}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from(value),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    // Flush to SSTable by closing and reopening
    engine.close().await.unwrap();
    let (config2, backend2) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend2, config2).await.unwrap();
    assert!(engine.l0_count() > 0, "expected data in SSTables after close");

    // Write k05–k09 into the active memtable
    for i in 5..10u32 {
        let key = format!("k{:02}", i);
        let value = format!("val{}", i);
        engine
            .put(
                b"rec1",
                key.as_bytes(),
                Bytes::from(value),
                Bytes::new(),
                None,
            )
            .await
            .unwrap();
    }

    // Scan the full range — results should merge memtable + SSTable
    let result = engine
        .scan(b"rec1", None, None, RangeReadOptions::default())
        .await
        .unwrap();

    assert_eq!(
        result.entries.len(),
        10,
        "expected 10 entries spanning memtable + SSTable"
    );

    // Verify all 10 keys in sorted order
    for i in 0..10u32 {
        let expected_key = format!("k{:02}", i);
        let expected_val = format!("val{}", i);
        let entry = &result.entries[i as usize];
        assert_eq!(
            entry.composite_key.item_key(),
            expected_key.as_bytes(),
            "entry {} has wrong key",
            i
        );
        assert_eq!(
            entry.value,
            Bytes::from(expected_val),
            "entry {} has wrong value",
            i
        );
    }
}

// === run_maintenance Tests ===

#[tokio::test]
async fn test_run_maintenance_freezes_aged_memtable() {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let backend = LocalFsBackend::new(storage_dir);
    let config = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 4096,
            max_frozen_count: 3,
            ..Default::default()
        },
        wal_config: WalConfig::default(),
        flush_config: FlushConfig {
            sst_config: flushdb_engine::sstable::types::SstConfig::default(),
            max_frozen_count: 3,
            flush_trigger_size: 4096,
            flush_trigger_age: std::time::Duration::from_millis(1),
        },
        compaction_config: CompactionConfig::default(),
        manifest_config: ManifestConfig {
            base_path: "flushdb".to_string(),
            ..ManifestConfig::default()
        },
        cache_config: CacheConfig::default(),
        namespace: "maint-ns".to_string(),
        local_dir: dir.path().to_path_buf(),
    };

    let mut engine = Engine::open(backend, config).await.unwrap();

    engine
        .put(b"rec1", b"key1", Bytes::from("value1"), Bytes::new(), None)
        .await
        .unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    engine.run_maintenance().await.unwrap();

    assert_eq!(engine.frozen_memtable_count(), 0);
    assert!(engine.l0_count() > 0, "aged memtable should have been flushed to L0");
}

#[tokio::test]
async fn test_run_maintenance_skips_empty_active() {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let backend = LocalFsBackend::new(storage_dir);
    let config = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 4096,
            max_frozen_count: 3,
            ..Default::default()
        },
        wal_config: WalConfig::default(),
        flush_config: FlushConfig {
            sst_config: flushdb_engine::sstable::types::SstConfig::default(),
            max_frozen_count: 3,
            flush_trigger_size: 4096,
            flush_trigger_age: std::time::Duration::from_millis(1),
        },
        compaction_config: CompactionConfig::default(),
        manifest_config: ManifestConfig {
            base_path: "flushdb".to_string(),
            ..ManifestConfig::default()
        },
        cache_config: CacheConfig::default(),
        namespace: "maint-ns".to_string(),
        local_dir: dir.path().to_path_buf(),
    };

    let mut engine = Engine::open(backend, config).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    engine.run_maintenance().await.unwrap();

    assert_eq!(engine.frozen_memtable_count(), 0);
    assert_eq!(engine.l0_count(), 0);
}

#[tokio::test]
async fn test_run_maintenance_flushes_pending_frozen() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "maint-ns");

    let mut engine = Engine::open(backend, config).await.unwrap();

    // Write enough to trigger size-based freeze (threshold is 4096)
    for i in 0..100u32 {
        let key = format!("key{:04}", i);
        let value = format!("value_{}", i);
        engine
            .put(b"rec1", key.as_bytes(), Bytes::from(value), Bytes::new(), None)
            .await
            .unwrap();
    }

    // run_maintenance should flush any remaining frozen memtables
    engine.run_maintenance().await.unwrap();

    assert_eq!(engine.frozen_memtable_count(), 0);
    assert!(engine.l0_count() > 0, "frozen memtables should have been flushed");
}

#[tokio::test]
async fn test_write_stall_rejects_on_memory_pressure() {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();

    let backend = LocalFsBackend::new(storage_dir);
    let config = EngineConfig {
        memtable_config: MemtableConfig {
            size_threshold: 4096,
            max_frozen_count: 3,
            memtable_memory_limit: 256,
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
        namespace: "pressure-ns".to_string(),
        local_dir: dir.path().to_path_buf(),
    };

    let mut engine = Engine::open(backend, config).await.unwrap();

    let mut last_err = None;
    for i in 0..200u32 {
        let key = format!("key{:04}", i);
        let value = vec![0u8; 64];
        match engine
            .put(b"rec1", key.as_bytes(), Bytes::from(value), Bytes::new(), None)
            .await
        {
            Ok(_) => {}
            Err(e) => {
                last_err = Some(e);
                break;
            }
        }
    }

    let err = last_err.expect("should have hit memory pressure");
    match err {
        flushdb_types::FlushError::ResourceExhausted { resource, .. } => {
            assert_eq!(resource, "memtable_memory");
        }
        other => panic!("expected ResourceExhausted for memtable_memory, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_dedup_does_not_write_duplicate_to_wal() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");

    let token = IdempotencyToken::new(1);

    // Write, then attempt duplicate
    {
        let mut engine = Engine::open(backend.clone(), config.clone()).await.unwrap();
        engine
            .put(b"rec1", b"key1", Bytes::from("v1"), Bytes::new(), Some(token))
            .await
            .unwrap();

        let result = engine
            .put(b"rec1", b"key1", Bytes::from("v2"), Bytes::new(), Some(token))
            .await;
        assert!(result.is_err());
    }

    // Re-open (recovery replays WAL). If the duplicate was in WAL, recovery
    // would previously fail. Now it should succeed cleanly.
    let engine = Engine::open(backend, config).await.unwrap();
    let result = engine.get(b"rec1", b"key1").await.unwrap().unwrap();
    assert_eq!(result.value, Bytes::from("v1"));
}

#[tokio::test]
async fn test_dedup_does_not_consume_sequence_on_duplicate() {
    let dir = TempDir::new().unwrap();
    let (config, backend) = test_config(&dir, "test-ns");
    let mut engine = Engine::open(backend, config).await.unwrap();

    let token = IdempotencyToken::new(1);

    let seq1 = engine
        .put(b"rec1", b"key1", Bytes::from("v1"), Bytes::new(), Some(token))
        .await
        .unwrap();

    // Duplicate should fail
    let _ = engine
        .put(b"rec1", b"key1", Bytes::from("v2"), Bytes::new(), Some(token))
        .await;

    // Next successful write should get seq1 + 1 (no gap)
    let seq2 = engine
        .put(b"rec1", b"key2", Bytes::from("v3"), Bytes::new(), None)
        .await
        .unwrap();

    assert_eq!(seq2, seq1 + 1, "duplicate should not consume a sequence number");
}
