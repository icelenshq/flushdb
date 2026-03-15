use tempfile::TempDir;

use flushdb_engine::{
    Level, ManifestConfig, ManifestManager, ManifestUpdate, ManifestUpdateTrigger, SSTableMeta,
};
use flushdb_types::{LocalFsBackend, StorageBackend};

fn setup() -> (TempDir, LocalFsBackend) {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let backend = LocalFsBackend::new(storage_dir);
    (dir, backend)
}

fn test_sst_meta(id: &str, min_key: &[u8], max_key: &[u8], size: u64) -> SSTableMeta {
    SSTableMeta {
        id: id.to_string(),
        size_bytes: size,
        entry_count: 100,
        min_key: min_key.to_vec(),
        max_key: max_key.to_vec(),
        bloom_filter_offset: 0,
        bloom_filter_size: 0,
        index_offset: 0,
        index_size: 0,
        created_at_ms: 1000,
        sequence_range: (1, 100),
        record_id_count: 10,
        run_id: None,
        fragment_index: None,
        dedup_block_size: 0,
    }
}

#[tokio::test]
async fn test_load_latest_empty_namespace() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);

    let manifest = manager.load_latest().await.unwrap();
    assert_eq!(manifest.l0_count(), 0);
    assert_eq!(manifest.total_sstable_count(), 0);
}

#[tokio::test]
async fn test_load_latest_picks_highest_id() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());

    // Bootstrap manifest
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    // Add an SSTable
    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(update).await.unwrap();

    // Load from a new manager — should see the update
    let mut manager2 = ManifestManager::new(backend, "test-ns".into(), config);
    let manifest = manager2.load_latest().await.unwrap();
    assert_eq!(manifest.l0_count(), 1);
}

#[tokio::test]
async fn test_update_increments_manifest_id() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let id_before = manager.current().manifest_id;

    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(update).await.unwrap();

    assert!(manager.current().manifest_id > id_before);
}

#[tokio::test]
async fn test_update_sets_previous_id() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let id_before = manager.current().manifest_id;

    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(update).await.unwrap();

    assert_eq!(manager.current().previous_manifest_id, id_before);
}

#[tokio::test]
async fn test_epoch_fencing_rejects_stale_writer() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();

    // Manually set a higher epoch in the manifest to simulate another writer
    manager.acquire_writer_epoch().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap(); // epoch = 2

    // Reset our epoch to 1 (stale)
    manager.writer_epoch = 1;

    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: 1,
        compactor_epoch: 0,
    };

    let result = manager.update(update).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        flushdb_types::FlushError::EpochFenced { .. } => {}
        e => panic!("expected EpochFenced, got {:?}", e),
    }
}

#[tokio::test]
async fn test_update_rejects_removing_nonexistent() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Compaction,
        add_sstables: vec![],
        remove_sstables: vec![(Level::L0, "nonexistent".into())],
        new_last_flushed_sequence: None,
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };

    let result = manager.update(update).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_update_rejects_decreasing_sequence() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    // First update sets sequence to 100
    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(update).await.unwrap();

    // Second update tries to decrease
    let meta2 = test_sst_meta("sst2", b"a\x00", b"z\x00", 1000);
    let update2 = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta2)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(50),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };

    let result = manager.update(update2).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_compaction_add_and_remove() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    // Add L0 SSTables
    let meta1 = test_sst_meta("sst1", b"a\x00", b"m\x00", 1000);
    let meta2 = test_sst_meta("sst2", b"n\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta1), (Level::L0, meta2)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(200),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(update).await.unwrap();
    assert_eq!(manager.current().l0_count(), 2);

    // Compaction: remove from L0, add to L1
    let l1_meta = test_sst_meta("sst3-l1", b"a\x00", b"z\x00", 2000);
    let compaction_update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Compaction,
        add_sstables: vec![(Level::L1, l1_meta)],
        remove_sstables: vec![
            (Level::L0, "sst1".into()),
            (Level::L0, "sst2".into()),
        ],
        new_last_flushed_sequence: None,
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(compaction_update).await.unwrap();

    assert_eq!(manager.current().l0_count(), 0);
    assert_eq!(manager.current().sstables_at_level(Level::L1).len(), 1);
}

#[tokio::test]
async fn test_refresh_picks_up_external_update() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();

    let mut manager1 = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());
    manager1.load_latest().await.unwrap();
    manager1.acquire_writer_epoch().await.unwrap();

    // manager1 adds an SSTable
    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: manager1.writer_epoch,
        compactor_epoch: manager1.compactor_epoch,
    };
    manager1.update(update).await.unwrap();

    // manager2 refreshes and sees the update
    let mut manager2 = ManifestManager::new(backend, "test-ns".into(), config);
    manager2.load_latest().await.unwrap();
    assert_eq!(manager2.current().l0_count(), 1);
}

// === load_specific Tests ===

#[tokio::test]
async fn test_load_specific_loads_correct_id() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let first_id = manager.current().manifest_id;

    // Produce a second version
    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(update).await.unwrap();
    assert!(manager.current().manifest_id > first_id);

    // Load the first version by ID
    let manifest = manager.load_specific(first_id).await.unwrap();
    assert_eq!(manifest.manifest_id, first_id);
    assert_eq!(manifest.l0_count(), 0);
}

#[tokio::test]
async fn test_load_specific_not_found_errors() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();

    let nonexistent = flushdb_engine::ManifestId::new(99999);
    let result = manager.load_specific(nonexistent).await;
    assert!(result.is_err());
}

// === acquire_compactor_epoch Tests ===

#[tokio::test]
async fn test_acquire_compactor_epoch_increments() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();

    assert_eq!(manager.compactor_epoch, 0);

    let epoch1 = manager.acquire_compactor_epoch().await.unwrap();
    assert_eq!(epoch1, 1);
    assert_eq!(manager.compactor_epoch, 1);
    assert_eq!(manager.current().compactor_epoch, 1);

    let epoch2 = manager.acquire_compactor_epoch().await.unwrap();
    assert_eq!(epoch2, 2);
}

// === check_writer_epoch / check_compactor_epoch Tests ===

#[tokio::test]
async fn test_check_writer_epoch_stale_returns_error() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap(); // epoch = 2

    // Simulate stale writer
    manager.writer_epoch = 1;
    let result = manager.check_writer_epoch();
    assert!(result.is_err());
    match result.unwrap_err() {
        flushdb_types::FlushError::EpochFenced { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        e => panic!("expected EpochFenced, got {:?}", e),
    }
}

#[tokio::test]
async fn test_check_writer_epoch_current_is_ok() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    assert!(manager.check_writer_epoch().is_ok());
}

#[tokio::test]
async fn test_check_compactor_epoch_stale_returns_error() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_compactor_epoch().await.unwrap();
    manager.acquire_compactor_epoch().await.unwrap(); // epoch = 2

    manager.compactor_epoch = 1;
    let result = manager.check_compactor_epoch();
    assert!(result.is_err());
    match result.unwrap_err() {
        flushdb_types::FlushError::EpochFenced { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 2);
        }
        e => panic!("expected EpochFenced, got {:?}", e),
    }
}

// === prune_old_manifests Tests ===

#[tokio::test]
async fn test_prune_skips_when_few_entries() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();

    // Only 1 entry (the initial manifest) — nothing to prune
    let deleted = manager.prune_old_manifests().await.unwrap();
    assert_eq!(deleted, 0);
}

#[tokio::test]
async fn test_prune_skips_when_fewer_than_two_snapshots() {
    let (_dir, backend) = setup();
    let config = ManifestConfig {
        snapshot_interval: 1000, // Won't trigger snapshots
        ..ManifestConfig::default()
    };
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    // Generate a few manifests
    for i in 0..3 {
        let meta = test_sst_meta(&format!("sst{i}"), b"a\x00", b"z\x00", 1000);
        let update = ManifestUpdate {
            trigger: ManifestUpdateTrigger::Flush,
            add_sstables: vec![(Level::L0, meta)],
            remove_sstables: vec![],
            new_last_flushed_sequence: Some((i + 1) * 100),
            writer_epoch: manager.writer_epoch,
            compactor_epoch: manager.compactor_epoch,
        };
        manager.update(update).await.unwrap();
    }

    // No snapshots => can't prune
    let deleted = manager.prune_old_manifests().await.unwrap();
    assert_eq!(deleted, 0);
}

// === update rejects duplicate add Tests ===

#[tokio::test]
async fn test_update_rejects_duplicate_add() {
    let (_dir, backend) = setup();
    let config = ManifestConfig::default();
    let mut manager = ManifestManager::new(backend, "test-ns".into(), config);
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    let meta = test_sst_meta("sst1", b"a\x00", b"z\x00", 1000);
    let update = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta.clone())],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(100),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    manager.update(update).await.unwrap();

    // Try to add the same SSTable again
    let update2 = ManifestUpdate {
        trigger: ManifestUpdateTrigger::Flush,
        add_sstables: vec![(Level::L0, meta)],
        remove_sstables: vec![],
        new_last_flushed_sequence: Some(200),
        writer_epoch: manager.writer_epoch,
        compactor_epoch: manager.compactor_epoch,
    };
    let result = manager.update(update2).await;
    assert!(result.is_err());
}

// === prune_old_manifests deletion Tests ===

#[tokio::test]
async fn test_prune_deletes_manifests_before_second_newest_snapshot() {
    let (_dir, backend) = setup();
    let config = ManifestConfig {
        snapshot_interval: 5,
        ..ManifestConfig::default()
    };
    let mut manager = ManifestManager::new(backend.clone(), "test-ns".into(), config.clone());
    manager.load_latest().await.unwrap();
    manager.acquire_writer_epoch().await.unwrap();

    // Generate 20+ manifest updates to produce multiple snapshots.
    // Initial manifest is ID 0, acquire_writer_epoch creates ID 1,
    // then each update increments by 1. With snapshot_interval=5,
    // IDs 5, 10, 15, 20 will be snapshots.
    for i in 0..20u64 {
        let meta = test_sst_meta(
            &format!("sst-prune-{i}"),
            b"a\x00",
            b"z\x00",
            100,
        );
        let update = ManifestUpdate {
            trigger: ManifestUpdateTrigger::Flush,
            add_sstables: vec![(Level::L0, meta)],
            remove_sstables: vec![],
            new_last_flushed_sequence: Some((i + 1) * 10),
            writer_epoch: manager.writer_epoch,
            compactor_epoch: manager.compactor_epoch,
        };
        manager.update(update).await.unwrap();
    }

    // Count manifest files before pruning
    let prefix = format!("flushdb/test-ns/manifests/");
    let before = backend.list_prefix(&prefix).await.unwrap();
    let count_before = before.len();
    assert!(
        count_before > 10,
        "should have many manifest files, got {}",
        count_before
    );

    let deleted = manager.prune_old_manifests().await.unwrap();
    assert!(
        deleted > 0,
        "prune should have deleted old manifests, but deleted 0"
    );

    // Verify files are actually gone
    let after = backend.list_prefix(&prefix).await.unwrap();
    assert_eq!(
        after.len(),
        count_before - deleted,
        "file count should decrease by the number of pruned manifests"
    );

    // The latest manifest should still be loadable
    let mut manager2 = ManifestManager::new(backend, "test-ns".into(), config);
    let manifest = manager2.load_latest().await.unwrap();
    assert_eq!(manifest.l0_count(), 20);
}
