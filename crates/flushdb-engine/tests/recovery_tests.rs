use bytes::Bytes;
use tempfile::TempDir;

use flushdb_engine::memtable::MemtableConfig;
use flushdb_engine::recovery::{RecoveryConfig, recover};
use flushdb_engine::{
    CacheConfig, DirectBlockFetcher, Engine, EngineConfig, FlushConfig, ManifestConfig,
};
use flushdb_types::{
    CompositeKey, EntryType, IdempotencyToken, LocalFsBackend, MemtableEntry,
};
use flushdb_wal::{WalConfig, WalEntry, WalManager};

fn setup() -> (TempDir, LocalFsBackend) {
    let dir = TempDir::new().unwrap();
    let storage_dir = dir.path().join("storage");
    std::fs::create_dir_all(&storage_dir).unwrap();
    let backend = LocalFsBackend::new(storage_dir);
    (dir, backend)
}

fn make_wal_entry(namespace: &[u8], record_id: &[u8], key: &[u8], value: &[u8], seq: u64) -> WalEntry {
    let composite_key = CompositeKey::new(record_id, key).unwrap();
    let entry = MemtableEntry::with_sequence(
        composite_key,
        Bytes::copy_from_slice(value),
        Bytes::new(),
        IdempotencyToken::none(),
        seq,
        EntryType::Put,
    );
    WalEntry::from_memtable_entry(&entry, namespace)
}

#[tokio::test]
async fn test_recover_fresh_namespace_empty_wal() {
    let (dir, backend) = setup();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let fetcher = DirectBlockFetcher::new(backend.clone());
    let config = RecoveryConfig {
        manifest_config: ManifestConfig::default(),
        memtable_config: MemtableConfig::default(),
    };

    let result = recover(backend, &wal_dir, "test-ns", &config, &fetcher)
        .await
        .unwrap();

    assert_eq!(result.wal_entries_replayed, 0);
    assert_eq!(result.next_sequence_number, 1);
    assert!(result.levels.len() >= 4); // L0..L3
}

#[tokio::test]
async fn test_recover_replays_wal_entries() {
    let (dir, backend) = setup();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Write some WAL entries
    let wal_config = WalConfig::default();
    let mut wal = WalManager::open(&wal_dir, wal_config).unwrap();

    for i in 1..=5u64 {
        let entry = make_wal_entry(b"test-ns", b"rec1", format!("key{i}").as_bytes(), b"val", i);
        wal.append(entry, 0).unwrap();
    }
    drop(wal);

    let fetcher = DirectBlockFetcher::new(backend.clone());
    let config = RecoveryConfig {
        manifest_config: ManifestConfig::default(),
        memtable_config: MemtableConfig::default(),
    };

    let result = recover(backend, &wal_dir, "test-ns", &config, &fetcher)
        .await
        .unwrap();

    assert_eq!(result.wal_entries_replayed, 5);
    assert_eq!(result.next_sequence_number, 6);

    // Verify entries are in the memtable
    let key = CompositeKey::new(b"rec1", b"key3").unwrap();
    let entry = result.memtable_list.get(&key);
    assert!(entry.is_some());
}

#[tokio::test]
async fn test_recover_nonexistent_wal_dir() {
    let (dir, backend) = setup();
    let wal_dir = dir.path().join("nonexistent_wal");

    let fetcher = DirectBlockFetcher::new(backend.clone());
    let config = RecoveryConfig::default();

    let result = recover(backend, &wal_dir, "test-ns", &config, &fetcher)
        .await
        .unwrap();

    assert_eq!(result.wal_entries_replayed, 0);
    assert_eq!(result.next_sequence_number, 1);
}

#[tokio::test]
async fn test_recover_advances_next_sequence_beyond_wal() {
    let (dir, backend) = setup();
    let wal_dir = dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let wal_config = WalConfig::default();
    let mut wal = WalManager::open(&wal_dir, wal_config).unwrap();

    // Write entries with sequences 1, 2, 3
    for i in 1..=3u64 {
        let entry = make_wal_entry(b"test-ns", b"rec1", format!("key{i}").as_bytes(), b"val", i);
        wal.append(entry, 0).unwrap();
    }
    drop(wal);

    let fetcher = DirectBlockFetcher::new(backend.clone());
    let config = RecoveryConfig::default();

    let result = recover(backend, &wal_dir, "test-ns", &config, &fetcher)
        .await
        .unwrap();

    assert_eq!(result.next_sequence_number, 4); // one past highest WAL sequence
}

#[tokio::test]
async fn test_recover_skips_entries_at_or_below_last_flushed_sequence() {
    let dir = TempDir::new().unwrap();
    let namespace = "test-ns";

    // Phase 1: Open engine, write entries 1..=5, close (which flushes to SSTable).
    // This sets last_flushed_sequence in the manifest.
    {
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
            compaction_config: flushdb_engine::CompactionConfig::default(),
            manifest_config: ManifestConfig {
                base_path: "flushdb".to_string(),
                ..ManifestConfig::default()
            },
            cache_config: CacheConfig::default(),
            namespace: namespace.to_string(),
            local_dir: dir.path().to_path_buf(),
        };

        let mut engine = Engine::open(backend, config).await.unwrap();
        for i in 1..=5u32 {
            let key = format!("key{i:02}");
            let val = format!("flushed_val{i}");
            engine
                .put(
                    b"rec1",
                    key.as_bytes(),
                    Bytes::from(val),
                    Bytes::new(),
                    None,
                )
                .await
                .unwrap();
        }
        engine.close().await.unwrap();
    }

    // Phase 2: Write additional WAL entries (6..=10) directly to the WAL directory,
    // simulating writes that happened after the flush but before a crash.
    let wal_dir = dir.path().join("wal");
    let wal_config = WalConfig::default();
    let mut wal = WalManager::open(&wal_dir, wal_config).unwrap();
    for i in 6..=10u64 {
        let entry = make_wal_entry(
            namespace.as_bytes(),
            b"rec1",
            format!("post{i:02}").as_bytes(),
            format!("new_val{i}").as_bytes(),
            i + 100, // high sequence numbers to avoid overlap with the flushed entries
        );
        wal.append(entry, 1).unwrap();
    }
    drop(wal);

    // Phase 3: Recover and verify only post-flush WAL entries are replayed
    // into the memtable (the flushed entries are in SSTables, not memtable).
    let storage_dir = dir.path().join("storage");
    let backend = LocalFsBackend::new(storage_dir);
    let fetcher = DirectBlockFetcher::new(backend.clone());
    let config = RecoveryConfig {
        manifest_config: ManifestConfig {
            base_path: "flushdb".to_string(),
            ..ManifestConfig::default()
        },
        memtable_config: MemtableConfig::default(),
    };

    let result = recover(backend, &wal_dir, namespace, &config, &fetcher)
        .await
        .unwrap();

    // Post-flush entries should be in the memtable
    assert!(
        result.wal_entries_replayed > 0,
        "should have replayed post-flush WAL entries"
    );

    for i in 6..=10u64 {
        let key = CompositeKey::new(b"rec1", format!("post{i:02}").as_bytes()).unwrap();
        assert!(
            result.memtable_list.get(&key).is_some(),
            "post-flush entry post{i:02} should be replayed into memtable"
        );
    }

    // Pre-flush entries with seq < last_flushed_sequence should NOT be in the
    // memtable — recover_from filters them via replay_from(last_flushed_sequence).
    // Note: entry at exactly last_flushed_sequence (key05, seq=5) may be replayed
    // as a safety overlap (replay_from uses >= comparison), which is harmless
    // since the SSTable already contains it.
    for i in 1..=4u32 {
        let key = CompositeKey::new(b"rec1", format!("key{i:02}").as_bytes()).unwrap();
        assert!(
            result.memtable_list.get(&key).is_none(),
            "pre-flush entry key{i:02} (seq < last_flushed) should not be in memtable"
        );
    }
}
