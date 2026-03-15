use bytes::Bytes;
use tempfile::TempDir;

use flushdb_engine::memtable::MemtableConfig;
use flushdb_engine::recovery::{RecoveryConfig, recover};
use flushdb_engine::{DirectBlockFetcher, ManifestConfig};
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
