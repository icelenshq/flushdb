use bytes::Bytes;
use flushdb_types::{EntryType, FlushError, IdempotencyToken};
use flushdb_wal::{WalConfig, WalEntry, WalManager};

fn make_entry() -> WalEntry {
    WalEntry {
        sequence_number: 0,
        entry_type: EntryType::Put,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"key"),
        item_value: Bytes::from_static(b"value"),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    }
}

fn make_large_entry() -> WalEntry {
    WalEntry {
        sequence_number: 0,
        entry_type: EntryType::Put,
        namespace: Bytes::from_static(b"ns"),
        record_id: Bytes::from_static(b"rec"),
        item_key: Bytes::from_static(b"key"),
        item_value: Bytes::from(vec![0xAB; 512]),
        item_metadata: Bytes::new(),
        idempotency_token: IdempotencyToken::none(),
    }
}

// === Lifecycle Tests ===

#[tokio::test]
async fn test_open_creates_wal_directory() {
    let dir = tempfile::tempdir().unwrap();
    let wal_dir = dir.path().join("wal-partition");
    let config = WalConfig::default();
    let manager = WalManager::open(&wal_dir, config).unwrap();
    assert!(wal_dir.exists());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_open_on_existing_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();

    // First open and write
    {
        let mut manager = WalManager::open(dir.path(), config.clone()).unwrap();
        let notif = manager.append(make_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
        manager.shutdown().await.unwrap();
    }

    // Second open should resume
    let manager = WalManager::open(dir.path(), config).unwrap();
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shutdown_flushes_pending_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    let mut notifs = Vec::new();
    for _ in 0..5 {
        notifs.push(manager.append(make_entry(), 1).unwrap());
    }

    manager.shutdown().await.unwrap();

    for notif in notifs {
        let result = notif.await;
        assert!(result.is_ok());
    }

    let entries = WalManager::recover(dir.path()).unwrap();
    assert_eq!(entries.len(), 5);
}

// === Write Path Tests ===

#[tokio::test]
async fn test_append_returns_notification() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    let notif = manager.append(make_entry(), 1).unwrap();
    let result = notif.await.unwrap();
    assert!(result.is_ok());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_append_notification_fires() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    let notif = manager.append(make_entry(), 1).unwrap();
    let result = tokio::time::timeout(std::time::Duration::from_secs(5), notif)
        .await
        .expect("notification should fire within timeout");
    assert!(result.unwrap().is_ok());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_append_multiple_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    for _ in 0..100 {
        let notif = manager.append(make_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
    }
    manager.shutdown().await.unwrap();

    let entries = WalManager::recover(dir.path()).unwrap();
    assert_eq!(entries.len(), 100);
}

#[tokio::test]
async fn test_append_preserves_entry_data() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    let entry = WalEntry {
        sequence_number: 0,
        entry_type: EntryType::Delete,
        namespace: Bytes::from_static(b"prod"),
        record_id: Bytes::from_static(b"user-42"),
        item_key: Bytes::from_static(b"session"),
        item_value: Bytes::new(),
        item_metadata: Bytes::from_static(b"expired"),
        idempotency_token: IdempotencyToken::none(),
    };
    let notif = manager.append(entry, 1).unwrap();
    notif.await.unwrap().unwrap();
    manager.shutdown().await.unwrap();

    let entries = WalManager::recover(dir.path()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].namespace.as_ref(), b"prod");
    assert_eq!(entries[0].record_id.as_ref(), b"user-42");
    assert_eq!(entries[0].entry_type, EntryType::Delete);
}

// === Recovery Tests ===

#[tokio::test]
async fn test_recover_reads_all_entries() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    for _ in 0..20 {
        let notif = manager.append(make_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
    }
    manager.shutdown().await.unwrap();

    let entries = WalManager::recover(dir.path()).unwrap();
    assert_eq!(entries.len(), 20);
}

#[tokio::test]
async fn test_recover_from_filters_by_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    for _ in 0..20 {
        let notif = manager.append(make_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
    }
    manager.shutdown().await.unwrap();

    let entries = WalManager::recover_from(dir.path(), 10).unwrap();
    assert!(entries.iter().all(|e| e.sequence_number >= 10));
    assert_eq!(entries.len(), 11); // 10 through 20
}

#[tokio::test]
async fn test_recover_empty_wal() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let manager = WalManager::open(dir.path(), config).unwrap();
    manager.shutdown().await.unwrap();

    let entries = WalManager::recover(dir.path()).unwrap();
    assert!(entries.is_empty());
}

#[tokio::test]
async fn test_recover_after_crash_simulation() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    for _ in 0..10 {
        let notif = manager.append(make_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
    }

    // Don't call shutdown - simulate crash
    drop(manager);

    let entries = WalManager::recover(dir.path()).unwrap();
    assert_eq!(entries.len(), 10);
}

// === Backpressure Tests ===

#[tokio::test]
async fn test_backpressure_rejects_writes() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        max_wal_size: 256, // very small
        ..WalConfig::default()
    };
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    // Write until backpressure kicks in
    let mut rejected = false;
    for _ in 0..100 {
        match manager.append_if_not_full(make_large_entry(), 1) {
            Ok(notif) => {
                let _ = notif.await;
            }
            Err(FlushError::ResourceExhausted { .. }) => {
                rejected = true;
                break;
            }
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    assert!(rejected, "should have hit backpressure");
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_is_backpressured_reflects_wal_size() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        max_wal_size: 256,
        ..WalConfig::default()
    };
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    // Initially not backpressured (but may already exceed with header)
    for _ in 0..20 {
        match manager.append_if_not_full(make_large_entry(), 1) {
            Ok(notif) => {
                let _ = notif.await;
            }
            Err(FlushError::ResourceExhausted { .. }) => break,
            Err(e) => panic!("unexpected error: {e}"),
        }
    }

    // After many writes, should be backpressured
    let bp = manager.is_backpressured().unwrap();
    assert!(bp);
    manager.shutdown().await.unwrap();
}

// === Dirty Tracking Integration Tests ===

#[tokio::test]
async fn test_mark_flushed_returns_clean_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    let notif = manager.append(make_entry(), 1).unwrap();
    notif.await.unwrap().unwrap();

    let clean = manager.mark_generation_flushed(1).unwrap();
    // The current segment should be marked as clean since gen 1 was the only writer
    assert!(!clean.is_empty());
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_cleanup_deletes_clean_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        segment_size_target: 256,
        ..WalConfig::default()
    };
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    // Write enough to create multiple segments
    for _ in 0..50 {
        let notif = manager.append(make_large_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
    }

    let clean = manager.mark_generation_flushed(1).unwrap();
    assert!(
        !clean.is_empty(),
        "50 large entries with segment_size_target=256 should produce cleanable segments"
    );

    let count_before = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".wal")
        })
        .count();

    manager.cleanup_segments(&clean).unwrap();

    let count_after = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".wal")
        })
        .count();

    assert!(count_after < count_before, "segments should have been deleted");
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_cleanup_skips_dirty_segments() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    let notif = manager.append(make_entry(), 1).unwrap();
    notif.await.unwrap().unwrap();

    // Try to clean segment without marking as flushed (it's dirty)
    // Get a segment number that exists
    let count_before = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".wal")
        })
        .count();

    // Try to clean segment 1 which is dirty
    manager.cleanup_segments(&[1]).unwrap();

    let count_after = std::fs::read_dir(dir.path())
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".wal")
        })
        .count();

    assert_eq!(count_before, count_after, "dirty segments should not be deleted");
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_full_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig {
        segment_size_target: 256,
        ..WalConfig::default()
    };
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    // Write with gen 1
    for _ in 0..20 {
        let notif = manager.append(make_large_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
    }

    // Write with gen 2
    for _ in 0..20 {
        let notif = manager.append(make_large_entry(), 2).unwrap();
        notif.await.unwrap().unwrap();
    }

    // Flush gen 1 and cleanup
    let clean = manager.mark_generation_flushed(1).unwrap();
    if !clean.is_empty() {
        manager.cleanup_segments(&clean).unwrap();
    }

    // Gen 2 data should still be recoverable (its segments weren't cleaned)
    manager.shutdown().await.unwrap();

    let entries = WalManager::recover(dir.path()).unwrap();
    // All gen 2 entries must survive (their segments have unflushed gen 2 data).
    // Some gen 1 entries in mixed segments may also survive.
    assert!(
        entries.len() >= 20,
        "expected at least 20 entries (gen 2), got {}",
        entries.len()
    );
    // Sequences must be monotonically increasing
    for window in entries.windows(2) {
        assert!(
            window[1].sequence_number > window[0].sequence_number,
            "sequence not monotonic: {} vs {}",
            window[0].sequence_number,
            window[1].sequence_number
        );
    }
}

// === WAL Size Tests ===

#[tokio::test]
async fn test_wal_size_reflects_total_segment_size() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    let size_empty = manager.wal_size().unwrap();
    assert!(size_empty > 0, "even empty WAL has a segment with header");

    for _ in 0..10 {
        let notif = manager.append(make_entry(), 1).unwrap();
        notif.await.unwrap().unwrap();
    }

    let size_after = manager.wal_size().unwrap();
    assert!(
        size_after > size_empty,
        "WAL size should grow after writes"
    );

    // Verify it matches actual disk usage
    let mut disk_total = 0u64;
    for entry in std::fs::read_dir(dir.path()).unwrap() {
        let entry = entry.unwrap();
        if entry
            .file_name()
            .to_string_lossy()
            .ends_with(".wal")
        {
            disk_total += entry.metadata().unwrap().len();
        }
    }
    assert_eq!(size_after, disk_total);
    manager.shutdown().await.unwrap();
}

// === Flush Trigger Tests ===

#[tokio::test]
async fn test_flush_triggers_oldest_pinned_generation() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let mut manager = WalManager::open(dir.path(), config).unwrap();

    // Write entries across two generations
    let notif = manager.append(make_entry(), 5).unwrap();
    notif.await.unwrap().unwrap();
    let notif = manager.append(make_entry(), 10).unwrap();
    notif.await.unwrap().unwrap();

    let triggers = manager.flush_triggers().unwrap();
    // oldest_pinned_generation picks the smallest generation from the oldest dirty segment.
    // Both generations land on the same segment, and generations_for_segment returns sorted IDs,
    // so the first (smallest) is 5.
    assert_eq!(triggers.oldest_pinned_generation, Some(5));

    // After flushing generation 5, the oldest pinned generation should become 10
    manager.mark_generation_flushed(5).unwrap();
    let triggers = manager.flush_triggers().unwrap();
    assert_eq!(triggers.oldest_pinned_generation, Some(10));

    // After flushing generation 10, no pinned generations remain
    manager.mark_generation_flushed(10).unwrap();
    let triggers = manager.flush_triggers().unwrap();
    assert_eq!(triggers.oldest_pinned_generation, None);

    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_flush_triggers_none_when_healthy() {
    let dir = tempfile::tempdir().unwrap();
    let config = WalConfig::default();
    let manager = WalManager::open(dir.path(), config).unwrap();

    let triggers = manager.flush_triggers().unwrap();
    assert!(triggers.age_triggered_segments.is_empty());
    assert!(!triggers.size_pressure);
    assert!(!triggers.backpressure);
    manager.shutdown().await.unwrap();
}
