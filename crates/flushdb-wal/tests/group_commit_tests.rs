use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use bytes::Bytes;
use flushdb_types::{EntryType, IdempotencyToken};
use flushdb_wal::{
    FsyncMode, GroupCommitBuffer, WalConfig, WalEntry, WalReader, WalWriter,
};

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

fn setup(
    config: WalConfig,
) -> (
    tempfile::TempDir,
    GroupCommitBuffer,
    flushdb_wal::GroupCommitHandle,
    Arc<AtomicU64>,
) {
    let dir = tempfile::tempdir().unwrap();
    let writer = WalWriter::open(dir.path(), &config).unwrap();
    let seg = Arc::new(AtomicU64::new(writer.current_segment_number()));
    let (buffer, handle) = GroupCommitBuffer::new(writer, config, Arc::clone(&seg));
    (dir, buffer, handle, seg)
}

// === Basic Commit Tests ===

#[tokio::test]
async fn test_single_write_is_durable() {
    let (dir, buffer, _handle, _seg) = setup(WalConfig::default());
    let notification = buffer.submit(make_entry()).unwrap();
    let result = notification.await.unwrap();
    assert!(result.is_ok());

    // Verify entry is on disk
    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 1);
}

#[tokio::test]
async fn test_multiple_writes_all_notified() {
    let (dir, buffer, _handle, _seg) = setup(WalConfig::default());
    let mut notifications = Vec::new();
    for _ in 0..10 {
        notifications.push(buffer.submit(make_entry()).unwrap());
    }
    for notif in notifications {
        let result = notif.await.unwrap();
        assert!(result.is_ok());
    }

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 10);
}

#[tokio::test]
async fn test_write_data_preserved() {
    let (dir, buffer, _handle, _seg) = setup(WalConfig::default());
    let entry = WalEntry {
        sequence_number: 0,
        entry_type: EntryType::Delete,
        namespace: Bytes::from_static(b"production"),
        record_id: Bytes::from_static(b"user-42"),
        item_key: Bytes::from_static(b"session"),
        item_value: Bytes::new(),
        item_metadata: Bytes::from_static(b"expired"),
        idempotency_token: IdempotencyToken::none(),
    };
    let notif = buffer.submit(entry).unwrap();
    notif.await.unwrap().unwrap();

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].namespace.as_ref(), b"production");
    assert_eq!(entries[0].record_id.as_ref(), b"user-42");
    assert_eq!(entries[0].entry_type, EntryType::Delete);
}

// === Batching Tests ===

#[tokio::test]
async fn test_timer_trigger_commits_batch() {
    let config = WalConfig {
        group_commit_interval: std::time::Duration::from_millis(50),
        ..WalConfig::default()
    };
    let (_dir, buffer, _handle, _seg) = setup(config);

    let notif = buffer.submit(make_entry()).unwrap();
    // Wait for timer to fire
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), notif)
        .await
        .expect("notification should fire within timeout");
    assert!(result.unwrap().is_ok());
}

#[tokio::test]
async fn test_concurrent_writers() {
    let (dir, buffer, _handle, _seg) = setup(WalConfig::default());
    let buffer = Arc::new(buffer);

    let mut handles = Vec::new();
    for _ in 0..10 {
        let buf = Arc::clone(&buffer);
        handles.push(tokio::spawn(async move {
            let mut notifications = Vec::new();
            for _ in 0..10 {
                notifications.push(buf.submit(make_entry()).unwrap());
            }
            for notif in notifications {
                notif.await.unwrap().unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 100);
}

// === Ordering Tests ===

#[tokio::test]
async fn test_sequence_numbers_monotonic_across_batches() {
    let (dir, buffer, _handle, _seg) = setup(WalConfig::default());

    // Submit in batches with small delays to force separate batches
    for _ in 0..5 {
        let mut notifs = Vec::new();
        for _ in 0..5 {
            notifs.push(buffer.submit(make_entry()).unwrap());
        }
        for n in notifs {
            n.await.unwrap().unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 25);
    for window in entries.windows(2) {
        assert!(
            window[1].sequence_number > window[0].sequence_number,
            "sequence not monotonic: {} vs {}",
            window[0].sequence_number,
            window[1].sequence_number
        );
    }
}

// === FsyncMode Tests ===

#[tokio::test]
async fn test_batch_sync_mode_deferred_fsync() {
    let config = WalConfig {
        fsync_mode: FsyncMode::BatchSync,
        batch_sync_interval: std::time::Duration::from_millis(50),
        ..WalConfig::default()
    };
    let (_dir, buffer, _handle, _seg) = setup(config);

    let notif = buffer.submit(make_entry()).unwrap();
    // In batch sync mode, notification should fire quickly (before fsync)
    let result = tokio::time::timeout(std::time::Duration::from_secs(2), notif)
        .await
        .expect("notification should fire");
    assert!(result.unwrap().is_ok());
}

// === Submit-After-Shutdown Tests ===

#[tokio::test]
async fn test_submit_after_loop_exits_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    // Use a tiny segment so that a batch of writes triggers rotation,
    // which will fail once the directory is made read-only.
    let config = WalConfig {
        segment_size_target: 64,
        ..WalConfig::default()
    };
    let writer = WalWriter::open(dir.path(), &config).unwrap();
    let seg = Arc::new(AtomicU64::new(writer.current_segment_number()));
    let (buffer, handle) = GroupCommitBuffer::new(writer, config, seg);

    // Confirm the loop is alive with a successful write
    let notif = buffer.submit(make_entry()).unwrap();
    notif.await.unwrap().unwrap();

    // Make the directory read-only so the next segment rotation fails
    // when it tries to create a new segment file.
    let mut perms = std::fs::metadata(dir.path()).unwrap().permissions();
    perms.set_readonly(true);
    std::fs::set_permissions(dir.path(), perms.clone()).unwrap();

    // Submit entries until the commit loop hits the I/O error from
    // rotation and exits, causing subsequent submits to fail.
    let mut got_submit_error = false;
    for _ in 0..200 {
        match buffer.submit(make_entry()) {
            Err(_) => {
                got_submit_error = true;
                break;
            }
            Ok(notif) => {
                // The notification may carry the rotation I/O error.
                // Once the loop exits, the receiver is dropped and future
                // try_send calls will return Closed.
                let _ = notif.await;
                tokio::task::yield_now().await;
            }
        }
    }

    assert!(
        got_submit_error,
        "submit should return an error after the commit loop exits"
    );

    // Restore permissions so tempdir cleanup succeeds
    perms.set_readonly(false);
    std::fs::set_permissions(dir.path(), perms).unwrap();

    // Shutdown may return an error because the loop already exited
    let _ = handle.shutdown().await;
}

// === Shutdown Tests ===

#[tokio::test]
async fn test_shutdown_drains_pending_writes() {
    let (dir, buffer, handle, _seg) = setup(WalConfig::default());

    let mut notifications = Vec::new();
    for _ in 0..10 {
        notifications.push(buffer.submit(make_entry()).unwrap());
    }

    // Drop buffer to signal shutdown
    drop(buffer);
    handle.shutdown().await.unwrap();

    // All notifications should have fired
    for notif in notifications {
        let result = notif.await;
        assert!(result.is_ok());
    }

    let reader = WalReader::open(dir.path()).unwrap();
    let entries = reader.replay_all().unwrap();
    assert_eq!(entries.len(), 10);
}

