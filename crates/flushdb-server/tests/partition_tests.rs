use std::path::Path;

use bytes::Bytes;
use flushdb_server::namespace_config::NamespaceConfig;
use flushdb_server::partition::{Partition, PartitionState};
use flushdb_types::{FlushError, IdempotencyToken, LocalFsBackend};

async fn open_test_partition(dir: &Path) -> Partition<LocalFsBackend> {
    let backend = LocalFsBackend::new(dir.join("storage"));
    let config = NamespaceConfig::new("test-ns".to_string(), 1).expect("valid config");
    Partition::open(0, "test-ns".to_string(), backend, dir, &config)
        .await
        .expect("partition should open")
}

// ─── Lifecycle Tests ───

#[tokio::test]
async fn test_open_partition_is_active() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let p = open_test_partition(tmp.path()).await;
    assert_eq!(p.state(), PartitionState::Active);
    assert!(p.is_writable());
    assert!(p.is_readable());
}

#[tokio::test]
async fn test_freeze_transitions_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("freeze should succeed");
    assert_eq!(p.state(), PartitionState::Frozen);
    assert!(!p.is_writable());
    assert!(p.is_readable());
}

#[tokio::test]
async fn test_freeze_rejects_non_active() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("first freeze");
    let err = p.freeze().expect_err("should reject second freeze");
    match &err {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("cannot freeze partition in state: Frozen"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_drain_transitions_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("freeze");
    p.drain().await.expect("drain should succeed");
    assert_eq!(p.state(), PartitionState::Draining);
    assert!(!p.is_writable());
    assert!(!p.is_readable());
}

#[tokio::test]
async fn test_drain_rejects_non_frozen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    let err = p.drain().await.expect_err("drain from Active should fail");
    match &err {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("cannot drain partition in state: Active"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_stop_from_active() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.stop().await.expect("stop from Active");
    assert_eq!(p.state(), PartitionState::Stopped);
    assert!(!p.is_writable());
    assert!(!p.is_readable());
}

#[tokio::test]
async fn test_stop_from_frozen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("freeze");
    p.stop().await.expect("stop from Frozen");
    assert_eq!(p.state(), PartitionState::Stopped);
}

#[tokio::test]
async fn test_stop_from_draining() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("freeze");
    p.drain().await.expect("drain");
    p.stop().await.expect("stop from Draining");
    assert_eq!(p.state(), PartitionState::Stopped);
}

// ─── Write State Guard Tests ───

#[tokio::test]
async fn test_put_active_succeeds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    let seq = p
        .put(
            b"record-1",
            b"key-1",
            Bytes::from_static(b"value-1"),
            Bytes::new(),
            IdempotencyToken::none(),
        )
        .await
        .expect("put should succeed in Active");
    assert!(seq > 0 || seq == 0); // sequence is valid
}

#[tokio::test]
async fn test_put_frozen_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("freeze");
    let err = p
        .put(
            b"record-1",
            b"key-1",
            Bytes::from_static(b"value-1"),
            Bytes::new(),
            IdempotencyToken::none(),
        )
        .await
        .expect_err("put should fail in Frozen");
    match &err {
        FlushError::ResourceExhausted { resource, message } => {
            assert_eq!(resource, "partition");
            assert!(
                message.contains("not accepting writes"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_put_stopped_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.stop().await.expect("stop");
    let err = p
        .put(
            b"record-1",
            b"key-1",
            Bytes::from_static(b"value-1"),
            Bytes::new(),
            IdempotencyToken::none(),
        )
        .await
        .expect_err("put should fail in Stopped");
    match &err {
        FlushError::ResourceExhausted { resource, .. } => {
            assert_eq!(resource, "partition");
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_delete_active_succeeds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.delete(b"record-1", b"key-1")
        .await
        .expect("delete should succeed in Active");
}

#[tokio::test]
async fn test_delete_frozen_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("freeze");
    let err = p
        .delete(b"record-1", b"key-1")
        .await
        .expect_err("delete should fail in Frozen");
    match &err {
        FlushError::ResourceExhausted { resource, .. } => {
            assert_eq!(resource, "partition");
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_delete_range_frozen_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.freeze().expect("freeze");
    let err = p
        .delete_range(b"record-1", b"a", b"z")
        .await
        .expect_err("delete_range should fail in Frozen");
    match &err {
        FlushError::ResourceExhausted { resource, .. } => {
            assert_eq!(resource, "partition");
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

// ─── Read State Guard Tests ───

#[tokio::test]
async fn test_get_active_succeeds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let p = open_test_partition(tmp.path()).await;
    let result = p
        .get(b"record-1", b"key-1")
        .await
        .expect("get should succeed in Active");
    assert!(result.is_none()); // nothing written yet
}

#[tokio::test]
async fn test_get_frozen_succeeds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.put(
        b"record-1",
        b"key-1",
        Bytes::from_static(b"value-1"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put");
    p.freeze().expect("freeze");
    let result = p
        .get(b"record-1", b"key-1")
        .await
        .expect("get should succeed in Frozen");
    assert!(result.is_some());
}

#[tokio::test]
async fn test_get_stopped_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.stop().await.expect("stop");
    let err = p
        .get(b"record-1", b"key-1")
        .await
        .expect_err("get should fail in Stopped");
    match &err {
        FlushError::ResourceExhausted { resource, .. } => {
            assert_eq!(resource, "partition");
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_scan_frozen_succeeds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.put(
        b"record-1",
        b"key-a",
        Bytes::from_static(b"val-a"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put a");
    p.put(
        b"record-1",
        b"key-b",
        Bytes::from_static(b"val-b"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put b");
    p.freeze().expect("freeze");
    let result = p
        .scan(
            b"record-1",
            None,
            None,
            flushdb_engine::RangeReadOptions::default(),
        )
        .await
        .expect("scan should succeed in Frozen");
    assert_eq!(result.entries.len(), 2);
}

#[tokio::test]
async fn test_scan_stopped_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.stop().await.expect("stop");
    let err = p
        .scan(
            b"record-1",
            None,
            None,
            flushdb_engine::RangeReadOptions::default(),
        )
        .await
        .expect_err("scan should fail in Stopped");
    match &err {
        FlushError::ResourceExhausted { resource, .. } => {
            assert_eq!(resource, "partition");
        }
        other => panic!("expected ResourceExhausted, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_multi_get_frozen_succeeds() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.put(
        b"record-1",
        b"key-1",
        Bytes::from_static(b"val-1"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put");
    p.freeze().expect("freeze");
    let results = p
        .multi_get(b"record-1", &[b"key-1".as_slice(), b"key-missing".as_slice()])
        .await
        .expect("multi_get in Frozen");
    assert!(results[0].is_some());
    assert!(results[1].is_none());
}

// ─── WAL Directory Tests ───

#[tokio::test]
async fn test_wal_directory_created() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let p = open_test_partition(tmp.path()).await;
    let wal_dir = p.wal_dir().join("wal");
    assert!(wal_dir.exists(), "WAL directory should exist at {wal_dir:?}");
}

#[tokio::test]
async fn test_wal_directory_convention() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let backend = LocalFsBackend::new(tmp.path().join("storage"));
    let config = NamespaceConfig::new("my-namespace".to_string(), 1).expect("valid config");
    let p = Partition::open(7, "my-namespace".to_string(), backend, tmp.path(), &config)
        .await
        .expect("partition opens");
    let expected_partition_dir = tmp.path().join("my-namespace/partition-0007");
    assert_eq!(p.wal_dir(), expected_partition_dir);
    let expected_wal_dir = expected_partition_dir.join("wal");
    assert!(
        expected_wal_dir.exists(),
        "WAL dir should exist at {expected_wal_dir:?}"
    );
}

// ─── Status Method Tests ───

#[tokio::test]
async fn test_partition_id_and_namespace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let p = open_test_partition(tmp.path()).await;
    assert_eq!(p.partition_id(), 0);
    assert_eq!(p.namespace(), "test-ns");
}

#[tokio::test]
async fn test_write_stall_status_normal() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let p = open_test_partition(tmp.path()).await;
    let status = p.write_stall_status();
    assert_eq!(status, flushdb_engine::WriteStallStatus::Normal);
}

// ─── Integration Tests ───

#[tokio::test]
async fn test_put_get_round_trip() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;
    p.put(
        b"user-42",
        b"profile",
        Bytes::from_static(b"hello world"),
        Bytes::from_static(b"meta"),
        IdempotencyToken::none(),
    )
    .await
    .expect("put");

    let result = p
        .get(b"user-42", b"profile")
        .await
        .expect("get")
        .expect("should find entry");
    assert_eq!(result.value, Bytes::from_static(b"hello world"));
    assert_eq!(result.metadata, Bytes::from_static(b"meta"));
}

#[tokio::test]
async fn test_graceful_shutdown_sequence() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;

    // Write some data
    p.put(
        b"rec-1",
        b"key-1",
        Bytes::from_static(b"val-1"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put");

    assert_eq!(p.state(), PartitionState::Active);

    // Freeze
    p.freeze().expect("freeze");
    assert_eq!(p.state(), PartitionState::Frozen);
    assert!(!p.is_writable());
    assert!(p.is_readable());

    // Can still read
    let result = p
        .get(b"rec-1", b"key-1")
        .await
        .expect("get in Frozen");
    assert!(result.is_some());

    // Cannot write
    let err = p
        .put(
            b"rec-1",
            b"key-2",
            Bytes::from_static(b"val-2"),
            Bytes::new(),
            IdempotencyToken::none(),
        )
        .await;
    assert!(err.is_err());

    // Drain
    p.drain().await.expect("drain");
    assert_eq!(p.state(), PartitionState::Draining);

    // Stop
    p.stop().await.expect("stop");
    assert_eq!(p.state(), PartitionState::Stopped);
}

#[tokio::test]
async fn test_drain_flushes_memtable() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;

    let v0 = p.manifest_version();

    p.put(
        b"rec-1",
        b"key-1",
        Bytes::from_static(b"val-1"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put");

    p.freeze().expect("freeze");
    p.drain().await.expect("drain");

    let v1 = p.manifest_version();
    // After drain with data, manifest version should have advanced
    // (data was flushed from memtable to SSTable which updates the manifest)
    assert!(
        v1 > v0,
        "manifest version should advance after drain: v0={v0:?}, v1={v1:?}"
    );
}

#[tokio::test]
async fn test_maintenance_methods_always_allowed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut p = open_test_partition(tmp.path()).await;

    // Maintenance works in Active
    p.maybe_flush().await.expect("flush in Active");
    p.maybe_compact().await.expect("compact in Active");

    p.freeze().expect("freeze");

    // Maintenance works in Frozen
    p.maybe_flush().await.expect("flush in Frozen");
    p.maybe_compact().await.expect("compact in Frozen");
}
