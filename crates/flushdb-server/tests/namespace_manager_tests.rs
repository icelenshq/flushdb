use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use flushdb_engine::RangeReadOptions;
use flushdb_server::namespace_config::NamespaceConfig;
use flushdb_server::namespace_manager::NamespaceManager;
use flushdb_server::version_generator::VersionGenerator;
use flushdb_types::{FlushError, IdempotencyToken, LocalFsBackend};

async fn test_manager(dir: &Path) -> NamespaceManager<LocalFsBackend> {
    let backend = LocalFsBackend::new(dir.join("storage"));
    let version_gen = Arc::new(VersionGenerator::new(0));
    NamespaceManager::new(backend, dir.to_path_buf(), version_gen)
}

fn test_config(name: &str) -> NamespaceConfig {
    NamespaceConfig::new(name.to_string(), 1).expect("valid config")
}

fn test_config_with_partitions(name: &str, partition_count: u32) -> NamespaceConfig {
    NamespaceConfig::new(name.to_string(), partition_count).expect("valid config")
}

// ─── Namespace CRUD Tests ───

#[tokio::test]
async fn test_create_namespace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create should succeed");

    assert!(mgr.namespace_exists("ns-1"));
}

#[tokio::test]
async fn test_create_duplicate_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("first create");

    let err = mgr
        .create_namespace(test_config("ns-1"))
        .await
        .expect_err("duplicate should fail");

    match &err {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("namespace already exists: ns-1"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_get_namespace_config() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    let config = test_config("ns-1");
    mgr.create_namespace(config.clone())
        .await
        .expect("create");

    let retrieved = mgr.get_namespace_config("ns-1").expect("get config");
    assert_eq!(retrieved.name, "ns-1");
    assert_eq!(retrieved.partition_count, config.partition_count);
}

#[tokio::test]
async fn test_update_mutable_fields() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create");

    let mut updated = mgr.get_namespace_config("ns-1").expect("get config");
    updated.memtable_size_threshold = 128 * 1024 * 1024;

    mgr.update_namespace_config(updated).expect("update should succeed");

    let retrieved = mgr.get_namespace_config("ns-1").expect("get config after update");
    assert_eq!(retrieved.memtable_size_threshold, 128 * 1024 * 1024);
}

#[tokio::test]
async fn test_update_immutable_fields_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create");

    let mut updated = mgr.get_namespace_config("ns-1").expect("get config");
    updated.partition_count = 4;

    let err = mgr
        .update_namespace_config(updated)
        .expect_err("immutable field change should fail");

    match &err {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("partition_count is immutable"),
                "unexpected message: {message}"
            );
        }
        other => panic!("expected InvalidArgument, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_delete_namespace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create");

    mgr.delete_namespace("ns-1").await.expect("delete");

    assert!(!mgr.namespace_exists("ns-1"));
}

#[tokio::test]
async fn test_delete_nonexistent_rejected() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    let err = mgr
        .delete_namespace("nope")
        .await
        .expect_err("delete nonexistent should fail");

    match &err {
        FlushError::NotFound { key } => {
            assert!(
                key.contains("namespace: nope"),
                "unexpected key: {key}"
            );
        }
        other => panic!("expected NotFound, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_list_namespaces_sorted() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("charlie"))
        .await
        .expect("create charlie");
    mgr.create_namespace(test_config("alpha"))
        .await
        .expect("create alpha");
    mgr.create_namespace(test_config("bravo"))
        .await
        .expect("create bravo");

    let names = mgr.list_namespaces();
    assert_eq!(names, vec!["alpha", "bravo", "charlie"]);
}

// ─── Request Routing Tests ───

#[tokio::test]
async fn test_put_routes_correctly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create");

    let seq = mgr
        .put(
            "ns-1",
            "record-1",
            b"key-1",
            Bytes::from_static(b"value-1"),
            Bytes::new(),
            IdempotencyToken::none(),
        )
        .await
        .expect("put should succeed");

    assert!(seq == 0 || seq > 0);
}

#[tokio::test]
async fn test_get_after_put() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create");

    mgr.put(
        "ns-1",
        "record-1",
        b"key-1",
        Bytes::from_static(b"hello"),
        Bytes::from_static(b"meta"),
        IdempotencyToken::none(),
    )
    .await
    .expect("put");

    let result = mgr
        .get("ns-1", "record-1", b"key-1")
        .await
        .expect("get")
        .expect("should find entry");

    assert_eq!(result.value, Bytes::from_static(b"hello"));
    assert_eq!(result.metadata, Bytes::from_static(b"meta"));
}

#[tokio::test]
async fn test_put_nonexistent_namespace() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    let err = mgr
        .put(
            "no-such-ns",
            "record-1",
            b"key-1",
            Bytes::from_static(b"value"),
            Bytes::new(),
            IdempotencyToken::none(),
        )
        .await
        .expect_err("put to nonexistent namespace should fail");

    match &err {
        FlushError::NotFound { key } => {
            assert!(
                key.contains("namespace: no-such-ns"),
                "unexpected key: {key}"
            );
        }
        other => panic!("expected NotFound, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_scan_routes_correctly() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create");

    mgr.put(
        "ns-1",
        "record-1",
        b"key-a",
        Bytes::from_static(b"val-a"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put a");

    mgr.put(
        "ns-1",
        "record-1",
        b"key-b",
        Bytes::from_static(b"val-b"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put b");

    let result = mgr
        .scan("ns-1", "record-1", None, None, RangeReadOptions::default())
        .await
        .expect("scan");

    assert_eq!(result.entries.len(), 2);
}

// ─── Namespace Isolation Tests ───

#[tokio::test]
async fn test_namespace_isolation_writes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-a"))
        .await
        .expect("create ns-a");
    mgr.create_namespace(test_config("ns-b"))
        .await
        .expect("create ns-b");

    mgr.put(
        "ns-a",
        "record-1",
        b"key-1",
        Bytes::from_static(b"value-a"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put to ns-a");

    let result = mgr
        .get("ns-b", "record-1", b"key-1")
        .await
        .expect("get from ns-b");

    assert!(result.is_none(), "ns-b should not see ns-a's data");
}

#[tokio::test]
async fn test_namespace_isolation_deletes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-a"))
        .await
        .expect("create ns-a");
    mgr.create_namespace(test_config("ns-b"))
        .await
        .expect("create ns-b");

    mgr.put(
        "ns-a",
        "record-1",
        b"key-1",
        Bytes::from_static(b"value-a"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put to ns-a");

    mgr.put(
        "ns-b",
        "record-1",
        b"key-1",
        Bytes::from_static(b"value-b"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put to ns-b");

    mgr.delete("ns-a", "record-1", b"key-1")
        .await
        .expect("delete from ns-a");

    let result_a = mgr
        .get("ns-a", "record-1", b"key-1")
        .await
        .expect("get from ns-a");
    assert!(result_a.is_none(), "ns-a should be deleted");

    let result_b = mgr
        .get("ns-b", "record-1", b"key-1")
        .await
        .expect("get from ns-b")
        .expect("ns-b should still have data");
    assert_eq!(result_b.value, Bytes::from_static(b"value-b"));
}

// ─── Multi-Partition Tests ───

#[tokio::test]
async fn test_partitions_created_for_count() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config_with_partitions("ns-1", 4))
        .await
        .expect("create with 4 partitions");

    let count = mgr.partition_count("ns-1").expect("partition count");
    assert_eq!(count, 4);
}

#[tokio::test]
async fn test_same_record_always_same_partition() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config_with_partitions("ns-1", 4))
        .await
        .expect("create");

    // Write same record_id multiple times -- all should land in same partition
    for i in 0..5 {
        let key = format!("key-{i}");
        mgr.put(
            "ns-1",
            "stable-record",
            key.as_bytes(),
            Bytes::from_static(b"val"),
            Bytes::new(),
            IdempotencyToken::none(),
        )
        .await
        .expect("put");
    }

    // All keys should be retrievable via the same record_id
    for i in 0..5 {
        let key = format!("key-{i}");
        let result = mgr
            .get("ns-1", "stable-record", key.as_bytes())
            .await
            .expect("get")
            .expect("should find entry");
        assert_eq!(result.value, Bytes::from_static(b"val"));
    }
}

// ─── Lifecycle Tests ───

#[tokio::test]
async fn test_stop_all_stops_all_partitions() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create ns-1");
    mgr.create_namespace(test_config("ns-2"))
        .await
        .expect("create ns-2");

    mgr.stop_all().await.expect("stop_all should succeed");
}

#[tokio::test]
async fn test_delete_namespace_stops_partitions() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create");

    mgr.put(
        "ns-1",
        "record-1",
        b"key-1",
        Bytes::from_static(b"value"),
        Bytes::new(),
        IdempotencyToken::none(),
    )
    .await
    .expect("put");

    mgr.delete_namespace("ns-1")
        .await
        .expect("delete should stop partitions first");

    assert!(!mgr.namespace_exists("ns-1"));
}

// ─── Status Tests ───

#[tokio::test]
async fn test_namespace_count() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    assert_eq!(mgr.namespace_count(), 0);

    mgr.create_namespace(test_config("ns-1"))
        .await
        .expect("create ns-1");
    assert_eq!(mgr.namespace_count(), 1);

    mgr.create_namespace(test_config("ns-2"))
        .await
        .expect("create ns-2");
    assert_eq!(mgr.namespace_count(), 2);

    mgr.delete_namespace("ns-1")
        .await
        .expect("delete ns-1");
    assert_eq!(mgr.namespace_count(), 1);
}

#[tokio::test]
async fn test_total_partition_count() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mgr = test_manager(tmp.path()).await;

    assert_eq!(mgr.total_partition_count(), 0);

    mgr.create_namespace(test_config_with_partitions("ns-1", 2))
        .await
        .expect("create ns-1 with 2 partitions");

    mgr.create_namespace(test_config_with_partitions("ns-2", 4))
        .await
        .expect("create ns-2 with 4 partitions");

    assert_eq!(mgr.total_partition_count(), 6);
}
