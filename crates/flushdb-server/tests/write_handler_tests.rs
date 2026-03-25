use std::sync::Arc;

use flushdb_proto::flushdb::v1 as proto;
use flushdb_proto::flushdb::v1::flush_db_server::FlushDb;
use flushdb_server::handlers::FlushDbService;
use flushdb_server::namespace_config::NamespaceConfig;
use flushdb_server::namespace_manager::NamespaceManager;
use flushdb_server::version_generator::VersionGenerator;
use flushdb_types::LocalFsBackend;
use tempfile::TempDir;
use tonic::Request;

async fn setup_service() -> (FlushDbService<LocalFsBackend>, TempDir) {
    let dir = TempDir::new().expect("tempdir");
    let backend = LocalFsBackend::new(dir.path().join("storage"));
    let version_gen = Arc::new(VersionGenerator::new(0));
    let manager = Arc::new(NamespaceManager::new(
        backend,
        dir.path().to_path_buf(),
        version_gen.clone(),
    ));

    let config = NamespaceConfig::new("test-ns".to_string(), 1).expect("valid config");
    manager
        .create_namespace(config)
        .await
        .expect("create namespace");

    let service = FlushDbService::new(manager, version_gen);
    (service, dir)
}

fn make_item(key: &[u8], value: &[u8], metadata: &[u8]) -> proto::Item {
    proto::Item {
        key: key.to_vec(),
        value: value.to_vec(),
        metadata: metadata.to_vec(),
        chunk: 0,
    }
}

fn make_put_request(namespace: &str, id: &str, items: Vec<proto::Item>) -> proto::PutItemsRequest {
    proto::PutItemsRequest {
        namespace: namespace.to_string(),
        id: id.to_string(),
        items,
        idempotency_token: None,
    }
}

// ─── PutItems Tests ───

#[tokio::test]
async fn test_put_items_single_item() {
    let (service, _dir) = setup_service().await;

    let req = make_put_request(
        "test-ns",
        "record-1",
        vec![make_item(b"key-1", b"value-1", b"")],
    );

    let resp = service
        .put_items(Request::new(req))
        .await
        .expect("put_items should succeed");

    let version = resp.into_inner().version.expect("should have version");
    assert!(version.timestamp_ms > 0);
}

#[tokio::test]
async fn test_put_items_multiple_items() {
    let (service, _dir) = setup_service().await;

    let items = vec![
        make_item(b"key-a", b"val-a", b""),
        make_item(b"key-b", b"val-b", b""),
        make_item(b"key-c", b"val-c", b""),
    ];

    let req = make_put_request("test-ns", "record-1", items);
    let resp = service
        .put_items(Request::new(req))
        .await
        .expect("put_items should succeed");

    assert!(resp.into_inner().version.is_some());

    // Verify all items were written by reading them back
    for key in [b"key-a".as_slice(), b"key-b", b"key-c"] {
        let result = service
            .namespace_manager
            .get("test-ns", "record-1", key)
            .await
            .expect("get should succeed")
            .expect("item should exist");
        assert!(!result.value.is_empty());
    }
}

#[tokio::test]
async fn test_put_items_empty_namespace_rejected() {
    let (service, _dir) = setup_service().await;

    let req = make_put_request("", "record-1", vec![make_item(b"key-1", b"val", b"")]);

    let err = service
        .put_items(Request::new(req))
        .await
        .expect_err("empty namespace should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_put_items_empty_record_id_rejected() {
    let (service, _dir) = setup_service().await;

    let req = make_put_request("test-ns", "", vec![make_item(b"key-1", b"val", b"")]);

    let err = service
        .put_items(Request::new(req))
        .await
        .expect_err("empty record_id should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_put_items_no_items_rejected() {
    let (service, _dir) = setup_service().await;

    let req = make_put_request("test-ns", "record-1", vec![]);

    let err = service
        .put_items(Request::new(req))
        .await
        .expect_err("empty items should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_put_items_version_monotonic() {
    let (service, _dir) = setup_service().await;

    let mut prev_ts = 0u64;
    let mut prev_seq = 0u32;

    for i in 0..5 {
        let key = format!("key-{i}");
        let req = make_put_request(
            "test-ns",
            "record-1",
            vec![make_item(key.as_bytes(), b"val", b"")],
        );

        let resp = service
            .put_items(Request::new(req))
            .await
            .expect("put_items should succeed");

        let version = resp.into_inner().version.expect("should have version");

        // Each version should be >= the previous (monotonically non-decreasing)
        assert!(
            (version.timestamp_ms, version.sequence) >= (prev_ts, prev_seq),
            "version should be monotonically increasing: ({}, {}) >= ({}, {})",
            version.timestamp_ms,
            version.sequence,
            prev_ts,
            prev_seq,
        );

        prev_ts = version.timestamp_ms;
        prev_seq = version.sequence;
    }
}

#[tokio::test]
async fn test_put_items_nonexistent_namespace() {
    let (service, _dir) = setup_service().await;

    let req = make_put_request(
        "no-such-ns",
        "record-1",
        vec![make_item(b"key-1", b"val", b"")],
    );

    let err = service
        .put_items(Request::new(req))
        .await
        .expect_err("nonexistent namespace should fail");

    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_put_items_with_metadata() {
    let (service, _dir) = setup_service().await;

    let req = make_put_request(
        "test-ns",
        "record-1",
        vec![make_item(b"key-1", b"the-value", b"the-metadata")],
    );

    service
        .put_items(Request::new(req))
        .await
        .expect("put_items should succeed");

    let result = service
        .namespace_manager
        .get("test-ns", "record-1", b"key-1")
        .await
        .expect("get should succeed")
        .expect("item should exist");

    assert_eq!(result.value.as_ref(), b"the-value");
    assert_eq!(result.metadata.as_ref(), b"the-metadata");
}

// ─── DeleteItems Tests ───

#[tokio::test]
async fn test_delete_match_all() {
    let (service, _dir) = setup_service().await;

    // Write some data first
    let put_req = make_put_request(
        "test-ns",
        "record-1",
        vec![
            make_item(b"key-a", b"val-a", b""),
            make_item(b"key-b", b"val-b", b""),
        ],
    );
    service
        .put_items(Request::new(put_req))
        .await
        .expect("put should succeed");

    // Delete all items
    let delete_req = proto::DeleteItemsRequest {
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        predicate: Some(proto::Predicate {
            predicate: Some(proto::predicate::Predicate::MatchAll(true)),
        }),
        idempotency_token: None,
    };

    let resp = service
        .delete_items(Request::new(delete_req))
        .await
        .expect("delete should succeed");

    assert!(resp.into_inner().version.is_some());
}

#[tokio::test]
async fn test_delete_match_keys() {
    let (service, _dir) = setup_service().await;

    // Write data
    let put_req = make_put_request(
        "test-ns",
        "record-1",
        vec![
            make_item(b"key-a", b"val-a", b""),
            make_item(b"key-b", b"val-b", b""),
            make_item(b"key-c", b"val-c", b""),
        ],
    );
    service
        .put_items(Request::new(put_req))
        .await
        .expect("put should succeed");

    // Delete specific keys
    let delete_req = proto::DeleteItemsRequest {
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        predicate: Some(proto::Predicate {
            predicate: Some(proto::predicate::Predicate::MatchKeys(proto::MatchKeys {
                keys: vec![b"key-a".to_vec(), b"key-c".to_vec()],
            })),
        }),
        idempotency_token: None,
    };

    service
        .delete_items(Request::new(delete_req))
        .await
        .expect("delete should succeed");

    // key-a and key-c should be deleted, key-b should remain
    let result_a = service
        .namespace_manager
        .get("test-ns", "record-1", b"key-a")
        .await
        .expect("get should succeed");
    assert!(result_a.is_none(), "key-a should be deleted");

    let result_b = service
        .namespace_manager
        .get("test-ns", "record-1", b"key-b")
        .await
        .expect("get should succeed")
        .expect("key-b should still exist");
    assert_eq!(result_b.value.as_ref(), b"val-b");

    let result_c = service
        .namespace_manager
        .get("test-ns", "record-1", b"key-c")
        .await
        .expect("get should succeed");
    assert!(result_c.is_none(), "key-c should be deleted");
}

#[tokio::test]
async fn test_delete_no_predicate_rejected() {
    let (service, _dir) = setup_service().await;

    let delete_req = proto::DeleteItemsRequest {
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        predicate: None,
        idempotency_token: None,
    };

    let err = service
        .delete_items(Request::new(delete_req))
        .await
        .expect_err("missing predicate should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_delete_returns_version() {
    let (service, _dir) = setup_service().await;

    // Write data first
    let put_req = make_put_request(
        "test-ns",
        "record-1",
        vec![make_item(b"key-1", b"val", b"")],
    );
    service
        .put_items(Request::new(put_req))
        .await
        .expect("put should succeed");

    let delete_req = proto::DeleteItemsRequest {
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        predicate: Some(proto::Predicate {
            predicate: Some(proto::predicate::Predicate::MatchAll(true)),
        }),
        idempotency_token: None,
    };

    let resp = service
        .delete_items(Request::new(delete_req))
        .await
        .expect("delete should succeed");

    let version = resp.into_inner().version.expect("should have version");
    assert!(version.timestamp_ms > 0);
}

#[tokio::test]
async fn test_delete_nonexistent_namespace() {
    let (service, _dir) = setup_service().await;

    let delete_req = proto::DeleteItemsRequest {
        namespace: "no-such-ns".to_string(),
        id: "record-1".to_string(),
        predicate: Some(proto::Predicate {
            predicate: Some(proto::predicate::Predicate::MatchAll(true)),
        }),
        idempotency_token: None,
    };

    let err = service
        .delete_items(Request::new(delete_req))
        .await
        .expect_err("nonexistent namespace should fail");

    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_delete_items_empty_namespace_rejected() {
    let (service, _dir) = setup_service().await;

    let delete_req = proto::DeleteItemsRequest {
        namespace: "".to_string(),
        id: "record-1".to_string(),
        predicate: Some(proto::Predicate {
            predicate: Some(proto::predicate::Predicate::MatchAll(true)),
        }),
        idempotency_token: None,
    };

    let err = service
        .delete_items(Request::new(delete_req))
        .await
        .expect_err("empty namespace should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_delete_items_empty_record_id_rejected() {
    let (service, _dir) = setup_service().await;

    let delete_req = proto::DeleteItemsRequest {
        namespace: "test-ns".to_string(),
        id: "".to_string(),
        predicate: Some(proto::Predicate {
            predicate: Some(proto::predicate::Predicate::MatchAll(true)),
        }),
        idempotency_token: None,
    };

    let err = service
        .delete_items(Request::new(delete_req))
        .await
        .expect_err("empty record_id should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ─── Idempotency Tests ───

#[tokio::test]
async fn test_put_items_bypass_dedup() {
    let (service, _dir) = setup_service().await;

    // All-zero token (IdempotencyToken::none()) should allow repeated writes
    let req1 = make_put_request(
        "test-ns",
        "record-1",
        vec![make_item(b"key-1", b"val-first", b"")],
    );

    service
        .put_items(Request::new(req1))
        .await
        .expect("first put should succeed");

    let req2 = make_put_request(
        "test-ns",
        "record-1",
        vec![make_item(b"key-1", b"val-second", b"")],
    );

    service
        .put_items(Request::new(req2))
        .await
        .expect("second put with no token should succeed (overwrite)");

    let result = service
        .namespace_manager
        .get("test-ns", "record-1", b"key-1")
        .await
        .expect("get should succeed")
        .expect("item should exist");

    assert_eq!(
        result.value.as_ref(),
        b"val-second",
        "second write should overwrite the first"
    );
}

fn make_proto_token(gen_time: u64, nonce_byte: u8) -> proto::IdempotencyToken {
    let mut token_bytes = vec![0u8; 16];
    token_bytes[0] = nonce_byte;
    proto::IdempotencyToken {
        generation_time: gen_time,
        token: token_bytes,
    }
}

#[tokio::test]
async fn test_put_items_batch_with_token_persists_all_items() {
    let (service, _dir) = setup_service().await;

    let req = proto::PutItemsRequest {
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        items: vec![
            make_item(b"key-a", b"val-a", b""),
            make_item(b"key-b", b"val-b", b""),
            make_item(b"key-c", b"val-c", b""),
        ],
        idempotency_token: Some(make_proto_token(1000, 0x42)),
    };

    service
        .put_items(Request::new(req))
        .await
        .expect("batch put should succeed");

    // ALL three items must be persisted
    for (key, expected_val) in [(b"key-a".as_slice(), b"val-a".as_slice()), (b"key-b", b"val-b"), (b"key-c", b"val-c")] {
        let result = service
            .namespace_manager
            .get("test-ns", "record-1", key)
            .await
            .expect("get should succeed")
            .expect("item should exist");
        assert_eq!(result.value.as_ref(), expected_val, "item {:?} mismatch", key);
    }
}

#[tokio::test]
async fn test_put_items_batch_retry_is_idempotent() {
    let (service, _dir) = setup_service().await;

    let make_req = || proto::PutItemsRequest {
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        items: vec![
            make_item(b"key-a", b"val-a", b""),
            make_item(b"key-b", b"val-b", b""),
        ],
        idempotency_token: Some(make_proto_token(2000, 0x99)),
    };

    // First write
    service
        .put_items(Request::new(make_req()))
        .await
        .expect("first batch put");

    // Retry with same token — should succeed (idempotent)
    service
        .put_items(Request::new(make_req()))
        .await
        .expect("retry should succeed as idempotent");

    // Items should still have original values
    for (key, expected_val) in [(b"key-a".as_slice(), b"val-a".as_slice()), (b"key-b", b"val-b")] {
        let result = service
            .namespace_manager
            .get("test-ns", "record-1", key)
            .await
            .expect("get should succeed")
            .expect("item should exist");
        assert_eq!(result.value.as_ref(), expected_val);
    }
}

#[tokio::test]
async fn test_put_items_single_item_with_token_backward_compat() {
    let (service, _dir) = setup_service().await;

    let req = proto::PutItemsRequest {
        namespace: "test-ns".to_string(),
        id: "record-1".to_string(),
        items: vec![make_item(b"key-1", b"val-1", b"")],
        idempotency_token: Some(make_proto_token(3000, 0x01)),
    };

    service
        .put_items(Request::new(req.clone()))
        .await
        .expect("first put");

    // Retry — should succeed silently
    service
        .put_items(Request::new(req))
        .await
        .expect("idempotent retry should succeed");

    let result = service
        .namespace_manager
        .get("test-ns", "record-1", b"key-1")
        .await
        .expect("get should succeed")
        .expect("item should exist");
    assert_eq!(result.value.as_ref(), b"val-1");
}
