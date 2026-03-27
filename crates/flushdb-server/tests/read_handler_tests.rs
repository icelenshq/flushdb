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

async fn setup_with_data() -> (FlushDbService<LocalFsBackend>, TempDir) {
    let (service, dir) = setup_service().await;

    for (key, value) in &[
        (b"a".as_slice(), b"value-a".as_slice()),
        (b"b", b"value-b"),
        (b"c", b"value-c"),
        (b"d", b"value-d"),
        (b"e", b"value-e"),
    ] {
        let req = proto::PutItemsRequest {
            namespace: "test-ns".to_string(),
            id: "record-1".to_string(),
            items: vec![proto::Item {
                key: key.to_vec(),
                value: value.to_vec(),
                metadata: vec![],
                chunk: 0,
            }],
            idempotency_token: None,
        };
        service
            .put_items(Request::new(req))
            .await
            .expect("put should succeed");
    }

    (service, dir)
}

fn match_keys_predicate(keys: Vec<&[u8]>) -> Option<proto::Predicate> {
    Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchKeys(proto::MatchKeys {
            keys: keys.into_iter().map(|k| k.to_vec()).collect(),
        })),
    })
}

fn match_all_predicate() -> Option<proto::Predicate> {
    Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchAll(true)),
    })
}

fn match_range_predicate(start: &[u8], end: &[u8]) -> Option<proto::Predicate> {
    Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchRange(proto::MatchRange {
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            start_inclusive: true,
            end_inclusive: true,
        })),
    })
}

fn make_get_request(
    namespace: &str,
    id: &str,
    predicate: Option<proto::Predicate>,
    selection: Option<proto::Selection>,
) -> proto::GetItemsRequest {
    proto::GetItemsRequest {
        namespace: namespace.to_string(),
        id: id.to_string(),
        predicate,
        selection,
        signals: Default::default(),
    }
}

fn make_scan_request(
    namespace: &str,
    id: &str,
    predicate: Option<proto::Predicate>,
) -> proto::ScanItemsRequest {
    proto::ScanItemsRequest {
        namespace: namespace.to_string(),
        id: id.to_string(),
        predicate,
        signals: Default::default(),
    }
}

async fn collect_scan_stream(
    stream: &mut std::pin::Pin<
        Box<dyn futures::Stream<Item = Result<proto::ScanItemsResponse, tonic::Status>> + Send>,
    >,
) -> Vec<proto::Item> {
    use tokio_stream::StreamExt;

    let mut items = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(batch) => items.extend(batch.items),
            Err(e) => panic!("stream error: {}", e),
        }
    }
    items
}

// ─── GetItems — MatchKeys Tests ───

#[tokio::test]
async fn test_get_match_keys_single() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request(
        "test-ns",
        "record-1",
        match_keys_predicate(vec![b"b"]),
        None,
    );
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert_eq!(inner.items.len(), 1);
    assert_eq!(inner.items[0].key, b"b");
    assert_eq!(inner.items[0].value, b"value-b");
}

#[tokio::test]
async fn test_get_match_keys_multi() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request(
        "test-ns",
        "record-1",
        match_keys_predicate(vec![b"a", b"c", b"e"]),
        None,
    );
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert_eq!(inner.items.len(), 3);

    let keys: Vec<&[u8]> = inner.items.iter().map(|i| i.key.as_slice()).collect();
    assert!(keys.contains(&b"a".as_slice()));
    assert!(keys.contains(&b"c".as_slice()));
    assert!(keys.contains(&b"e".as_slice()));
}

#[tokio::test]
async fn test_get_match_keys_missing_key() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request(
        "test-ns",
        "record-1",
        match_keys_predicate(vec![b"a", b"nonexistent"]),
        None,
    );
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert_eq!(inner.items.len(), 1);
    assert_eq!(inner.items[0].key, b"a");
}

#[tokio::test]
async fn test_get_match_keys_all_missing() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request(
        "test-ns",
        "record-1",
        match_keys_predicate(vec![b"x", b"y", b"z"]),
        None,
    );
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert!(inner.items.is_empty());
}

#[tokio::test]
async fn test_get_match_keys_empty_rejected() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request(
        "test-ns",
        "record-1",
        Some(proto::Predicate {
            predicate: Some(proto::predicate::Predicate::MatchKeys(proto::MatchKeys {
                keys: vec![],
            })),
        }),
        None,
    );
    let err = service
        .get_items(Request::new(req))
        .await
        .expect_err("empty keys should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

// ─── GetItems — MatchAll Tests ───

#[tokio::test]
async fn test_get_match_all_returns_all() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request("test-ns", "record-1", match_all_predicate(), None);
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert_eq!(inner.items.len(), 5);

    let mut keys: Vec<Vec<u8>> = inner.items.iter().map(|i| i.key.clone()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
        ]
    );
}

#[tokio::test]
async fn test_get_match_all_empty_record() {
    let (service, _dir) = setup_service().await;

    let req = make_get_request("test-ns", "empty-record", match_all_predicate(), None);
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert!(inner.items.is_empty());
}

// ─── GetItems — exclude_values Tests ───

#[tokio::test]
async fn test_exclude_values_strips_value() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request(
        "test-ns",
        "record-1",
        match_keys_predicate(vec![b"a", b"b"]),
        Some(proto::Selection {
            exclude_values: true,
            page_size_bytes: 0,
            item_limit: 0,
            page_token: vec![],
        }),
    );
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert_eq!(inner.items.len(), 2);
    for item in &inner.items {
        assert!(item.value.is_empty(), "value should be stripped");
    }
}

#[tokio::test]
async fn test_exclude_values_preserves_keys() {
    let (service, _dir) = setup_with_data().await;

    let req = make_get_request(
        "test-ns",
        "record-1",
        match_all_predicate(),
        Some(proto::Selection {
            exclude_values: true,
            page_size_bytes: 0,
            item_limit: 0,
            page_token: vec![],
        }),
    );
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();
    assert_eq!(inner.items.len(), 5);

    let mut keys: Vec<Vec<u8>> = inner.items.iter().map(|i| i.key.clone()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
        ]
    );

    for item in &inner.items {
        assert!(
            item.value.is_empty(),
            "value should be empty with exclude_values"
        );
    }
}

// ─── GetItems — MatchRange Tests ───

#[tokio::test]
async fn test_get_match_range() {
    let (service, _dir) = setup_with_data().await;

    // Engine range scan uses exclusive end boundary, so end_key "e" returns b, c, d
    let req = make_get_request(
        "test-ns",
        "record-1",
        match_range_predicate(b"b", b"e"),
        None,
    );
    let resp = service
        .get_items(Request::new(req))
        .await
        .expect("get_items should succeed");

    let inner = resp.into_inner();

    let mut keys: Vec<Vec<u8>> = inner.items.iter().map(|i| i.key.clone()).collect();
    keys.sort();
    assert!(keys.contains(&b"b".to_vec()));
    assert!(keys.contains(&b"c".to_vec()));
    assert!(keys.contains(&b"d".to_vec()));
}

// ─── ScanItems Tests ───

#[tokio::test]
async fn test_scan_streams_all_items() {
    let (service, _dir) = setup_with_data().await;

    let req = make_scan_request(
        "test-ns",
        "record-1",
        match_keys_predicate(vec![b"a", b"c", b"e"]),
    );
    let resp = service
        .scan_items(Request::new(req))
        .await
        .expect("scan_items should succeed");

    let mut stream = resp.into_inner();
    let items = collect_scan_stream(&mut stream).await;

    assert_eq!(items.len(), 3);
    let keys: Vec<&[u8]> = items.iter().map(|i| i.key.as_slice()).collect();
    assert!(keys.contains(&b"a".as_slice()));
    assert!(keys.contains(&b"c".as_slice()));
    assert!(keys.contains(&b"e".as_slice()));
}

#[tokio::test]
async fn test_scan_match_all() {
    let (service, _dir) = setup_with_data().await;

    let req = make_scan_request("test-ns", "record-1", match_all_predicate());
    let resp = service
        .scan_items(Request::new(req))
        .await
        .expect("scan_items should succeed");

    let mut stream = resp.into_inner();
    let items = collect_scan_stream(&mut stream).await;

    assert_eq!(items.len(), 5);
    let mut keys: Vec<Vec<u8>> = items.iter().map(|i| i.key.clone()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
            b"e".to_vec(),
        ]
    );
}

#[tokio::test]
async fn test_scan_empty_record() {
    let (service, _dir) = setup_service().await;

    let req = make_scan_request("test-ns", "empty-record", match_all_predicate());
    let resp = service
        .scan_items(Request::new(req))
        .await
        .expect("scan_items should succeed");

    let mut stream = resp.into_inner();
    let items = collect_scan_stream(&mut stream).await;

    assert!(items.is_empty());
}

// ─── Error Tests ───

#[tokio::test]
async fn test_get_nonexistent_namespace() {
    let (service, _dir) = setup_service().await;

    let req = make_get_request("no-such-ns", "record-1", match_all_predicate(), None);
    let err = service
        .get_items(Request::new(req))
        .await
        .expect_err("nonexistent namespace should fail");

    assert_eq!(err.code(), tonic::Code::NotFound);
}

#[tokio::test]
async fn test_get_no_predicate() {
    let (service, _dir) = setup_service().await;

    let req = make_get_request("test-ns", "record-1", None, None);
    let err = service
        .get_items(Request::new(req))
        .await
        .expect_err("missing predicate should fail");

    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}

#[tokio::test]
async fn test_scan_nonexistent_namespace() {
    let (service, _dir) = setup_service().await;

    let req = make_scan_request("no-such-ns", "record-1", match_all_predicate());
    let result = service.scan_items(Request::new(req)).await;

    match result {
        Err(status) => assert_eq!(status.code(), tonic::Code::NotFound),
        Ok(_) => panic!("nonexistent namespace should fail"),
    }
}
