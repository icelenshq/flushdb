use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use tempfile::TempDir;
use tokio::sync::watch;
use tonic::transport::Channel;

use flushdb_proto::flushdb::v1 as proto;
use flushdb_proto::flushdb::v1::flush_db_client::FlushDbClient;
use flushdb_proto::flushdb::v1::flush_db_server::FlushDbServer as TonicFlushDbServer;
use flushdb_server::{FlushDbService, NamespaceConfig, NamespaceManager, VersionGenerator};
use flushdb_types::LocalFsBackend;

// ===========================================================================
// Test Server Harness
// ===========================================================================

struct TestServer {
    client: FlushDbClient<Channel>,
    shutdown_tx: watch::Sender<bool>,
    server_handle: tokio::task::JoinHandle<()>,
    _dir: TempDir,
    namespace_manager: Arc<NamespaceManager<LocalFsBackend>>,
}

impl TestServer {
    async fn start() -> Self {
        let dir = TempDir::new().unwrap();
        let backend = LocalFsBackend::new(dir.path().join("storage"));
        let version_gen = Arc::new(VersionGenerator::new(1));
        let namespace_manager = Arc::new(NamespaceManager::new(
            backend,
            dir.path().to_path_buf(),
            version_gen.clone(),
        ));

        let service = FlushDbService::new(namespace_manager.clone(), version_gen);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let mut rx = shutdown_rx;
            let shutdown = async move {
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            };
            tonic::transport::Server::builder()
                .add_service(TonicFlushDbServer::new(service))
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .ok();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let client = FlushDbClient::connect(format!("http://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        Self {
            client,
            shutdown_tx,
            server_handle,
            _dir: dir,
            namespace_manager,
        }
    }

    async fn start_with_namespaces(configs: Vec<NamespaceConfig>) -> Self {
        let server = Self::start().await;
        for config in configs {
            server
                .namespace_manager
                .create_namespace(config)
                .await
                .unwrap();
        }
        server
    }

    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.server_handle.await;
    }
}

// ===========================================================================
// Request / Predicate Helpers
// ===========================================================================

fn make_put_request(
    namespace: &str,
    record_id: &str,
    items: Vec<(&str, &[u8])>,
) -> proto::PutItemsRequest {
    proto::PutItemsRequest {
        namespace: namespace.to_string(),
        id: record_id.to_string(),
        items: items
            .into_iter()
            .map(|(k, v)| proto::Item {
                key: k.as_bytes().to_vec(),
                value: v.to_vec(),
                metadata: vec![],
                chunk: 0,
            })
            .collect(),
        idempotency_token: None,
    }
}

fn make_put_request_with_metadata(
    namespace: &str,
    record_id: &str,
    items: Vec<(&[u8], &[u8], &[u8])>,
) -> proto::PutItemsRequest {
    proto::PutItemsRequest {
        namespace: namespace.to_string(),
        id: record_id.to_string(),
        items: items
            .into_iter()
            .map(|(k, v, m)| proto::Item {
                key: k.to_vec(),
                value: v.to_vec(),
                metadata: m.to_vec(),
                chunk: 0,
            })
            .collect(),
        idempotency_token: None,
    }
}

fn make_put_request_with_token(
    namespace: &str,
    record_id: &str,
    items: Vec<(&str, &[u8])>,
    token: proto::IdempotencyToken,
) -> proto::PutItemsRequest {
    proto::PutItemsRequest {
        namespace: namespace.to_string(),
        id: record_id.to_string(),
        items: items
            .into_iter()
            .map(|(k, v)| proto::Item {
                key: k.as_bytes().to_vec(),
                value: v.to_vec(),
                metadata: vec![],
                chunk: 0,
            })
            .collect(),
        idempotency_token: Some(token),
    }
}

fn make_get_request(
    namespace: &str,
    record_id: &str,
    predicate: Option<proto::Predicate>,
    selection: Option<proto::Selection>,
) -> proto::GetItemsRequest {
    proto::GetItemsRequest {
        namespace: namespace.to_string(),
        id: record_id.to_string(),
        predicate,
        selection,
        signals: Default::default(),
    }
}

fn make_delete_request(
    namespace: &str,
    record_id: &str,
    predicate: Option<proto::Predicate>,
) -> proto::DeleteItemsRequest {
    proto::DeleteItemsRequest {
        namespace: namespace.to_string(),
        id: record_id.to_string(),
        predicate,
        idempotency_token: None,
    }
}

fn match_all_predicate() -> Option<proto::Predicate> {
    Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchAll(true)),
    })
}

fn match_keys_predicate(keys: Vec<&[u8]>) -> Option<proto::Predicate> {
    Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchKeys(proto::MatchKeys {
            keys: keys.into_iter().map(|k| k.to_vec()).collect(),
        })),
    })
}

fn match_range_predicate(
    start: &[u8],
    end: &[u8],
    start_inclusive: bool,
    end_inclusive: bool,
) -> Option<proto::Predicate> {
    Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchRange(proto::MatchRange {
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            start_inclusive,
            end_inclusive,
        })),
    })
}

fn make_idempotency_token(gen_time: u64, token_bytes: [u8; 16]) -> proto::IdempotencyToken {
    proto::IdempotencyToken {
        generation_time: gen_time,
        token: token_bytes.to_vec(),
    }
}

async fn collect_scan(
    client: &mut FlushDbClient<Channel>,
    req: proto::ScanItemsRequest,
) -> Vec<proto::Item> {
    let response = client.scan_items(req).await.unwrap();
    let mut stream = response.into_inner();
    let mut items = Vec::new();
    while let Some(Ok(batch)) = tokio_stream::StreamExt::next(&mut stream).await {
        items.extend(batch.items);
    }
    items
}

fn sorted_keys(items: &[proto::Item]) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = items.iter().map(|i| i.key.clone()).collect();
    keys.sort();
    keys
}

// ===========================================================================
// Category 1: Full CRUD Cycle
// ===========================================================================

#[tokio::test]
async fn test_crud_put_get_delete_get() {
    let ns = "crud-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let put_req = make_put_request(ns, "rec-1", vec![("key-a", b"val-a")]);
    let put_resp = client.put_items(put_req).await.unwrap().into_inner();
    assert!(put_resp.version.is_some());

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"key-a"]), None);
    let get_resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(get_resp.items.len(), 1);
    assert_eq!(get_resp.items[0].key, b"key-a");
    assert_eq!(get_resp.items[0].value, b"val-a");

    let del_req = make_delete_request(ns, "rec-1", match_keys_predicate(vec![b"key-a"]));
    let del_resp = client.delete_items(del_req).await.unwrap().into_inner();
    assert!(del_resp.version.is_some());

    let get_req2 = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"key-a"]), None);
    let get_resp2 = client.get_items(get_req2).await.unwrap().into_inner();
    assert!(get_resp2.items.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_crud_put_overwrite() {
    let ns = "overwrite-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req1 = make_put_request(ns, "rec-1", vec![("key-x", b"original")]);
    client.put_items(req1).await.unwrap();

    let req2 = make_put_request(ns, "rec-1", vec![("key-x", b"updated")]);
    client.put_items(req2).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"key-x"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value, b"updated");

    server.shutdown().await;
}

#[tokio::test]
async fn test_crud_put_multiple_items() {
    let ns = "multi-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let items: Vec<(&str, &[u8])> = vec![
        ("item-a", b"val-a"),
        ("item-b", b"val-b"),
        ("item-c", b"val-c"),
        ("item-d", b"val-d"),
        ("item-e", b"val-e"),
    ];
    let req = make_put_request(ns, "rec-1", items);
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 5);

    let keys = sorted_keys(&resp.items);
    let expected: Vec<Vec<u8>> = vec![
        b"item-a".to_vec(),
        b"item-b".to_vec(),
        b"item-c".to_vec(),
        b"item-d".to_vec(),
        b"item-e".to_vec(),
    ];
    assert_eq!(keys, expected);

    server.shutdown().await;
}

#[tokio::test]
async fn test_crud_overwrite_preserves_other_keys() {
    let ns = "ow-preserve-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(
        ns,
        "rec-1",
        vec![("alpha", b"a-val"), ("beta", b"b-val"), ("gamma", b"g-val")],
    );
    client.put_items(req).await.unwrap();

    // Overwrite only beta
    let req2 = make_put_request(ns, "rec-1", vec![("beta", b"b-new")]);
    client.put_items(req2).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 3);

    let item_map: HashMap<Vec<u8>, Vec<u8>> = resp
        .items
        .iter()
        .map(|i| (i.key.clone(), i.value.clone()))
        .collect();
    assert_eq!(item_map[b"alpha".as_slice()], b"a-val");
    assert_eq!(item_map[b"beta".as_slice()], b"b-new");
    assert_eq!(item_map[b"gamma".as_slice()], b"g-val");

    server.shutdown().await;
}

#[tokio::test]
async fn test_crud_delete_then_rewrite() {
    let ns = "del-rewrite-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "rec-1", vec![("k1", b"original")]);
    client.put_items(req).await.unwrap();

    let del_req = make_delete_request(ns, "rec-1", match_keys_predicate(vec![b"k1"]));
    client.delete_items(del_req).await.unwrap();

    // Verify deleted
    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"k1"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert!(resp.items.is_empty());

    // Re-write same key
    let req2 = make_put_request(ns, "rec-1", vec![("k1", b"resurrected")]);
    client.put_items(req2).await.unwrap();

    let get_req2 = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"k1"]), None);
    let resp2 = client.get_items(get_req2).await.unwrap().into_inner();
    assert_eq!(resp2.items.len(), 1);
    assert_eq!(resp2.items[0].value, b"resurrected");

    server.shutdown().await;
}

#[tokio::test]
async fn test_crud_get_nonexistent_record() {
    let ns = "norecord-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let get_req = make_get_request(
        ns,
        "does-not-exist",
        match_keys_predicate(vec![b"key"]),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert!(resp.items.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_crud_get_nonexistent_key_within_existing_record() {
    let ns = "nokey-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "rec-1", vec![("exists", b"yes")]);
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(
        ns,
        "rec-1",
        match_keys_predicate(vec![b"does-not-exist"]),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert!(resp.items.is_empty());

    server.shutdown().await;
}

// ===========================================================================
// Category 2: Data Correctness — byte-level verification
// ===========================================================================

#[tokio::test]
async fn test_data_correctness_empty_value() {
    let ns = "empty-val-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "rec-1", vec![("key", b"")]);
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"key"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert!(resp.items[0].value.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_data_correctness_binary_values() {
    let ns = "binary-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // All byte values 0x00..0xFF
    let binary_val: Vec<u8> = (0..=255u8).collect();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items: vec![proto::Item {
            key: b"binkey".to_vec(),
            value: binary_val.clone(),
            metadata: vec![],
            chunk: 0,
        }],
        idempotency_token: None,
    };
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"binkey"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value, binary_val);

    server.shutdown().await;
}

#[tokio::test]
async fn test_data_correctness_metadata_round_trip() {
    let ns = "meta-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let metadata = b"content-type:application/json;charset=utf-8";
    let req = make_put_request_with_metadata(
        ns,
        "rec-1",
        vec![(b"k1", b"the-value", metadata.as_slice())],
    );
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"k1"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value, b"the-value");
    assert_eq!(resp.items[0].metadata, metadata.as_slice());

    server.shutdown().await;
}

#[tokio::test]
async fn test_data_correctness_large_value() {
    let ns = "largeval-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // 1 MB value with a recognizable pattern
    let large_val: Vec<u8> = (0..1_048_576u32).map(|i| (i % 251) as u8).collect();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items: vec![proto::Item {
            key: b"big".to_vec(),
            value: large_val.clone(),
            metadata: vec![],
            chunk: 0,
        }],
        idempotency_token: None,
    };
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"big"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value.len(), 1_048_576);
    assert_eq!(resp.items[0].value, large_val);

    server.shutdown().await;
}

#[tokio::test]
async fn test_data_correctness_many_items_per_record() {
    let ns = "manyitems-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let count = 200;
    // Write in batches of 50 items
    for batch in 0..4 {
        let items: Vec<proto::Item> = (0..50)
            .map(|i| {
                let idx = batch * 50 + i;
                proto::Item {
                    key: format!("k-{idx:04}").into_bytes(),
                    value: format!("v-{idx:04}").into_bytes(),
                    metadata: vec![],
                    chunk: 0,
                }
            })
            .collect();

        let req = proto::PutItemsRequest {
            namespace: ns.to_string(),
            id: "rec-1".to_string(),
            items,
            idempotency_token: None,
        };
        client.put_items(req).await.unwrap();
    }

    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), count);

    let keys = sorted_keys(&resp.items);
    let expected_keys: Vec<Vec<u8>> = (0..count)
        .map(|i| format!("k-{i:04}").into_bytes())
        .collect();
    assert_eq!(keys, expected_keys);

    server.shutdown().await;
}

#[tokio::test]
async fn test_data_correctness_multi_get_partial_results() {
    let ns = "partial-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "rec-1", vec![("exists-1", b"v1"), ("exists-2", b"v2")]);
    client.put_items(req).await.unwrap();

    // Ask for 3 keys, only 2 exist
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_keys_predicate(vec![b"exists-1", b"missing", b"exists-2"]),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 2);

    let keys = sorted_keys(&resp.items);
    assert_eq!(keys, vec![b"exists-1".to_vec(), b"exists-2".to_vec()]);

    server.shutdown().await;
}

// ===========================================================================
// Category 3: Delete Variants
// ===========================================================================

#[tokio::test]
async fn test_delete_match_all_removes_everything() {
    let ns = "delall-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(
        ns,
        "rec-1",
        vec![("a", b"1"), ("b", b"2"), ("c", b"3"), ("d", b"4")],
    );
    client.put_items(req).await.unwrap();

    let del_req = make_delete_request(ns, "rec-1", match_all_predicate());
    client.delete_items(del_req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert!(resp.items.is_empty(), "all items should be deleted");

    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_match_keys_selective() {
    let ns = "delkeys-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(
        ns,
        "rec-1",
        vec![
            ("alpha", b"1"),
            ("beta", b"2"),
            ("gamma", b"3"),
            ("delta", b"4"),
        ],
    );
    client.put_items(req).await.unwrap();

    // Delete only alpha and gamma
    let del_req =
        make_delete_request(ns, "rec-1", match_keys_predicate(vec![b"alpha", b"gamma"]));
    client.delete_items(del_req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 2);

    let keys = sorted_keys(&resp.items);
    assert_eq!(keys, vec![b"beta".to_vec(), b"delta".to_vec()]);

    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_match_range() {
    let ns = "delrange-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(
        ns,
        "rec-1",
        vec![
            ("k-000", b"v0"),
            ("k-001", b"v1"),
            ("k-002", b"v2"),
            ("k-003", b"v3"),
            ("k-004", b"v4"),
        ],
    );
    client.put_items(req).await.unwrap();

    // Delete range [k-001, k-003)
    let del_req = make_delete_request(
        ns,
        "rec-1",
        match_range_predicate(b"k-001", b"k-003", true, false),
    );
    client.delete_items(del_req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();

    let keys = sorted_keys(&resp.items);
    assert_eq!(
        keys,
        vec![b"k-000".to_vec(), b"k-003".to_vec(), b"k-004".to_vec()]
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_idempotent_no_error_on_missing() {
    let ns = "del-idemp-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Deleting from a record that doesn't exist should succeed (no-op)
    let del_req = make_delete_request(
        ns,
        "nonexistent-record",
        match_keys_predicate(vec![b"missing"]),
    );
    let resp = client.delete_items(del_req).await.unwrap().into_inner();
    assert!(resp.version.is_some());

    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_all_then_match_all_returns_empty() {
    let ns = "del-all-scan-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..10 {
        let key = format!("k-{i:03}");
        let val = format!("v-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, val.as_bytes())]);
        client.put_items(req).await.unwrap();
    }

    let del_req = make_delete_request(ns, "rec-1", match_all_predicate());
    client.delete_items(del_req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert!(resp.items.is_empty());

    // Also verify via scan streaming
    let scan_req = proto::ScanItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        predicate: match_all_predicate(),
        signals: Default::default(),
    };
    let items = collect_scan(&mut client, scan_req).await;
    assert!(items.is_empty());

    server.shutdown().await;
}

// ===========================================================================
// Category 4: Range Queries (MatchRange)
// ===========================================================================

#[tokio::test]
async fn test_range_query_inclusive_both() {
    let ns = "range-incl-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..10 {
        let key = format!("k-{i:03}");
        let val = format!("v-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, val.as_bytes())]);
        client.put_items(req).await.unwrap();
    }

    // [k-002, k-005] inclusive both
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_range_predicate(b"k-002", b"k-005", true, true),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();

    let keys = sorted_keys(&resp.items);
    assert_eq!(
        keys,
        vec![
            b"k-002".to_vec(),
            b"k-003".to_vec(),
            b"k-004".to_vec(),
            b"k-005".to_vec(),
        ]
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_range_query_exclusive_start() {
    let ns = "range-exstart-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..10 {
        let key = format!("k-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, b"v")]);
        client.put_items(req).await.unwrap();
    }

    // (k-002, k-005] — start exclusive, end inclusive
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_range_predicate(b"k-002", b"k-005", false, true),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();

    let keys = sorted_keys(&resp.items);
    assert_eq!(
        keys,
        vec![
            b"k-003".to_vec(),
            b"k-004".to_vec(),
            b"k-005".to_vec(),
        ]
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_range_query_exclusive_end() {
    let ns = "range-exend-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..10 {
        let key = format!("k-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, b"v")]);
        client.put_items(req).await.unwrap();
    }

    // [k-002, k-005) — start inclusive, end exclusive
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_range_predicate(b"k-002", b"k-005", true, false),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();

    let keys = sorted_keys(&resp.items);
    assert_eq!(
        keys,
        vec![b"k-002".to_vec(), b"k-003".to_vec(), b"k-004".to_vec(),]
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_range_query_exclusive_both() {
    let ns = "range-exboth-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..10 {
        let key = format!("k-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, b"v")]);
        client.put_items(req).await.unwrap();
    }

    // (k-002, k-005) — both exclusive
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_range_predicate(b"k-002", b"k-005", false, false),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();

    let keys = sorted_keys(&resp.items);
    assert_eq!(
        keys,
        vec![b"k-003".to_vec(), b"k-004".to_vec(),]
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_range_query_empty_result() {
    let ns = "range-empty-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "rec-1", vec![("aaa", b"v"), ("zzz", b"v")]);
    client.put_items(req).await.unwrap();

    // Range that falls between existing keys
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_range_predicate(b"mmm", b"nnn", true, true),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert!(resp.items.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_range_query_open_start() {
    let ns = "range-open-start-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..5 {
        let key = format!("k-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, b"v")]);
        client.put_items(req).await.unwrap();
    }

    // Open start (empty start_key), end at k-002 inclusive
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_range_predicate(b"", b"k-002", true, true),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();

    let keys = sorted_keys(&resp.items);
    assert_eq!(
        keys,
        vec![b"k-000".to_vec(), b"k-001".to_vec(), b"k-002".to_vec(),]
    );

    server.shutdown().await;
}

// ===========================================================================
// Category 5: Namespace Isolation
// ===========================================================================

#[tokio::test]
async fn test_namespace_isolation_writes() {
    let ns_a = "iso-a";
    let ns_b = "iso-b";
    let config_a = NamespaceConfig::new(ns_a.to_string(), 1).unwrap();
    let config_b = NamespaceConfig::new(ns_b.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config_a, config_b]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns_a, "rec-1", vec![("key-1", b"a-value")]);
    client.put_items(req).await.unwrap();

    let get_a = make_get_request(ns_a, "rec-1", match_keys_predicate(vec![b"key-1"]), None);
    let resp_a = client.get_items(get_a).await.unwrap().into_inner();
    assert_eq!(resp_a.items.len(), 1);
    assert_eq!(resp_a.items[0].value, b"a-value");

    let get_b = make_get_request(ns_b, "rec-1", match_keys_predicate(vec![b"key-1"]), None);
    let resp_b = client.get_items(get_b).await.unwrap().into_inner();
    assert!(resp_b.items.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_namespace_isolation_deletes_do_not_cross() {
    let ns_a = "iso-del-a";
    let ns_b = "iso-del-b";
    let config_a = NamespaceConfig::new(ns_a.to_string(), 1).unwrap();
    let config_b = NamespaceConfig::new(ns_b.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config_a, config_b]).await;
    let mut client = server.client.clone();

    // Write same record+key to both namespaces
    let req_a = make_put_request(ns_a, "rec-1", vec![("shared-key", b"a-data")]);
    let req_b = make_put_request(ns_b, "rec-1", vec![("shared-key", b"b-data")]);
    client.put_items(req_a).await.unwrap();
    client.put_items(req_b).await.unwrap();

    // Delete from ns_a only
    let del = make_delete_request(ns_a, "rec-1", match_all_predicate());
    client.delete_items(del).await.unwrap();

    // ns_a should be empty
    let get_a = make_get_request(
        ns_a,
        "rec-1",
        match_keys_predicate(vec![b"shared-key"]),
        None,
    );
    let resp_a = client.get_items(get_a).await.unwrap().into_inner();
    assert!(resp_a.items.is_empty());

    // ns_b should still have data
    let get_b = make_get_request(
        ns_b,
        "rec-1",
        match_keys_predicate(vec![b"shared-key"]),
        None,
    );
    let resp_b = client.get_items(get_b).await.unwrap().into_inner();
    assert_eq!(resp_b.items.len(), 1);
    assert_eq!(resp_b.items[0].value, b"b-data");

    server.shutdown().await;
}

#[tokio::test]
async fn test_namespace_isolation_many_namespaces() {
    let ns_count = 8;
    let configs: Vec<NamespaceConfig> = (0..ns_count)
        .map(|i| NamespaceConfig::new(format!("ns-{i}"), 1).unwrap())
        .collect();
    let server = TestServer::start_with_namespaces(configs).await;
    let mut client = server.client.clone();

    // Write unique data to each namespace
    for i in 0..ns_count {
        let ns = format!("ns-{i}");
        let val = format!("value-from-ns-{i}");
        let req = make_put_request(&ns, "rec-1", vec![("k", val.as_bytes())]);
        client.put_items(req).await.unwrap();
    }

    // Verify each namespace has its own data
    for i in 0..ns_count {
        let ns = format!("ns-{i}");
        let expected_val = format!("value-from-ns-{i}");
        let get_req = make_get_request(&ns, "rec-1", match_keys_predicate(vec![b"k"]), None);
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 1);
        assert_eq!(
            resp.items[0].value,
            expected_val.as_bytes(),
            "ns-{i} should have its own value"
        );
    }

    server.shutdown().await;
}

// ===========================================================================
// Category 6: Partition Routing
// ===========================================================================

#[tokio::test]
async fn test_partition_simple_routing() {
    let ns = "routing-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..10 {
        let record_id = format!("rec-{i}");
        let key = format!("key-{i}");
        let value = format!("value-{i}");
        let req = make_put_request(ns, &record_id, vec![(&key, value.as_bytes())]);
        client.put_items(req).await.unwrap();
    }

    for i in 0..10 {
        let record_id = format!("rec-{i}");
        let key = format!("key-{i}");
        let expected_value = format!("value-{i}");
        let get_req = make_get_request(
            ns,
            &record_id,
            match_keys_predicate(vec![key.as_bytes()]),
            None,
        );
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 1, "record {record_id} should be found");
        assert_eq!(resp.items[0].value, expected_value.as_bytes());
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_partition_routing_many_records_across_partitions() {
    let ns = "multpart-ns";
    let partition_count = 16;
    let config = NamespaceConfig::new(ns.to_string(), partition_count).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let record_count = 100;
    for i in 0..record_count {
        let record_id = format!("product-{i:04}");
        let req = make_put_request(
            ns,
            &record_id,
            vec![
                ("name", format!("Product {i}").as_bytes()),
                ("price", format!("{}.99", i * 10).as_bytes()),
            ],
        );
        client.put_items(req).await.unwrap();
    }

    // Verify every record can be read back correctly
    for i in 0..record_count {
        let record_id = format!("product-{i:04}");
        let get_req = make_get_request(&ns, &record_id, match_all_predicate(), None);
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(
            resp.items.len(),
            2,
            "record {record_id} should have 2 items"
        );

        let item_map: HashMap<Vec<u8>, Vec<u8>> = resp
            .items
            .iter()
            .map(|it| (it.key.clone(), it.value.clone()))
            .collect();
        assert_eq!(
            item_map[b"name".as_slice()],
            format!("Product {i}").into_bytes()
        );
        assert_eq!(
            item_map[b"price".as_slice()],
            format!("{}.99", i * 10).into_bytes()
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_partition_routing_record_isolation() {
    let ns = "rec-iso-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Write same key name to different records
    let req1 = make_put_request(ns, "rec-A", vec![("shared-key", b"A-data")]);
    let req2 = make_put_request(ns, "rec-B", vec![("shared-key", b"B-data")]);
    client.put_items(req1).await.unwrap();
    client.put_items(req2).await.unwrap();

    let get_a = make_get_request(
        ns,
        "rec-A",
        match_keys_predicate(vec![b"shared-key"]),
        None,
    );
    let resp_a = client.get_items(get_a).await.unwrap().into_inner();
    assert_eq!(resp_a.items[0].value, b"A-data");

    let get_b = make_get_request(
        ns,
        "rec-B",
        match_keys_predicate(vec![b"shared-key"]),
        None,
    );
    let resp_b = client.get_items(get_b).await.unwrap().into_inner();
    assert_eq!(resp_b.items[0].value, b"B-data");

    server.shutdown().await;
}

// ===========================================================================
// Category 7: Idempotency
// ===========================================================================

#[tokio::test]
async fn test_bypass_token_allows_multiple_writes() {
    let ns = "idemp-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let bypass_token = proto::IdempotencyToken {
        generation_time: 0,
        token: vec![],
    };

    for i in 0..3 {
        let req = proto::PutItemsRequest {
            namespace: ns.to_string(),
            id: "rec-1".to_string(),
            items: vec![proto::Item {
                key: b"key-1".to_vec(),
                value: format!("write-{i}").into_bytes(),
                metadata: vec![],
                chunk: 0,
            }],
            idempotency_token: Some(bypass_token.clone()),
        };
        let resp = client.put_items(req).await.unwrap().into_inner();
        assert!(resp.version.is_some());
    }

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"key-1"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value, b"write-2");

    server.shutdown().await;
}

#[tokio::test]
async fn test_idempotency_token_dedup() {
    let ns = "dedup-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let token = make_idempotency_token(12345, [1u8; 16]);

    // First write with token
    let req1 = make_put_request_with_token(ns, "rec-1", vec![("k1", b"first-write")], token.clone());
    let resp1 = client.put_items(req1).await.unwrap().into_inner();
    assert!(resp1.version.is_some());

    // Second write with SAME token — should be deduped (idempotent)
    let req2 = make_put_request_with_token(ns, "rec-1", vec![("k1", b"second-write")], token);
    let resp2 = client.put_items(req2).await.unwrap().into_inner();
    assert!(resp2.version.is_some());

    // Value should still be from the first write (dedup means second was no-op)
    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"k1"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value, b"first-write");

    server.shutdown().await;
}

#[tokio::test]
async fn test_idempotency_different_tokens_both_write() {
    let ns = "diff-tok-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let token1 = make_idempotency_token(100, [1u8; 16]);
    let token2 = make_idempotency_token(200, [2u8; 16]);

    let req1 = make_put_request_with_token(ns, "rec-1", vec![("k1", b"from-token1")], token1);
    client.put_items(req1).await.unwrap();

    let req2 = make_put_request_with_token(ns, "rec-1", vec![("k1", b"from-token2")], token2);
    client.put_items(req2).await.unwrap();

    // Second token is different, so second write should take effect
    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"k1"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items[0].value, b"from-token2");

    server.shutdown().await;
}

#[tokio::test]
async fn test_idempotency_invalid_token_length_rejected() {
    let ns = "badtok-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let bad_token = proto::IdempotencyToken {
        generation_time: 999,
        token: vec![1, 2, 3], // not 16 bytes
    };

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items: vec![proto::Item {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            metadata: vec![],
            chunk: 0,
        }],
        idempotency_token: Some(bad_token),
    };

    let err = client.put_items(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

// ===========================================================================
// Category 8: Pagination
// ===========================================================================

#[tokio::test]
async fn test_pagination_full_traversal() {
    let ns = "page-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let items: Vec<(&str, &[u8])> = (0..20)
        .map(|i| {
            let key: &str = Box::leak(format!("k-{i:03}").into_boxed_str());
            let val: &[u8] = Box::leak(format!("v-{i:03}").into_boxed_str()).as_bytes();
            (key, val)
        })
        .collect();
    let req = make_put_request(ns, "rec-1", items);
    client.put_items(req).await.unwrap();

    let mut all_items: Vec<proto::Item> = Vec::new();
    let mut page_token: Vec<u8> = vec![];

    loop {
        let selection = Some(proto::Selection {
            page_size_bytes: 0,
            item_limit: 5,
            exclude_values: false,
            page_token: page_token.clone(),
        });
        let get_req = make_get_request(ns, "rec-1", match_all_predicate(), selection);
        let resp = client.get_items(get_req).await.unwrap().into_inner();

        all_items.extend(resp.items);

        if resp.next_page_token.is_empty() {
            break;
        }
        page_token = resp.next_page_token;
    }

    assert_eq!(all_items.len(), 20);

    let collected_keys = sorted_keys(&all_items);
    let expected_keys: Vec<Vec<u8>> = (0..20)
        .map(|i| format!("k-{i:03}").into_bytes())
        .collect();
    assert_eq!(collected_keys, expected_keys);

    server.shutdown().await;
}

#[tokio::test]
async fn test_pagination_item_limit_one() {
    let ns = "page1-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(
        ns,
        "rec-1",
        vec![("aaa", b"v1"), ("bbb", b"v2"), ("ccc", b"v3")],
    );
    client.put_items(req).await.unwrap();

    let mut all_items: Vec<proto::Item> = Vec::new();
    let mut page_token: Vec<u8> = vec![];
    let mut pages = 0;

    loop {
        let selection = Some(proto::Selection {
            page_size_bytes: 0,
            item_limit: 1,
            exclude_values: false,
            page_token: page_token.clone(),
        });
        let get_req = make_get_request(ns, "rec-1", match_all_predicate(), selection);
        let resp = client.get_items(get_req).await.unwrap().into_inner();

        assert!(
            resp.items.len() <= 1,
            "page should have at most 1 item, got {}",
            resp.items.len()
        );
        all_items.extend(resp.items);
        pages += 1;

        if resp.next_page_token.is_empty() {
            break;
        }
        page_token = resp.next_page_token;
    }

    assert_eq!(all_items.len(), 3);
    assert!(pages >= 3, "should have at least 3 pages for 3 items");

    server.shutdown().await;
}

#[tokio::test]
async fn test_pagination_no_duplicates() {
    let ns = "nodup-page-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let item_count = 50;
    let items: Vec<proto::Item> = (0..item_count)
        .map(|i| proto::Item {
            key: format!("k-{i:04}").into_bytes(),
            value: format!("v-{i:04}").into_bytes(),
            metadata: vec![],
            chunk: 0,
        })
        .collect();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items,
        idempotency_token: None,
    };
    client.put_items(req).await.unwrap();

    let mut all_keys: Vec<Vec<u8>> = Vec::new();
    let mut page_token: Vec<u8> = vec![];

    loop {
        let selection = Some(proto::Selection {
            page_size_bytes: 0,
            item_limit: 7, // odd number to test boundary
            exclude_values: false,
            page_token: page_token.clone(),
        });
        let get_req = make_get_request(ns, "rec-1", match_all_predicate(), selection);
        let resp = client.get_items(get_req).await.unwrap().into_inner();

        for item in &resp.items {
            assert!(
                !all_keys.contains(&item.key),
                "duplicate key found: {:?}",
                String::from_utf8_lossy(&item.key)
            );
            all_keys.push(item.key.clone());
        }

        if resp.next_page_token.is_empty() {
            break;
        }
        page_token = resp.next_page_token;
    }

    assert_eq!(all_keys.len(), item_count);

    server.shutdown().await;
}

#[tokio::test]
async fn test_pagination_values_match_across_pages() {
    let ns = "pageval-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let item_count = 30;
    let items: Vec<proto::Item> = (0..item_count)
        .map(|i| proto::Item {
            key: format!("k-{i:04}").into_bytes(),
            value: format!("val-{i:04}").into_bytes(),
            metadata: vec![],
            chunk: 0,
        })
        .collect();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items,
        idempotency_token: None,
    };
    client.put_items(req).await.unwrap();

    let mut all_items: Vec<proto::Item> = Vec::new();
    let mut page_token: Vec<u8> = vec![];

    loop {
        let selection = Some(proto::Selection {
            page_size_bytes: 0,
            item_limit: 10,
            exclude_values: false,
            page_token: page_token.clone(),
        });
        let get_req = make_get_request(ns, "rec-1", match_all_predicate(), selection);
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        all_items.extend(resp.items);
        if resp.next_page_token.is_empty() {
            break;
        }
        page_token = resp.next_page_token;
    }

    // Verify each key's value is correct
    for item in &all_items {
        let key_str = String::from_utf8(item.key.clone()).unwrap();
        let expected_val = key_str.replace("k-", "val-");
        assert_eq!(
            item.value,
            expected_val.as_bytes(),
            "value mismatch for key {key_str}"
        );
    }

    server.shutdown().await;
}

// ===========================================================================
// Category 9: Selection / Projection
// ===========================================================================

#[tokio::test]
async fn test_selection_exclude_values() {
    let ns = "excl-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request_with_metadata(
        ns,
        "rec-1",
        vec![
            (b"k1", b"big-value-1", b"meta-1"),
            (b"k2", b"big-value-2", b"meta-2"),
        ],
    );
    client.put_items(req).await.unwrap();

    let selection = Some(proto::Selection {
        page_size_bytes: 0,
        item_limit: 0,
        exclude_values: true,
        page_token: vec![],
    });
    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), selection);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 2);

    for item in &resp.items {
        assert!(
            item.value.is_empty(),
            "value should be excluded but got {} bytes",
            item.value.len()
        );
        assert!(
            !item.metadata.is_empty(),
            "metadata should still be present"
        );
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_selection_exclude_values_with_match_keys() {
    let ns = "excl-mk-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request_with_metadata(
        ns,
        "rec-1",
        vec![(b"key-a", b"value-a", b"metadata-a")],
    );
    client.put_items(req).await.unwrap();

    let selection = Some(proto::Selection {
        page_size_bytes: 0,
        item_limit: 0,
        exclude_values: true,
        page_token: vec![],
    });
    let get_req = make_get_request(
        ns,
        "rec-1",
        match_keys_predicate(vec![b"key-a"]),
        selection,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert!(resp.items[0].value.is_empty());
    assert_eq!(resp.items[0].metadata, b"metadata-a");

    server.shutdown().await;
}

// ===========================================================================
// Category 10: ScanItems Streaming
// ===========================================================================

#[tokio::test]
async fn test_scan_streams_all_items() {
    let ns = "scan-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let items: Vec<(&str, &[u8])> = vec![("scan-a", b"v1"), ("scan-b", b"v2"), ("scan-c", b"v3")];
    let req = make_put_request(ns, "rec-1", items);
    client.put_items(req).await.unwrap();

    let scan_req = proto::ScanItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        predicate: match_all_predicate(),
        signals: Default::default(),
    };

    let items = collect_scan(&mut client, scan_req).await;
    assert_eq!(items.len(), 3);

    let keys = sorted_keys(&items);
    assert_eq!(
        keys,
        vec![b"scan-a".to_vec(), b"scan-b".to_vec(), b"scan-c".to_vec()]
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_scan_with_match_keys() {
    let ns = "scan-mk-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(
        ns,
        "rec-1",
        vec![("aa", b"1"), ("bb", b"2"), ("cc", b"3"), ("dd", b"4")],
    );
    client.put_items(req).await.unwrap();

    let scan_req = proto::ScanItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        predicate: match_keys_predicate(vec![b"bb", b"dd"]),
        signals: Default::default(),
    };

    let items = collect_scan(&mut client, scan_req).await;
    assert_eq!(items.len(), 2);

    let keys = sorted_keys(&items);
    assert_eq!(keys, vec![b"bb".to_vec(), b"dd".to_vec()]);

    server.shutdown().await;
}

#[tokio::test]
async fn test_scan_with_range_predicate() {
    let ns = "scan-range-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for i in 0..10 {
        let key = format!("k-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, b"v")]);
        client.put_items(req).await.unwrap();
    }

    let scan_req = proto::ScanItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        predicate: match_range_predicate(b"k-003", b"k-007", true, false),
        signals: Default::default(),
    };

    let items = collect_scan(&mut client, scan_req).await;

    let keys = sorted_keys(&items);
    assert_eq!(
        keys,
        vec![
            b"k-003".to_vec(),
            b"k-004".to_vec(),
            b"k-005".to_vec(),
            b"k-006".to_vec(),
        ]
    );

    server.shutdown().await;
}

#[tokio::test]
async fn test_scan_empty_record() {
    let ns = "scan-empty-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let scan_req = proto::ScanItemsRequest {
        namespace: ns.to_string(),
        id: "nonexistent-record".to_string(),
        predicate: match_all_predicate(),
        signals: Default::default(),
    };

    let items = collect_scan(&mut client, scan_req).await;
    assert!(items.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_scan_large_dataset() {
    let ns = "scan-large-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let count = 100;
    let items: Vec<proto::Item> = (0..count)
        .map(|i| proto::Item {
            key: format!("k-{i:04}").into_bytes(),
            value: vec![0xAB; 1024], // 1KB each
            metadata: vec![],
            chunk: 0,
        })
        .collect();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items,
        idempotency_token: None,
    };
    client.put_items(req).await.unwrap();

    let scan_req = proto::ScanItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        predicate: match_all_predicate(),
        signals: Default::default(),
    };

    let items = collect_scan(&mut client, scan_req).await;
    assert_eq!(items.len(), count);

    for item in &items {
        assert_eq!(item.value.len(), 1024);
        assert!(item.value.iter().all(|&b| b == 0xAB));
    }

    server.shutdown().await;
}

// ===========================================================================
// Category 11: OrderedKey Versions
// ===========================================================================

#[tokio::test]
async fn test_versions_monotonically_increasing() {
    let ns = "version-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let mut prev_ts = 0u64;
    let mut prev_seq = 0u32;

    for i in 0..5 {
        let req = make_put_request(ns, "rec-1", vec![(&format!("k{i}"), b"v")]);
        let resp = client.put_items(req).await.unwrap().into_inner();
        let version = resp.version.unwrap();

        let is_greater = version.timestamp_ms > prev_ts
            || (version.timestamp_ms == prev_ts && version.sequence > prev_seq);
        assert!(
            is_greater,
            "version ({}, {}) should be greater than ({}, {})",
            version.timestamp_ms, version.sequence, prev_ts, prev_seq,
        );

        prev_ts = version.timestamp_ms;
        prev_seq = version.sequence;
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_versions_unique() {
    let ns = "vuniq-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let mut seen: Vec<(u64, u32, u32)> = Vec::new();

    for i in 0..10 {
        let req = make_put_request(ns, "rec-1", vec![(&format!("k{i}"), b"v")]);
        let resp = client.put_items(req).await.unwrap().into_inner();
        let v = resp.version.unwrap();
        let tuple = (v.timestamp_ms, v.node_id, v.sequence);
        assert!(!seen.contains(&tuple), "duplicate version: {tuple:?}");
        seen.push(tuple);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_version_node_id_matches_server_config() {
    let ns = "vnid-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "rec-1", vec![("k", b"v")]);
    let resp = client.put_items(req).await.unwrap().into_inner();
    let version = resp.version.unwrap();

    // TestServer uses node_id=1
    assert_eq!(version.node_id, 1);

    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_returns_version() {
    let ns = "delver-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let put_req = make_put_request(ns, "rec-1", vec![("k", b"v")]);
    let put_resp = client.put_items(put_req).await.unwrap().into_inner();
    let put_version = put_resp.version.unwrap();

    let del_req = make_delete_request(ns, "rec-1", match_all_predicate());
    let del_resp = client.delete_items(del_req).await.unwrap().into_inner();
    let del_version = del_resp.version.unwrap();

    // Delete version should be newer than put version
    assert!(
        del_version.timestamp_ms > put_version.timestamp_ms
            || (del_version.timestamp_ms == put_version.timestamp_ms
                && del_version.sequence > put_version.sequence),
        "delete version should be after put version"
    );

    server.shutdown().await;
}

// ===========================================================================
// Category 12: Error Handling
// ===========================================================================

#[tokio::test]
async fn test_nonexistent_namespace_not_found() {
    let ns = "exists-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let put_req = make_put_request("no-such-ns", "rec-1", vec![("k", b"v")]);
    let err = client.put_items(put_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    let get_req = make_get_request(
        "no-such-ns",
        "rec-1",
        match_keys_predicate(vec![b"k"]),
        None,
    );
    let err = client.get_items(get_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    let del_req = make_delete_request("no-such-ns", "rec-1", match_all_predicate());
    let err = client.delete_items(del_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    server.shutdown().await;
}

#[tokio::test]
async fn test_empty_namespace_invalid() {
    let ns = "valid-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request("", "rec-1", vec![("k", b"v")]);
    let err = client.put_items(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_empty_record_id_invalid() {
    let ns = "recid-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "", vec![("k", b"v")]);
    let err = client.put_items(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_put_no_items_invalid() {
    let ns = "noitems-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items: vec![],
        idempotency_token: None,
    };
    let err = client.put_items(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_get_no_predicate_invalid() {
    let ns = "nopred-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let get_req = make_get_request(ns, "rec-1", None, None);
    let err = client.get_items(get_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_delete_no_predicate_invalid() {
    let ns = "del-nopred-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let del_req = make_delete_request(ns, "rec-1", None);
    let err = client.delete_items(del_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_match_keys_empty_keys_invalid() {
    let ns = "emptykeys-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let pred = Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchKeys(proto::MatchKeys {
            keys: vec![],
        })),
    });
    let get_req = make_get_request(ns, "rec-1", pred, None);
    let err = client.get_items(get_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_record_id_with_null_bytes_rejected() {
    let ns = "null-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec\0-1".to_string(),
        items: vec![proto::Item {
            key: b"k".to_vec(),
            value: b"v".to_vec(),
            metadata: vec![],
            chunk: 0,
        }],
        idempotency_token: None,
    };
    let err = client.put_items(req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_empty_namespace_get_rejected() {
    let ns = "valid-get-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let get_req = make_get_request("", "rec-1", match_all_predicate(), None);
    let err = client.get_items(get_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_empty_record_id_get_rejected() {
    let ns = "valid-get-recid-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let get_req = make_get_request(ns, "", match_all_predicate(), None);
    let err = client.get_items(get_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_empty_namespace_delete_rejected() {
    let ns = "valid-del-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let del_req = make_delete_request("", "rec-1", match_all_predicate());
    let err = client.delete_items(del_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

#[tokio::test]
async fn test_empty_record_id_delete_rejected() {
    let ns = "valid-del-recid-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let del_req = make_delete_request(ns, "", match_all_predicate());
    let err = client.delete_items(del_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    server.shutdown().await;
}

// ===========================================================================
// Category 13: Concurrent Access
// ===========================================================================

#[tokio::test]
async fn test_concurrent_writes_to_different_records() {
    let ns = "conc-diff-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;

    let mut handles = vec![];
    for i in 0..20 {
        let mut client = server.client.clone();
        handles.push(tokio::spawn(async move {
            let record_id = format!("rec-{i}");
            let key = format!("key-{i}");
            let value = format!("value-{i}");
            let req = make_put_request(ns, &record_id, vec![(&key, value.as_bytes())]);
            client.put_items(req).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // Verify all 20 records
    let mut client = server.client.clone();
    for i in 0..20 {
        let record_id = format!("rec-{i}");
        let key = format!("key-{i}");
        let expected_value = format!("value-{i}");
        let get_req = make_get_request(
            ns,
            &record_id,
            match_keys_predicate(vec![key.as_bytes()]),
            None,
        );
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 1, "record rec-{i} should exist");
        assert_eq!(resp.items[0].value, expected_value.as_bytes());
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_reads_and_writes() {
    let ns = "conc-rw-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;

    // Seed data
    let mut client = server.client.clone();
    for i in 0..10 {
        let record_id = format!("rec-{i}");
        let req = make_put_request(ns, &record_id, vec![("data", b"initial")]);
        client.put_items(req).await.unwrap();
    }

    // Concurrent reads and writes
    let mut handles = vec![];

    // Writers update existing records
    for i in 0..10 {
        let mut c = server.client.clone();
        handles.push(tokio::spawn(async move {
            let record_id = format!("rec-{i}");
            let req = make_put_request(ns, &record_id, vec![("data", b"updated")]);
            c.put_items(req).await.unwrap();
        }));
    }

    // Readers read records (should see either initial or updated)
    for i in 0..10 {
        let mut c = server.client.clone();
        handles.push(tokio::spawn(async move {
            let record_id = format!("rec-{i}");
            let get_req =
                make_get_request(ns, &record_id, match_keys_predicate(vec![b"data"]), None);
            let resp = c.get_items(get_req).await.unwrap().into_inner();
            assert_eq!(resp.items.len(), 1);
            assert!(
                resp.items[0].value == b"initial" || resp.items[0].value == b"updated",
                "value should be either initial or updated"
            );
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_concurrent_writes_to_same_record_different_keys() {
    let ns = "conc-same-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;

    let mut handles = vec![];
    for i in 0..10 {
        let mut c = server.client.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("key-{i}");
            let val = format!("val-{i}");
            let req = make_put_request(ns, "shared-rec", vec![(&key, val.as_bytes())]);
            c.put_items(req).await.unwrap();
        }));
    }

    for h in handles {
        h.await.unwrap();
    }

    // All 10 keys should exist
    let mut client = server.client.clone();
    let get_req = make_get_request(ns, "shared-rec", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 10);

    server.shutdown().await;
}

// ===========================================================================
// Category 14: Edge Cases
// ===========================================================================

#[tokio::test]
async fn test_edge_single_byte_key_and_value() {
    let ns = "1byte-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "r".to_string(),
        items: vec![proto::Item {
            key: vec![0x41], // 'A'
            value: vec![0x42], // 'B'
            metadata: vec![],
            chunk: 0,
        }],
        idempotency_token: None,
    };
    client.put_items(req).await.unwrap();

    let get_req = make_get_request("1byte-ns", "r", match_keys_predicate(vec![&[0x41]]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].key, vec![0x41]);
    assert_eq!(resp.items[0].value, vec![0x42]);

    server.shutdown().await;
}

#[tokio::test]
async fn test_edge_unicode_record_id() {
    let ns = "unicode-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "日本語レコード", vec![("key", b"value")]);
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(
        ns,
        "日本語レコード",
        match_keys_predicate(vec![b"key"]),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value, b"value");

    server.shutdown().await;
}

#[tokio::test]
async fn test_edge_special_chars_in_record_id() {
    let ns = "special-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let special_ids = vec![
        "rec with spaces",
        "rec-with-dashes",
        "rec.with.dots",
        "rec_with_underscores",
        "REC-UPPER-CASE",
        "123-numeric-start",
        "rec@special#chars$!",
    ];

    for id in &special_ids {
        let req = make_put_request(ns, id, vec![("k", id.as_bytes())]);
        client.put_items(req).await.unwrap();
    }

    for id in &special_ids {
        let get_req = make_get_request(ns, id, match_keys_predicate(vec![b"k"]), None);
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 1, "record '{id}' should be found");
        assert_eq!(resp.items[0].value, id.as_bytes());
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_edge_binary_keys() {
    let ns = "binkey-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Binary key (non-UTF8)
    let binary_key: Vec<u8> = vec![0x01, 0x02, 0xFF, 0xFE, 0x80];

    let req = proto::PutItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        items: vec![proto::Item {
            key: binary_key.clone(),
            value: b"binary-key-value".to_vec(),
            metadata: vec![],
            chunk: 0,
        }],
        idempotency_token: None,
    };
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(
        ns,
        "rec-1",
        match_keys_predicate(vec![&binary_key]),
        None,
    );
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].key, binary_key);
    assert_eq!(resp.items[0].value, b"binary-key-value");

    server.shutdown().await;
}

#[tokio::test]
async fn test_edge_empty_metadata() {
    let ns = "emptymeta-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request_with_metadata(ns, "rec-1", vec![(b"k", b"v", b"")]);
    client.put_items(req).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"k"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert!(resp.items[0].metadata.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_edge_overwrite_with_empty_value() {
    let ns = "ow-empty-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let req = make_put_request(ns, "rec-1", vec![("k", b"non-empty")]);
    client.put_items(req).await.unwrap();

    let req2 = make_put_request(ns, "rec-1", vec![("k", b"")]);
    client.put_items(req2).await.unwrap();

    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"k"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert!(resp.items[0].value.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_edge_many_small_records() {
    let ns = "many-small-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let count = 500;
    for i in 0..count {
        let record_id = format!("r-{i:04}");
        let req = make_put_request(ns, &record_id, vec![("k", b"v")]);
        client.put_items(req).await.unwrap();
    }

    // Verify random samples
    for i in [0, 100, 250, 499] {
        let record_id = format!("r-{i:04}");
        let get_req = make_get_request(ns, &record_id, match_keys_predicate(vec![b"k"]), None);
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 1, "record r-{i:04} should exist");
    }

    server.shutdown().await;
}

// ===========================================================================
// Category 15: Graceful Shutdown — Data Persistence
// ===========================================================================

#[tokio::test]
async fn test_graceful_shutdown_preserves_data() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let ns = "persist-ns";

    let make_ns_config = || {
        let mut cfg = NamespaceConfig::new(ns.to_string(), 1).unwrap();
        cfg.s3_path_prefix = format!("flushdb/{ns}");
        cfg
    };

    // ----- First server lifecycle: write data -----
    {
        let backend = LocalFsBackend::new(dir_path.join("storage"));
        let version_gen = Arc::new(VersionGenerator::new(1));
        let namespace_manager = Arc::new(NamespaceManager::new(
            backend,
            dir_path.clone(),
            version_gen.clone(),
        ));

        let ns_config = make_ns_config();
        namespace_manager
            .create_namespace(ns_config)
            .await
            .unwrap();

        let service = FlushDbService::new(namespace_manager.clone(), version_gen);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let mut rx = shutdown_rx;
            let shutdown = async move {
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            };
            tonic::transport::Server::builder()
                .add_service(TonicFlushDbServer::new(service))
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .ok();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut client = FlushDbClient::connect(format!("http://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        let req = make_put_request(ns, "rec-1", vec![("persist-key", b"persist-val")]);
        client.put_items(req).await.unwrap();

        namespace_manager.stop_all().await.unwrap();
        let _ = shutdown_tx.send(true);
        let _ = server_handle.await;
    }

    // ----- Second server lifecycle: read data from same directory -----
    {
        let backend = LocalFsBackend::new(dir_path.join("storage"));
        let version_gen = Arc::new(VersionGenerator::new(1));
        let namespace_manager = Arc::new(NamespaceManager::new(
            backend,
            dir_path.clone(),
            version_gen.clone(),
        ));

        let ns_config = make_ns_config();
        namespace_manager
            .create_namespace(ns_config)
            .await
            .unwrap();

        let service = FlushDbService::new(namespace_manager.clone(), version_gen);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let mut rx = shutdown_rx;
            let shutdown = async move {
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            };
            tonic::transport::Server::builder()
                .add_service(TonicFlushDbServer::new(service))
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .ok();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let mut client = FlushDbClient::connect(format!("http://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        let get_req = make_get_request(
            ns,
            "rec-1",
            match_keys_predicate(vec![b"persist-key"]),
            None,
        );
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 1, "data should survive restart");
        assert_eq!(resp.items[0].key, b"persist-key");
        assert_eq!(resp.items[0].value, b"persist-val");

        namespace_manager.stop_all().await.unwrap();
        let _ = shutdown_tx.send(true);
        let _ = server_handle.await;
    }
}

#[tokio::test]
async fn test_graceful_shutdown_preserves_multiple_records() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let ns = "persist-multi-ns";

    let make_ns_config = || {
        let mut cfg = NamespaceConfig::new(ns.to_string(), 4).unwrap();
        cfg.s3_path_prefix = format!("flushdb/{ns}");
        cfg
    };

    // First lifecycle: write many records across partitions
    {
        let backend = LocalFsBackend::new(dir_path.join("storage"));
        let version_gen = Arc::new(VersionGenerator::new(1));
        let namespace_manager = Arc::new(NamespaceManager::new(
            backend,
            dir_path.clone(),
            version_gen.clone(),
        ));
        namespace_manager
            .create_namespace(make_ns_config())
            .await
            .unwrap();

        let service = FlushDbService::new(namespace_manager.clone(), version_gen);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let mut rx = shutdown_rx;
            let shutdown = async move {
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            };
            tonic::transport::Server::builder()
                .add_service(TonicFlushDbServer::new(service))
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .ok();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut client = FlushDbClient::connect(format!("http://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        for i in 0..20 {
            let rec = format!("rec-{i:03}");
            let req = make_put_request(
                ns,
                &rec,
                vec![
                    ("name", format!("item-{i}").as_bytes()),
                    ("idx", format!("{i}").as_bytes()),
                ],
            );
            client.put_items(req).await.unwrap();
        }

        namespace_manager.stop_all().await.unwrap();
        let _ = shutdown_tx.send(true);
        let _ = server_handle.await;
    }

    // Second lifecycle: verify all records survive
    {
        let backend = LocalFsBackend::new(dir_path.join("storage"));
        let version_gen = Arc::new(VersionGenerator::new(1));
        let namespace_manager = Arc::new(NamespaceManager::new(
            backend,
            dir_path.clone(),
            version_gen.clone(),
        ));
        namespace_manager
            .create_namespace(make_ns_config())
            .await
            .unwrap();

        let service = FlushDbService::new(namespace_manager.clone(), version_gen);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let server_handle = tokio::spawn(async move {
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
            let mut rx = shutdown_rx;
            let shutdown = async move {
                while !*rx.borrow() {
                    if rx.changed().await.is_err() {
                        break;
                    }
                }
            };
            tonic::transport::Server::builder()
                .add_service(TonicFlushDbServer::new(service))
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await
                .ok();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut client = FlushDbClient::connect(format!("http://127.0.0.1:{}", addr.port()))
            .await
            .unwrap();

        for i in 0..20 {
            let rec = format!("rec-{i:03}");
            let get_req = make_get_request(ns, &rec, match_all_predicate(), None);
            let resp = client.get_items(get_req).await.unwrap().into_inner();
            assert_eq!(
                resp.items.len(),
                2,
                "record {rec} should have 2 items after restart"
            );

            let item_map: HashMap<Vec<u8>, Vec<u8>> = resp
                .items
                .iter()
                .map(|it| (it.key.clone(), it.value.clone()))
                .collect();
            assert_eq!(
                item_map[b"name".as_slice()],
                format!("item-{i}").into_bytes()
            );
            assert_eq!(item_map[b"idx".as_slice()], format!("{i}").into_bytes());
        }

        namespace_manager.stop_all().await.unwrap();
        let _ = shutdown_tx.send(true);
        let _ = server_handle.await;
    }
}

// ===========================================================================
// Category 16: Benchmarks
// ===========================================================================

#[tokio::test]
async fn bench_sequential_write_throughput() {
    let ns = "bench-write-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let count = 1000;
    let value = vec![0xABu8; 256]; // 256-byte values

    let start = Instant::now();
    for i in 0..count {
        let record_id = format!("rec-{i:04}");
        let req = proto::PutItemsRequest {
            namespace: ns.to_string(),
            id: record_id,
            items: vec![proto::Item {
                key: b"data".to_vec(),
                value: value.clone(),
                metadata: vec![],
                chunk: 0,
            }],
            idempotency_token: None,
        };
        client.put_items(req).await.unwrap();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "BENCH sequential_write: {count} puts in {:.2}ms ({:.0} ops/sec)",
        elapsed.as_millis(),
        ops_per_sec
    );
    assert!(
        ops_per_sec > 100.0,
        "sequential write throughput too low: {ops_per_sec:.0} ops/sec"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn bench_sequential_read_throughput() {
    let ns = "bench-read-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let count = 1000;
    let value = vec![0xCDu8; 256];

    // Seed data
    for i in 0..count {
        let record_id = format!("rec-{i:04}");
        let req = proto::PutItemsRequest {
            namespace: ns.to_string(),
            id: record_id,
            items: vec![proto::Item {
                key: b"data".to_vec(),
                value: value.clone(),
                metadata: vec![],
                chunk: 0,
            }],
            idempotency_token: None,
        };
        client.put_items(req).await.unwrap();
    }

    // Benchmark reads
    let start = Instant::now();
    for i in 0..count {
        let record_id = format!("rec-{i:04}");
        let get_req = make_get_request(
            ns,
            &record_id,
            match_keys_predicate(vec![b"data"]),
            None,
        );
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 1);
    }
    let elapsed = start.elapsed();

    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "BENCH sequential_read: {count} gets in {:.2}ms ({:.0} ops/sec)",
        elapsed.as_millis(),
        ops_per_sec
    );
    assert!(
        ops_per_sec > 100.0,
        "sequential read throughput too low: {ops_per_sec:.0} ops/sec"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn bench_concurrent_write_throughput() {
    let ns = "bench-conc-write-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;

    let count = 1000;
    let concurrency = 10;
    let per_task = count / concurrency;
    let value = vec![0xEFu8; 256];

    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..concurrency {
        let mut c = server.client.clone();
        let v = value.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per_task {
                let idx = t * per_task + i;
                let record_id = format!("rec-{idx:04}");
                let req = proto::PutItemsRequest {
                    namespace: ns.to_string(),
                    id: record_id,
                    items: vec![proto::Item {
                        key: b"data".to_vec(),
                        value: v.clone(),
                        metadata: vec![],
                        chunk: 0,
                    }],
                    idempotency_token: None,
                };
                c.put_items(req).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "BENCH concurrent_write ({concurrency} tasks): {count} puts in {:.2}ms ({:.0} ops/sec)",
        elapsed.as_millis(),
        ops_per_sec
    );
    assert!(
        ops_per_sec > 200.0,
        "concurrent write throughput too low: {ops_per_sec:.0} ops/sec"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn bench_concurrent_read_throughput() {
    let ns = "bench-conc-read-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let count = 1000;
    let value = vec![0xABu8; 256];

    // Seed
    for i in 0..count {
        let record_id = format!("rec-{i:04}");
        let req = proto::PutItemsRequest {
            namespace: ns.to_string(),
            id: record_id,
            items: vec![proto::Item {
                key: b"data".to_vec(),
                value: value.clone(),
                metadata: vec![],
                chunk: 0,
            }],
            idempotency_token: None,
        };
        client.put_items(req).await.unwrap();
    }

    let concurrency = 10;
    let per_task = count / concurrency;

    let start = Instant::now();
    let mut handles = vec![];
    for t in 0..concurrency {
        let mut c = server.client.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..per_task {
                let idx = t * per_task + i;
                let record_id = format!("rec-{idx:04}");
                let get_req = make_get_request(
                    ns,
                    &record_id,
                    match_keys_predicate(vec![b"data"]),
                    None,
                );
                let resp = c.get_items(get_req).await.unwrap().into_inner();
                assert_eq!(resp.items.len(), 1);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = count as f64 / elapsed.as_secs_f64();
    eprintln!(
        "BENCH concurrent_read ({concurrency} tasks): {count} gets in {:.2}ms ({:.0} ops/sec)",
        elapsed.as_millis(),
        ops_per_sec
    );
    assert!(
        ops_per_sec > 200.0,
        "concurrent read throughput too low: {ops_per_sec:.0} ops/sec"
    );

    server.shutdown().await;
}

#[tokio::test]
async fn bench_mixed_read_write_throughput() {
    let ns = "bench-mixed-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let count = 500;
    let value = vec![0xCDu8; 256];

    // Seed half
    for i in 0..count {
        let record_id = format!("rec-{i:04}");
        let req = proto::PutItemsRequest {
            namespace: ns.to_string(),
            id: record_id,
            items: vec![proto::Item {
                key: b"data".to_vec(),
                value: value.clone(),
                metadata: vec![],
                chunk: 0,
            }],
            idempotency_token: None,
        };
        client.put_items(req).await.unwrap();
    }

    // Mixed: 80% reads, 20% writes
    let total_ops = 1000;
    let start = Instant::now();
    let mut handles = vec![];

    for op in 0..total_ops {
        let mut c = server.client.clone();
        let v = value.clone();
        handles.push(tokio::spawn(async move {
            if op % 5 == 0 {
                // Write (20%)
                let record_id = format!("rec-{:04}", op % count);
                let req = proto::PutItemsRequest {
                    namespace: ns.to_string(),
                    id: record_id,
                    items: vec![proto::Item {
                        key: b"data".to_vec(),
                        value: v,
                        metadata: vec![],
                        chunk: 0,
                    }],
                    idempotency_token: None,
                };
                c.put_items(req).await.unwrap();
            } else {
                // Read (80%)
                let record_id = format!("rec-{:04}", op % count);
                let get_req = make_get_request(
                    ns,
                    &record_id,
                    match_keys_predicate(vec![b"data"]),
                    None,
                );
                c.get_items(get_req).await.unwrap();
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
    let elapsed = start.elapsed();

    let ops_per_sec = total_ops as f64 / elapsed.as_secs_f64();
    eprintln!(
        "BENCH mixed (80/20 r/w): {total_ops} ops in {:.2}ms ({:.0} ops/sec)",
        elapsed.as_millis(),
        ops_per_sec
    );
    assert!(
        ops_per_sec > 200.0,
        "mixed throughput too low: {ops_per_sec:.0} ops/sec"
    );

    server.shutdown().await;
}

// ===========================================================================
// Category 17: Complex Multi-Operation Scenarios
// ===========================================================================

#[tokio::test]
async fn test_scenario_ecommerce_product_lifecycle() {
    let ns = "ecom-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let product_id = "product-12345";

    // 1. Create product with attributes
    let req = make_put_request_with_metadata(
        ns,
        product_id,
        vec![
            (b"name", b"Wireless Headphones", b"text/plain"),
            (b"price", b"79.99", b"currency/usd"),
            (b"stock", b"150", b"counter"),
            (b"category", b"electronics", b"text/plain"),
            (b"description", b"Premium wireless headphones with ANC", b"text/plain"),
        ],
    );
    client.put_items(req).await.unwrap();

    // 2. Read all product fields
    let get_all = make_get_request(ns, product_id, match_all_predicate(), None);
    let resp = client.get_items(get_all).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 5);

    // 3. Update price (overwrite single field)
    let update = make_put_request_with_metadata(
        ns,
        product_id,
        vec![(b"price", b"69.99", b"currency/usd")],
    );
    client.put_items(update).await.unwrap();

    // 4. Verify price changed, others unchanged
    let get_price = make_get_request(
        ns,
        product_id,
        match_keys_predicate(vec![b"price"]),
        None,
    );
    let resp = client.get_items(get_price).await.unwrap().into_inner();
    assert_eq!(resp.items[0].value, b"69.99");

    let get_name = make_get_request(
        ns,
        product_id,
        match_keys_predicate(vec![b"name"]),
        None,
    );
    let resp = client.get_items(get_name).await.unwrap().into_inner();
    assert_eq!(resp.items[0].value, b"Wireless Headphones");

    // 5. Delete description field only
    let del = make_delete_request(
        ns,
        product_id,
        match_keys_predicate(vec![b"description"]),
    );
    client.delete_items(del).await.unwrap();

    // 6. Verify 4 fields remain
    let get_all2 = make_get_request(ns, product_id, match_all_predicate(), None);
    let resp = client.get_items(get_all2).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 4);
    let keys = sorted_keys(&resp.items);
    assert!(!keys.contains(&b"description".to_vec()));

    // 7. Delete entire product
    let del_all = make_delete_request(ns, product_id, match_all_predicate());
    client.delete_items(del_all).await.unwrap();

    // 8. Product should be gone
    let get_final = make_get_request(ns, product_id, match_all_predicate(), None);
    let resp = client.get_items(get_final).await.unwrap().into_inner();
    assert!(resp.items.is_empty());

    server.shutdown().await;
}

#[tokio::test]
async fn test_scenario_batch_import_and_verify() {
    let ns = "batch-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let batch_size = 50;

    // Bulk import
    for i in 0..batch_size {
        let record_id = format!("user-{i:04}");
        let req = make_put_request(
            ns,
            &record_id,
            vec![
                ("email", format!("user{i}@example.com").as_bytes()),
                ("role", if i % 3 == 0 { b"admin" } else { b"user" }),
            ],
        );
        client.put_items(req).await.unwrap();
    }

    // Verify entire batch
    for i in 0..batch_size {
        let record_id = format!("user-{i:04}");
        let get_req = make_get_request(ns, &record_id, match_all_predicate(), None);
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items.len(), 2, "user-{i:04} should have 2 fields");

        let item_map: HashMap<Vec<u8>, Vec<u8>> = resp
            .items
            .iter()
            .map(|it| (it.key.clone(), it.value.clone()))
            .collect();

        assert_eq!(
            item_map[b"email".as_slice()],
            format!("user{i}@example.com").into_bytes()
        );
        let expected_role = if i % 3 == 0 { b"admin".as_slice() } else { b"user" };
        assert_eq!(item_map[b"role".as_slice()], expected_role);
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_scenario_delete_and_reinsert_cycle() {
    let ns = "cycle-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    for round in 0..5 {
        // Write
        let val = format!("round-{round}");
        let req = make_put_request(ns, "rec-1", vec![("data", val.as_bytes())]);
        client.put_items(req).await.unwrap();

        // Verify
        let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"data"]), None);
        let resp = client.get_items(get_req).await.unwrap().into_inner();
        assert_eq!(resp.items[0].value, val.as_bytes());

        // Delete
        let del_req = make_delete_request(ns, "rec-1", match_all_predicate());
        client.delete_items(del_req).await.unwrap();

        // Verify deleted
        let get_req2 = make_get_request(ns, "rec-1", match_all_predicate(), None);
        let resp2 = client.get_items(get_req2).await.unwrap().into_inner();
        assert!(resp2.items.is_empty(), "round {round}: should be empty after delete");
    }

    server.shutdown().await;
}

#[tokio::test]
async fn test_scenario_scan_after_partial_delete() {
    let ns = "scan-del-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Write 10 items
    for i in 0..10 {
        let key = format!("item-{i:03}");
        let val = format!("value-{i:03}");
        let req = make_put_request(ns, "rec-1", vec![(&key, val.as_bytes())]);
        client.put_items(req).await.unwrap();
    }

    // Delete even-numbered items
    for i in (0..10).step_by(2) {
        let key = format!("item-{i:03}");
        let del_req = make_delete_request(ns, "rec-1", match_keys_predicate(vec![key.as_bytes()]));
        client.delete_items(del_req).await.unwrap();
    }

    // Scan should return only odd items
    let scan_req = proto::ScanItemsRequest {
        namespace: ns.to_string(),
        id: "rec-1".to_string(),
        predicate: match_all_predicate(),
        signals: Default::default(),
    };
    let items = collect_scan(&mut client, scan_req).await;
    assert_eq!(items.len(), 5);

    let keys = sorted_keys(&items);
    let expected: Vec<Vec<u8>> = (0..10)
        .filter(|i| i % 2 == 1)
        .map(|i| format!("item-{i:03}").into_bytes())
        .collect();
    assert_eq!(keys, expected);

    server.shutdown().await;
}
