use std::sync::Arc;

use tempfile::TempDir;
use tokio::sync::watch;
use tonic::transport::Channel;

use flushdb_proto::flushdb::v1 as proto;
use flushdb_proto::flushdb::v1::flush_db_client::FlushDbClient;
use flushdb_proto::flushdb::v1::flush_db_server::FlushDbServer as TonicFlushDbServer;
use flushdb_server::{FlushDbService, NamespaceConfig, NamespaceManager, VersionGenerator};
use flushdb_types::LocalFsBackend;

// ---------------------------------------------------------------------------
// Test Server Harness
// ---------------------------------------------------------------------------

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
            server.namespace_manager.create_namespace(config).await.unwrap();
        }
        server
    }

    async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.server_handle.await;
    }
}

// ---------------------------------------------------------------------------
// Request / Predicate Helpers
// ---------------------------------------------------------------------------

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

#[allow(dead_code)]
fn match_range_predicate(start: &[u8], end: &[u8]) -> Option<proto::Predicate> {
    Some(proto::Predicate {
        predicate: Some(proto::predicate::Predicate::MatchRange(proto::MatchRange {
            start_key: start.to_vec(),
            end_key: end.to_vec(),
            start_inclusive: true,
            end_inclusive: false,
        })),
    })
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

// ---------------------------------------------------------------------------
// Full CRUD Cycle Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_crud_put_get_delete_get() {
    let ns = "crud-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Put
    let put_req = make_put_request(ns, "rec-1", vec![("key-a", b"val-a")]);
    let put_resp = client.put_items(put_req).await.unwrap().into_inner();
    assert!(put_resp.version.is_some());

    // Get — should see data
    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"key-a"]), None);
    let get_resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(get_resp.items.len(), 1);
    assert_eq!(get_resp.items[0].key, b"key-a");
    assert_eq!(get_resp.items[0].value, b"val-a");

    // Delete
    let del_req = make_delete_request(ns, "rec-1", match_keys_predicate(vec![b"key-a"]));
    let del_resp = client.delete_items(del_req).await.unwrap().into_inner();
    assert!(del_resp.version.is_some());

    // Get after delete — should be empty
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

    // First write
    let req1 = make_put_request(ns, "rec-1", vec![("key-x", b"original")]);
    client.put_items(req1).await.unwrap();

    // Overwrite same record+key
    let req2 = make_put_request(ns, "rec-1", vec![("key-x", b"updated")]);
    client.put_items(req2).await.unwrap();

    // Get — should return the latest value
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

    // GetItems with MatchAll
    let get_req = make_get_request(ns, "rec-1", match_all_predicate(), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 5);

    let mut returned_keys: Vec<Vec<u8>> = resp.items.iter().map(|i| i.key.clone()).collect();
    returned_keys.sort();
    let expected_keys: Vec<Vec<u8>> = vec![
        b"item-a".to_vec(),
        b"item-b".to_vec(),
        b"item-c".to_vec(),
        b"item-d".to_vec(),
        b"item-e".to_vec(),
    ];
    assert_eq!(returned_keys, expected_keys);

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Namespace Isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_namespace_isolation_writes() {
    let ns_a = "iso-a";
    let ns_b = "iso-b";
    let config_a = NamespaceConfig::new(ns_a.to_string(), 1).unwrap();
    let config_b = NamespaceConfig::new(ns_b.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config_a, config_b]).await;
    let mut client = server.client.clone();

    // Write to ns-A
    let req = make_put_request(ns_a, "rec-1", vec![("key-1", b"a-value")]);
    client.put_items(req).await.unwrap();

    // Read from ns-A — should have data
    let get_a = make_get_request(ns_a, "rec-1", match_keys_predicate(vec![b"key-1"]), None);
    let resp_a = client.get_items(get_a).await.unwrap().into_inner();
    assert_eq!(resp_a.items.len(), 1);
    assert_eq!(resp_a.items[0].value, b"a-value");

    // Read same record+key from ns-B — should be empty
    let get_b = make_get_request(ns_b, "rec-1", match_keys_predicate(vec![b"key-1"]), None);
    let resp_b = client.get_items(get_b).await.unwrap().into_inner();
    assert!(resp_b.items.is_empty());

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Partition Routing
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_partition_simple_routing() {
    let ns = "routing-ns";
    let config = NamespaceConfig::new(ns.to_string(), 4).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Write several records — each will be routed to a partition
    for i in 0..10 {
        let record_id = format!("rec-{i}");
        let key = format!("key-{i}");
        let value = format!("value-{i}");
        let req = make_put_request(ns, &record_id, vec![(&key, value.as_bytes())]);
        client.put_items(req).await.unwrap();
    }

    // Verify each record is consistently accessible
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

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_bypass_token_allows_multiple_writes() {
    let ns = "idemp-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // All-zero token (bypass) — should allow multiple writes
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

    // The last write wins
    let get_req = make_get_request(ns, "rec-1", match_keys_predicate(vec![b"key-1"]), None);
    let resp = client.get_items(get_req).await.unwrap().into_inner();
    assert_eq!(resp.items.len(), 1);
    assert_eq!(resp.items[0].value, b"write-2");

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// Pagination
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_pagination_full_traversal() {
    let ns = "page-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Write 20 items under a single record
    let items: Vec<(&str, &[u8])> = (0..20)
        .map(|i| {
            // Use zero-padded keys so they sort lexicographically
            let key: &str = Box::leak(format!("k-{i:03}").into_boxed_str());
            let val: &[u8] = Box::leak(format!("v-{i:03}").into_boxed_str()).as_bytes();
            (key, val)
        })
        .collect();
    let req = make_put_request(ns, "rec-1", items);
    client.put_items(req).await.unwrap();

    // Paginate with small item_limit
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

    // Verify no gaps — all 20 keys present
    let mut collected_keys: Vec<Vec<u8>> = all_items.iter().map(|i| i.key.clone()).collect();
    collected_keys.sort();
    let expected_keys: Vec<Vec<u8>> = (0..20)
        .map(|i| format!("k-{i:03}").into_bytes())
        .collect();
    assert_eq!(collected_keys, expected_keys);

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// ScanItems Streaming
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_scan_streams_all_items() {
    let ns = "scan-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    let items: Vec<(&str, &[u8])> = vec![
        ("scan-a", b"v1"),
        ("scan-b", b"v2"),
        ("scan-c", b"v3"),
    ];
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

    let mut keys: Vec<Vec<u8>> = items.iter().map(|i| i.key.clone()).collect();
    keys.sort();
    assert_eq!(
        keys,
        vec![b"scan-a".to_vec(), b"scan-b".to_vec(), b"scan-c".to_vec()]
    );

    server.shutdown().await;
}

// ---------------------------------------------------------------------------
// OrderedKey Versions
// ---------------------------------------------------------------------------

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

        // Each version should be >= the previous (timestamp_ms, then sequence)
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

// ---------------------------------------------------------------------------
// Error Handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_nonexistent_namespace_not_found() {
    let ns = "exists-ns";
    let config = NamespaceConfig::new(ns.to_string(), 1).unwrap();
    let server = TestServer::start_with_namespaces(vec![config]).await;
    let mut client = server.client.clone();

    // Put to nonexistent namespace
    let put_req = make_put_request("no-such-ns", "rec-1", vec![("k", b"v")]);
    let err = client.put_items(put_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Get from nonexistent namespace
    let get_req = make_get_request(
        "no-such-ns",
        "rec-1",
        match_keys_predicate(vec![b"k"]),
        None,
    );
    let err = client.get_items(get_req).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::NotFound);

    // Delete from nonexistent namespace
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

// ---------------------------------------------------------------------------
// Graceful Shutdown — Data Persistence
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_graceful_shutdown_preserves_data() {
    let dir = TempDir::new().unwrap();
    let dir_path = dir.path().to_path_buf();
    let ns = "persist-ns";

    // NamespaceConfig::new generates s3_path_prefix "flushdb/{name}/" with trailing slash.
    // On LocalFsBackend the OS normalises "a//b" to "a/b", which breaks list_prefix
    // matching on restart. Strip the trailing slash so paths round-trip correctly.
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
        namespace_manager.create_namespace(ns_config).await.unwrap();

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

        // Write data
        let req = make_put_request(ns, "rec-1", vec![("persist-key", b"persist-val")]);
        client.put_items(req).await.unwrap();

        // Graceful shutdown — engine close flushes WAL/memtable
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

        // Re-create namespace pointing at same data dir — engine reopens WAL
        let ns_config = make_ns_config();
        namespace_manager.create_namespace(ns_config).await.unwrap();

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

        // Read the data that was written by the first server instance
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
