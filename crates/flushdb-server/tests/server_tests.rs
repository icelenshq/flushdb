use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use flushdb_server::config::ServerConfig;
use flushdb_server::server::FlushDbServer;
use flushdb_types::LocalFsBackend;

async fn free_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind to random port");
    listener
        .local_addr()
        .expect("local addr")
        .port()
}

fn test_config(grpc_port: u16, metrics_port: u16, data_dir: &Path) -> ServerConfig {
    ServerConfig {
        grpc_listen_addr: SocketAddr::from(([127, 0, 0, 1], grpc_port)),
        metrics_listen_addr: SocketAddr::from(([127, 0, 0, 1], metrics_port)),
        data_dir: data_dir.to_path_buf(),
        ..ServerConfig::default()
    }
}

// ─── Config Tests ───

#[test]
fn test_config_defaults() {
    let config = ServerConfig::default();

    assert_eq!(
        config.grpc_listen_addr,
        "0.0.0.0:50051".parse::<SocketAddr>().expect("parse")
    );
    assert_eq!(
        config.metrics_listen_addr,
        "0.0.0.0:9090".parse::<SocketAddr>().expect("parse")
    );
    assert_eq!(config.node_id, 0);
    assert_eq!(config.data_dir.to_str().expect("utf8"), "./data");
    assert!(config.s3_bucket.is_none());
    assert_eq!(config.default_partition_count, 4);
    assert_eq!(config.default_memtable_size_mb, 64);
    assert_eq!(config.log_level, "info");
    assert_eq!(config.log_format, "json");
    assert_eq!(config.maintenance_interval_ms, 1000);
    assert_eq!(config.stats_interval_ms, 10000);
}

#[test]
fn test_config_from_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config_path = tmp.path().join("config.json");

    let json = r#"{
        "grpc_listen_addr": "127.0.0.1:60000",
        "metrics_listen_addr": "127.0.0.1:60001",
        "node_id": 7,
        "data_dir": "/tmp/mydb",
        "s3_bucket": "my-bucket",
        "default_partition_count": 8,
        "default_memtable_size_mb": 128,
        "log_level": "debug",
        "log_format": "pretty",
        "maintenance_interval_ms": 500,
        "stats_interval_ms": 5000
    }"#;

    std::fs::write(&config_path, json).expect("write config");

    let config = ServerConfig::from_file(&config_path).expect("load config");

    assert_eq!(
        config.grpc_listen_addr,
        "127.0.0.1:60000".parse::<SocketAddr>().expect("parse")
    );
    assert_eq!(
        config.metrics_listen_addr,
        "127.0.0.1:60001".parse::<SocketAddr>().expect("parse")
    );
    assert_eq!(config.node_id, 7);
    assert_eq!(config.data_dir.to_str().expect("utf8"), "/tmp/mydb");
    assert_eq!(config.s3_bucket, Some("my-bucket".to_string()));
    assert_eq!(config.default_partition_count, 8);
    assert_eq!(config.default_memtable_size_mb, 128);
    assert_eq!(config.log_level, "debug");
    assert_eq!(config.log_format, "pretty");
    assert_eq!(config.maintenance_interval_ms, 500);
    assert_eq!(config.stats_interval_ms, 5000);
}

#[test]
fn test_config_from_file_with_defaults() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config_path = tmp.path().join("minimal.json");

    std::fs::write(&config_path, "{}").expect("write config");

    let config = ServerConfig::from_file(&config_path).expect("load config");
    let defaults = ServerConfig::default();

    assert_eq!(config.grpc_listen_addr, defaults.grpc_listen_addr);
    assert_eq!(config.node_id, defaults.node_id);
    assert_eq!(config.default_partition_count, defaults.default_partition_count);
    assert_eq!(config.log_level, defaults.log_level);
}

#[test]
fn test_config_from_file_invalid_json() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let config_path = tmp.path().join("bad.json");

    std::fs::write(&config_path, "not json").expect("write");

    let err = ServerConfig::from_file(&config_path).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("invalid"), "error: {msg}");
}

#[test]
fn test_config_from_file_missing() {
    let result = ServerConfig::from_file(Path::new("/nonexistent/config.json"));
    assert!(result.is_err());
}

#[test]
fn test_config_serde_round_trip() {
    let original = ServerConfig {
        grpc_listen_addr: "10.0.0.1:8080".parse().expect("parse"),
        metrics_listen_addr: "10.0.0.1:9090".parse().expect("parse"),
        node_id: 42,
        data_dir: "/var/lib/flushdb".into(),
        s3_bucket: Some("prod-bucket".to_string()),
        default_partition_count: 16,
        default_memtable_size_mb: 256,
        log_level: "warn".to_string(),
        log_format: "pretty".to_string(),
        maintenance_interval_ms: 2000,
        stats_interval_ms: 30000,
        default_namespaces: vec![("test".to_string(), 4)],
    };

    let serialized = serde_json::to_string(&original).expect("serialize");
    let deserialized: ServerConfig = serde_json::from_str(&serialized).expect("deserialize");

    assert_eq!(original.grpc_listen_addr, deserialized.grpc_listen_addr);
    assert_eq!(original.metrics_listen_addr, deserialized.metrics_listen_addr);
    assert_eq!(original.node_id, deserialized.node_id);
    assert_eq!(original.data_dir, deserialized.data_dir);
    assert_eq!(original.s3_bucket, deserialized.s3_bucket);
    assert_eq!(original.default_partition_count, deserialized.default_partition_count);
    assert_eq!(original.default_memtable_size_mb, deserialized.default_memtable_size_mb);
    assert_eq!(original.log_level, deserialized.log_level);
    assert_eq!(original.log_format, deserialized.log_format);
    assert_eq!(original.maintenance_interval_ms, deserialized.maintenance_interval_ms);
    assert_eq!(original.stats_interval_ms, deserialized.stats_interval_ms);
}

// ─── Server Lifecycle Tests ───

#[tokio::test]
async fn test_server_creates_with_config() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let grpc_port = free_port().await;
    let metrics_port = free_port().await;
    let config = test_config(grpc_port, metrics_port, tmp.path());

    let backend = LocalFsBackend::new(tmp.path().join("storage"));
    let server = FlushDbServer::new(config.clone(), backend);

    assert_eq!(server.config().grpc_listen_addr, config.grpc_listen_addr);
    assert_eq!(server.namespace_manager().namespace_count(), 0);
}

#[tokio::test]
async fn test_server_starts_and_stops() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let grpc_port = free_port().await;
    let metrics_port = free_port().await;
    let config = test_config(grpc_port, metrics_port, tmp.path());

    let backend = LocalFsBackend::new(tmp.path().join("storage"));
    let server = Arc::new(FlushDbServer::new(config, backend));

    let server_clone = server.clone();
    let server_handle = tokio::spawn(async move {
        server_clone.start().await
    });

    // Give the server a moment to bind
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trigger shutdown
    server.shutdown().await.expect("shutdown");

    // Wait for the server to stop
    let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
        .await
        .expect("server should stop within 5s")
        .expect("join handle");

    result.expect("server should stop cleanly");
}

#[tokio::test]
async fn test_server_shutdown_is_idempotent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let grpc_port = free_port().await;
    let metrics_port = free_port().await;
    let config = test_config(grpc_port, metrics_port, tmp.path());

    let backend = LocalFsBackend::new(tmp.path().join("storage"));
    let server = Arc::new(FlushDbServer::new(config, backend));

    let server_clone = server.clone();
    let server_handle = tokio::spawn(async move {
        server_clone.start().await
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    server.shutdown().await.expect("first shutdown");
    server.shutdown().await.expect("second shutdown should also succeed");

    let result = tokio::time::timeout(Duration::from_secs(5), server_handle)
        .await
        .expect("server should stop")
        .expect("join handle");

    result.expect("server should stop cleanly");
}

// ─── Maintenance Loop Tests ───

#[tokio::test]
async fn test_maintenance_loop_stops_on_shutdown() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let backend = LocalFsBackend::new(tmp.path().join("storage"));
    let version_gen = Arc::new(flushdb_server::VersionGenerator::new(0));
    let manager = Arc::new(flushdb_server::NamespaceManager::new(
        backend,
        tmp.path().to_path_buf(),
        version_gen,
    ));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let manager_clone = manager.clone();
    let handle = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    let _ = manager_clone.run_maintenance().await;
                }
                _ = {
                    let mut rx = shutdown_rx.clone();
                    async move {
                        let _ = rx.changed().await;
                    }
                } => {
                    break;
                }
            }
        }
    });

    // Let the loop run a few iterations
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Signal shutdown
    let _ = shutdown_tx.send(true);

    // The loop should exit promptly
    let result = tokio::time::timeout(Duration::from_secs(2), handle)
        .await
        .expect("loop should stop within 2s")
        .expect("join handle");

    // No return value to check, just ensure it completed
    let _ = result;
}

// ─── Config from_env Tests ───

#[tokio::test]
async fn test_config_from_env_uses_defaults_when_no_vars() {
    // from_env should produce defaults when no FLUSHDB_* env vars are set.
    // We rely on the fact that these vars are not set in the test env.
    // If they are, this test would still pass since from_env is fallible.
    let config = ServerConfig::from_env().expect("from_env with defaults");
    let defaults = ServerConfig::default();

    assert_eq!(config.log_level, defaults.log_level);
    assert_eq!(config.log_format, defaults.log_format);
}
