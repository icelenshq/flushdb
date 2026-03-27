use flushdb_server::config::ServerConfig;
use flushdb_server::namespace_config::NamespaceConfig;
use flushdb_server::observability::init_tracing;
use flushdb_server::server::{install_signal_handlers, FlushDbServer};
use flushdb_types::{LocalFsBackend, StorageBackend};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::from_env()?;
    init_tracing(&config.log_level)?;

    tracing::info!(
        grpc_addr = %config.grpc_listen_addr,
        "Starting flushdb server",
    );

    if let Some(ref bucket) = config.s3_bucket {
        let backend =
            flushdb_server::s3_backend::S3StorageBackend::from_env(bucket.clone()).await?;
        let server = FlushDbServer::new(config, backend);
        create_default_namespaces(&server).await;
        install_signal_handlers(server.shutdown_sender());
        server.start().await?;
    } else {
        let backend = LocalFsBackend::new(config.data_dir.join("storage"));
        let server = FlushDbServer::new(config, backend);
        create_default_namespaces(&server).await;
        install_signal_handlers(server.shutdown_sender());
        server.start().await?;
    }

    Ok(())
}

async fn create_default_namespaces<B: StorageBackend + Clone + Send + Sync + 'static>(
    server: &FlushDbServer<B>,
) {
    let memtable_bytes = server.config().default_memtable_size_mb * 1024 * 1024;
    for (name, partition_count) in &server.config().default_namespaces {
        match NamespaceConfig::new(name.clone(), *partition_count) {
            Ok(mut ns_config) => {
                ns_config.memtable_size_threshold = memtable_bytes;
                ns_config.wal_fsync_mode = server.config().default_wal_fsync_mode.clone();
                ns_config.wal_group_commit_interval_us =
                    server.config().default_wal_group_commit_interval_us;
                ns_config.wal_batch_sync_interval_ms =
                    server.config().default_wal_batch_sync_interval_ms;
                match server.namespace_manager().create_namespace(ns_config).await {
                    Ok(()) => {
                        tracing::info!(
                            namespace = %name,
                            partitions = partition_count,
                            "Created default namespace",
                        );
                    }
                    Err(e) if e.to_string().contains("namespace already exists") => {
                        tracing::info!(
                            namespace = %name,
                            "Default namespace already exists, skipping",
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            namespace = %name,
                            error = %e,
                            "Failed to create default namespace",
                        );
                    }
                }
            }
            Err(e) => {
                tracing::error!(
                    namespace = %name,
                    error = %e,
                    "Invalid default namespace config",
                );
            }
        }
    }
}
