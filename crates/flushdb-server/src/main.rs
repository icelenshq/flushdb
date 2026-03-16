use flushdb_server::config::ServerConfig;
use flushdb_server::observability::init_tracing;
use flushdb_server::server::{install_signal_handlers, FlushDbServer};
use flushdb_types::LocalFsBackend;

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
        install_signal_handlers(server.shutdown_sender());
        server.start().await?;
    } else {
        let backend = LocalFsBackend::new(config.data_dir.join("storage"));
        let server = FlushDbServer::new(config, backend);
        install_signal_handlers(server.shutdown_sender());
        server.start().await?;
    }

    Ok(())
}
