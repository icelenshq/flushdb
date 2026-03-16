use std::sync::Arc;
use std::time::Duration;

use flushdb_proto::flushdb::v1::flush_db_server::FlushDbServer as TonicFlushDbServer;
use flushdb_types::{FlushError, FlushResult, StorageBackend};

use crate::config::ServerConfig;
use crate::handlers::FlushDbService;
use crate::namespace_manager::NamespaceManager;
use crate::version_generator::VersionGenerator;

pub struct FlushDbServer<B: StorageBackend> {
    config: ServerConfig,
    namespace_manager: Arc<NamespaceManager<B>>,
    version_generator: Arc<VersionGenerator>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    shutdown_rx: tokio::sync::watch::Receiver<bool>,
}

impl<B: StorageBackend + Clone + Send + Sync + 'static> FlushDbServer<B> {
    pub fn new(config: ServerConfig, backend: B) -> Self {
        let version_gen = Arc::new(VersionGenerator::new(config.node_id));
        let manager = Arc::new(NamespaceManager::new(
            backend,
            config.data_dir.clone(),
            version_gen.clone(),
        ));
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Self {
            config,
            namespace_manager: manager,
            version_generator: version_gen,
            shutdown_tx,
            shutdown_rx,
        }
    }

    pub fn namespace_manager(&self) -> &Arc<NamespaceManager<B>> {
        &self.namespace_manager
    }

    pub fn config(&self) -> &ServerConfig {
        &self.config
    }

    pub fn shutdown_sender(&self) -> tokio::sync::watch::Sender<bool> {
        self.shutdown_tx.clone()
    }

    pub async fn start(&self) -> FlushResult<()> {
        let maint_handle = spawn_maintenance_loop(
            self.namespace_manager.clone(),
            Duration::from_millis(self.config.maintenance_interval_ms),
            self.shutdown_rx.clone(),
        );

        let service = FlushDbService::new(
            self.namespace_manager.clone(),
            self.version_generator.clone(),
        );
        let svc = TonicFlushDbServer::new(service);

        let shutdown_rx = self.shutdown_rx.clone();
        let shutdown_signal = async move {
            let mut rx = shutdown_rx;
            while !*rx.borrow() {
                if rx.changed().await.is_err() {
                    break;
                }
            }
        };

        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_shutdown(self.config.grpc_listen_addr, shutdown_signal)
            .await
            .map_err(|e| FlushError::Io(std::io::Error::other(e.to_string())))?;

        maint_handle.abort();
        self.namespace_manager.stop_all().await?;

        Ok(())
    }

    pub async fn shutdown(&self) -> FlushResult<()> {
        let _ = self.shutdown_tx.send(true);
        Ok(())
    }
}

fn spawn_maintenance_loop<B: StorageBackend + Clone + Send + Sync + 'static>(
    namespace_manager: Arc<NamespaceManager<B>>,
    interval: Duration,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    let _ = namespace_manager.run_maintenance().await;
                }
                _ = shutdown_rx.changed() => {
                    break;
                }
            }
        }
    })
}

pub fn install_signal_handlers(
    shutdown_tx: tokio::sync::watch::Sender<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Received shutdown signal");
        let _ = shutdown_tx.send(true);
    })
}
