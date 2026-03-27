use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use flushdb_engine::{GetResult, PutBatchItem, RangeReadOptions, RangeReadResult};
use flushdb_types::{FlushError, FlushResult, IdempotencyToken, OrderedKey, StorageBackend};
use tokio::sync::RwLock;

use crate::namespace_config::NamespaceConfig;
use crate::partition::Partition;
use crate::partition_router::{LocalPartitionRouter, PartitionRouter};
use crate::version_generator::VersionGenerator;

pub struct NamespaceState<B: StorageBackend> {
    pub config: NamespaceConfig,
    pub router: LocalPartitionRouter,
    pub partitions: Vec<Arc<RwLock<Partition<B>>>>,
}

pub struct NamespaceManager<B: StorageBackend> {
    namespaces: DashMap<String, NamespaceState<B>>,
    backend: B,
    base_data_dir: PathBuf,
    version_gen: Arc<VersionGenerator>,
}

impl<B: StorageBackend + Clone + 'static> NamespaceManager<B> {
    pub fn new(backend: B, base_data_dir: PathBuf, version_gen: Arc<VersionGenerator>) -> Self {
        Self {
            namespaces: DashMap::new(),
            backend,
            base_data_dir,
            version_gen,
        }
    }

    // --- Namespace CRUD ---

    pub async fn create_namespace(&self, config: NamespaceConfig) -> FlushResult<()> {
        config.validate()?;

        if self.namespaces.contains_key(&config.name) {
            return Err(FlushError::InvalidArgument {
                message: format!("namespace already exists: {}", config.name),
            });
        }

        let mut partitions = Vec::with_capacity(config.partition_count as usize);
        for i in 0..config.partition_count {
            let partition = Partition::open(
                i,
                config.name.clone(),
                self.backend.clone(),
                &self.base_data_dir,
                &config,
            )
            .await?;
            partitions.push(Arc::new(RwLock::new(partition)));
        }

        let router = LocalPartitionRouter::new(&config);

        let state = NamespaceState {
            config,
            router,
            partitions,
        };

        let name = state.config.name.clone();
        self.namespaces.insert(name, state);

        Ok(())
    }

    pub fn get_namespace_config(&self, namespace: &str) -> FlushResult<NamespaceConfig> {
        let entry = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;
        Ok(entry.config.clone())
    }

    pub fn update_namespace_config(&self, config: NamespaceConfig) -> FlushResult<()> {
        let mut entry =
            self.namespaces
                .get_mut(&config.name)
                .ok_or_else(|| FlushError::NotFound {
                    key: format!("namespace: {}", config.name),
                })?;
        entry.config.can_update_from(&config)?;
        entry.config = config;
        Ok(())
    }

    pub async fn delete_namespace(&self, namespace: &str) -> FlushResult<()> {
        let partition_arcs: Vec<Arc<RwLock<Partition<B>>>> = {
            let entry = self
                .namespaces
                .get(namespace)
                .ok_or_else(|| FlushError::NotFound {
                    key: format!("namespace: {}", namespace),
                })?;
            entry.partitions.iter().map(Arc::clone).collect()
        };

        for partition_arc in &partition_arcs {
            let mut partition = partition_arc.write().await;
            partition.stop().await?;
        }

        self.namespaces.remove(namespace);

        Ok(())
    }

    pub fn list_namespaces(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .namespaces
            .iter()
            .map(|entry| entry.key().clone())
            .collect();
        names.sort();
        names
    }

    pub fn namespace_exists(&self, namespace: &str) -> bool {
        self.namespaces.contains_key(namespace)
    }

    // --- Partition Resolution ---
    // Resolves namespace + record_id to a partition Arc without holding the
    // DashMap guard across await points, preventing deadlocks.

    fn resolve_partition(
        &self,
        namespace: &str,
        record_id: &str,
    ) -> FlushResult<Arc<RwLock<Partition<B>>>> {
        let entry = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        let partition_id = entry.router.route(record_id)? as usize;
        let partition =
            entry
                .partitions
                .get(partition_id)
                .ok_or_else(|| FlushError::InvalidArgument {
                    message: format!("partition index out of bounds: {}", partition_id),
                })?;

        Ok(Arc::clone(partition))
    }

    // --- Request Dispatch ---

    pub async fn put(
        &self,
        namespace: &str,
        record_id: &str,
        item_key: &[u8],
        value: Bytes,
        metadata: Bytes,
        idempotency_token: IdempotencyToken,
    ) -> FlushResult<u64> {
        let partition_arc = self.resolve_partition(namespace, record_id)?;
        let mut partition = partition_arc.write().await;
        partition
            .put(
                record_id.as_bytes(),
                item_key,
                value,
                metadata,
                idempotency_token,
            )
            .await
    }

    pub async fn put_batch(
        &self,
        namespace: &str,
        record_id: &str,
        items: Vec<PutBatchItem>,
    ) -> FlushResult<usize> {
        let partition_arc = self.resolve_partition(namespace, record_id)?;
        let mut partition = partition_arc.write().await;
        partition.put_batch(record_id.as_bytes(), items).await
    }

    pub async fn delete(
        &self,
        namespace: &str,
        record_id: &str,
        item_key: &[u8],
    ) -> FlushResult<u64> {
        let partition_arc = self.resolve_partition(namespace, record_id)?;
        let mut partition = partition_arc.write().await;
        partition.delete(record_id.as_bytes(), item_key).await
    }

    pub async fn delete_range(
        &self,
        namespace: &str,
        record_id: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> FlushResult<u64> {
        let partition_arc = self.resolve_partition(namespace, record_id)?;
        let mut partition = partition_arc.write().await;
        partition
            .delete_range(record_id.as_bytes(), start_key, end_key)
            .await
    }

    pub async fn get(
        &self,
        namespace: &str,
        record_id: &str,
        item_key: &[u8],
    ) -> FlushResult<Option<GetResult>> {
        let partition_arc = self.resolve_partition(namespace, record_id)?;
        let partition = partition_arc.read().await;
        partition.get(record_id.as_bytes(), item_key).await
    }

    pub async fn scan(
        &self,
        namespace: &str,
        record_id: &str,
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        options: RangeReadOptions,
    ) -> FlushResult<RangeReadResult> {
        let partition_arc = self.resolve_partition(namespace, record_id)?;
        let partition = partition_arc.read().await;
        partition
            .scan(record_id.as_bytes(), start_key, end_key, options)
            .await
    }

    pub async fn multi_get(
        &self,
        namespace: &str,
        record_id: &str,
        keys: &[&[u8]],
    ) -> FlushResult<Vec<Option<GetResult>>> {
        let partition_arc = self.resolve_partition(namespace, record_id)?;
        let partition = partition_arc.read().await;
        partition.multi_get(record_id.as_bytes(), keys).await
    }

    // --- Background Maintenance ---

    pub async fn run_maintenance(&self) -> FlushResult<()> {
        let partition_arcs: Vec<Arc<RwLock<Partition<B>>>> = self
            .namespaces
            .iter()
            .flat_map(|entry| entry.partitions.iter().map(Arc::clone).collect::<Vec<_>>())
            .collect();

        for partition_arc in &partition_arcs {
            let mut partition = partition_arc.write().await;
            partition.run_maintenance().await?;
        }
        Ok(())
    }

    pub async fn flush_all(&self) -> FlushResult<()> {
        let partition_arcs: Vec<Arc<RwLock<Partition<B>>>> = self
            .namespaces
            .iter()
            .flat_map(|entry| entry.partitions.iter().map(Arc::clone).collect::<Vec<_>>())
            .collect();

        for partition_arc in &partition_arcs {
            let mut partition = partition_arc.write().await;
            partition.maybe_flush().await?;
        }
        Ok(())
    }

    pub async fn stop_all(&self) -> FlushResult<()> {
        let partition_arcs: Vec<Arc<RwLock<Partition<B>>>> = self
            .namespaces
            .iter()
            .flat_map(|entry| entry.partitions.iter().map(Arc::clone).collect::<Vec<_>>())
            .collect();

        for partition_arc in &partition_arcs {
            let mut partition = partition_arc.write().await;
            partition.stop().await?;
        }
        Ok(())
    }

    // --- Version Generation ---

    pub fn next_version(&self) -> OrderedKey {
        self.version_gen.next_version()
    }

    // --- Status Methods ---

    pub fn namespace_count(&self) -> usize {
        self.namespaces.len()
    }

    pub fn partition_count(&self, namespace: &str) -> FlushResult<u32> {
        let entry = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;
        Ok(entry.config.partition_count)
    }

    pub fn total_partition_count(&self) -> usize {
        self.namespaces
            .iter()
            .map(|entry| entry.partitions.len())
            .sum()
    }
}
