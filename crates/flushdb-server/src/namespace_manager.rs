use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use flushdb_engine::{GetResult, RangeReadOptions, RangeReadResult};
use flushdb_types::{FlushError, FlushResult, IdempotencyToken, OrderedKey, StorageBackend};

use crate::namespace_config::NamespaceConfig;
use crate::partition::Partition;
use crate::partition_router::{LocalPartitionRouter, PartitionRouter};
use crate::version_generator::VersionGenerator;

pub struct NamespaceState<B: StorageBackend> {
    pub config: NamespaceConfig,
    pub router: LocalPartitionRouter,
    pub partitions: Vec<Partition<B>>,
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
            partitions.push(partition);
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
        let mut entry = self
            .namespaces
            .get_mut(&config.name)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", config.name),
            })?;
        entry.config.can_update_from(&config)?;
        entry.config = config;
        Ok(())
    }

    pub async fn delete_namespace(&self, namespace: &str) -> FlushResult<()> {
        let mut entry = self
            .namespaces
            .get_mut(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        for partition in entry.partitions.iter_mut() {
            partition.stop().await?;
        }

        drop(entry);
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
        let mut entry = self
            .namespaces
            .get_mut(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        let partition_id = entry.router.route(record_id)? as usize;
        let partition = entry
            .partitions
            .get_mut(partition_id)
            .ok_or_else(|| FlushError::InvalidArgument {
                message: format!("partition index out of bounds: {}", partition_id),
            })?;

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

    pub async fn delete(
        &self,
        namespace: &str,
        record_id: &str,
        item_key: &[u8],
    ) -> FlushResult<u64> {
        let mut entry = self
            .namespaces
            .get_mut(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        let partition_id = entry.router.route(record_id)? as usize;
        let partition = entry
            .partitions
            .get_mut(partition_id)
            .ok_or_else(|| FlushError::InvalidArgument {
                message: format!("partition index out of bounds: {}", partition_id),
            })?;

        partition.delete(record_id.as_bytes(), item_key).await
    }

    pub async fn delete_range(
        &self,
        namespace: &str,
        record_id: &str,
        start_key: &[u8],
        end_key: &[u8],
    ) -> FlushResult<u64> {
        let mut entry = self
            .namespaces
            .get_mut(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        let partition_id = entry.router.route(record_id)? as usize;
        let partition = entry
            .partitions
            .get_mut(partition_id)
            .ok_or_else(|| FlushError::InvalidArgument {
                message: format!("partition index out of bounds: {}", partition_id),
            })?;

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
        let entry = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        let partition_id = entry.router.route(record_id)? as usize;
        let partition = entry
            .partitions
            .get(partition_id)
            .ok_or_else(|| FlushError::InvalidArgument {
                message: format!("partition index out of bounds: {}", partition_id),
            })?;

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
        let entry = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        let partition_id = entry.router.route(record_id)? as usize;
        let partition = entry
            .partitions
            .get(partition_id)
            .ok_or_else(|| FlushError::InvalidArgument {
                message: format!("partition index out of bounds: {}", partition_id),
            })?;

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
        let entry = self
            .namespaces
            .get(namespace)
            .ok_or_else(|| FlushError::NotFound {
                key: format!("namespace: {}", namespace),
            })?;

        let partition_id = entry.router.route(record_id)? as usize;
        let partition = entry
            .partitions
            .get(partition_id)
            .ok_or_else(|| FlushError::InvalidArgument {
                message: format!("partition index out of bounds: {}", partition_id),
            })?;

        partition.multi_get(record_id.as_bytes(), keys).await
    }

    // --- Background Maintenance ---

    pub async fn run_maintenance(&self) -> FlushResult<()> {
        for mut entry in self.namespaces.iter_mut() {
            for partition in entry.partitions.iter_mut() {
                partition.maybe_flush().await?;
                partition.maybe_compact().await?;
            }
        }
        Ok(())
    }

    pub async fn flush_all(&self) -> FlushResult<()> {
        for mut entry in self.namespaces.iter_mut() {
            for partition in entry.partitions.iter_mut() {
                partition.maybe_flush().await?;
            }
        }
        Ok(())
    }

    pub async fn stop_all(&self) -> FlushResult<()> {
        for mut entry in self.namespaces.iter_mut() {
            for partition in entry.partitions.iter_mut() {
                partition.stop().await?;
            }
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
