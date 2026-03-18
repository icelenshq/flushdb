use std::path::{Path, PathBuf};
use std::time::Instant;

use bytes::Bytes;
use flushdb_engine::{
    CacheStats, CompactionResult, Engine, FlushResult_, GetResult, ManifestId, RangeReadOptions,
    RangeReadResult, WriteStallStatus,
};
use flushdb_types::{FlushError, FlushResult, IdempotencyToken, StorageBackend};

use crate::namespace_config::NamespaceConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionState {
    Starting,
    Active,
    Frozen,
    Draining,
    Stopped,
}

pub struct Partition<B: StorageBackend> {
    partition_id: u32,
    namespace: String,
    engine: Engine<B>,
    state: PartitionState,
    wal_dir: PathBuf,
    created_at: Instant,
}

impl<B: StorageBackend + Clone + 'static> Partition<B> {
    pub async fn open(
        partition_id: u32,
        namespace: String,
        backend: B,
        base_data_dir: &Path,
        config: &NamespaceConfig,
    ) -> FlushResult<Self> {
        let partition_dir =
            base_data_dir.join(format!("{}/partition-{:04}", namespace, partition_id));
        std::fs::create_dir_all(&partition_dir)?;

        let mut engine_config = config.engine_config(partition_dir.clone());
        engine_config.namespace = format!("partition-{:04}", partition_id);
        let engine = Engine::open(backend, engine_config).await?;

        Ok(Self {
            partition_id,
            namespace,
            engine,
            state: PartitionState::Active,
            wal_dir: partition_dir,
            created_at: Instant::now(),
        })
    }

    // --- Lifecycle Methods ---

    pub fn freeze(&mut self) -> FlushResult<()> {
        if self.state != PartitionState::Active {
            return Err(FlushError::InvalidArgument {
                message: format!("cannot freeze partition in state: {:?}", self.state),
            });
        }
        self.state = PartitionState::Frozen;
        Ok(())
    }

    pub async fn drain(&mut self) -> FlushResult<()> {
        if self.state != PartitionState::Frozen {
            return Err(FlushError::InvalidArgument {
                message: format!("cannot drain partition in state: {:?}", self.state),
            });
        }
        self.state = PartitionState::Draining;
        // close() freezes the engine's active memtable and flushes all frozen
        // memtables to SSTables, ensuring all in-memory data is persisted.
        self.engine.close().await?;
        Ok(())
    }

    pub async fn stop(&mut self) -> FlushResult<()> {
        self.engine.close().await?;
        self.state = PartitionState::Stopped;
        Ok(())
    }

    pub fn state(&self) -> PartitionState {
        self.state
    }

    pub fn is_writable(&self) -> bool {
        self.state == PartitionState::Active
    }

    pub fn is_readable(&self) -> bool {
        matches!(self.state, PartitionState::Active | PartitionState::Frozen)
    }

    // --- Write Methods ---

    pub async fn put(
        &mut self,
        record_id: &[u8],
        item_key: &[u8],
        value: Bytes,
        metadata: Bytes,
        idempotency_token: IdempotencyToken,
    ) -> FlushResult<u64> {
        self.check_writable()?;
        self.engine
            .put(record_id, item_key, value, metadata, Some(idempotency_token))
            .await
    }

    pub async fn delete(&mut self, record_id: &[u8], item_key: &[u8]) -> FlushResult<u64> {
        self.check_writable()?;
        self.engine.delete(record_id, item_key).await
    }

    pub async fn delete_range(
        &mut self,
        record_id: &[u8],
        start_key: &[u8],
        end_key: &[u8],
    ) -> FlushResult<u64> {
        self.check_writable()?;
        self.engine.delete_range(record_id, start_key, end_key).await
    }

    // --- Read Methods ---

    pub async fn get(
        &self,
        record_id: &[u8],
        item_key: &[u8],
    ) -> FlushResult<Option<GetResult>> {
        self.check_readable()?;
        self.engine.get(record_id, item_key).await
    }

    pub async fn scan(
        &self,
        record_id: &[u8],
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        options: RangeReadOptions,
    ) -> FlushResult<RangeReadResult> {
        self.check_readable()?;
        self.engine.scan(record_id, start_key, end_key, options).await
    }

    pub async fn multi_get(
        &self,
        record_id: &[u8],
        keys: &[&[u8]],
    ) -> FlushResult<Vec<Option<GetResult>>> {
        self.check_readable()?;
        self.engine.multi_get(record_id, keys).await
    }

    // --- Maintenance Methods ---

    pub async fn run_maintenance(&mut self) -> FlushResult<()> {
        self.engine.run_maintenance().await?;
        self.maybe_compact().await?;
        Ok(())
    }

    pub async fn maybe_flush(&mut self) -> FlushResult<Option<FlushResult_>> {
        self.engine.maybe_flush().await
    }

    pub async fn maybe_compact(&mut self) -> FlushResult<Vec<CompactionResult>> {
        self.engine.maybe_compact().await
    }

    // --- Status Methods ---

    pub fn partition_id(&self) -> u32 {
        self.partition_id
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn write_stall_status(&self) -> WriteStallStatus {
        self.engine.write_stall_status()
    }

    pub fn manifest_version(&self) -> ManifestId {
        self.engine.manifest_version()
    }

    pub fn cache_stats(&self) -> CacheStats {
        self.engine.cache_stats()
    }

    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    // --- Internal Helpers ---

    fn check_writable(&self) -> FlushResult<()> {
        if !self.is_writable() {
            return Err(FlushError::ResourceExhausted {
                resource: "partition".into(),
                message: format!(
                    "partition {} is not accepting writes (state: {:?})",
                    self.partition_id, self.state
                ),
            });
        }
        Ok(())
    }

    fn check_readable(&self) -> FlushResult<()> {
        if !self.is_readable() {
            return Err(FlushError::ResourceExhausted {
                resource: "partition".into(),
                message: format!(
                    "partition {} is not accepting reads (state: {:?})",
                    self.partition_id, self.state
                ),
            });
        }
        Ok(())
    }
}
