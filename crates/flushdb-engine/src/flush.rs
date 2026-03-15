use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flushdb_types::{FlushError, FlushResult, MemtableEntry, StorageBackend};

use crate::manifest::manager::ManifestManager;
use crate::manifest::types::{Level, ManifestUpdate, ManifestUpdateTrigger, SSTableMeta};
use crate::memtable::Memtable;
use crate::sstable::types::SstConfig;
use crate::sstable::writer::{SSTableWriter, SstInfo, generate_sst_path};

#[derive(Clone, Debug)]
pub struct FlushConfig {
    pub sst_config: SstConfig,
    pub max_frozen_count: usize,
    pub flush_trigger_size: usize,
    pub flush_trigger_age: Duration,
}

impl Default for FlushConfig {
    fn default() -> Self {
        Self {
            sst_config: SstConfig::default(),
            max_frozen_count: 3,
            flush_trigger_size: 64 * 1024 * 1024,
            flush_trigger_age: Duration::from_secs(300),
        }
    }
}

#[derive(Debug)]
pub struct FlushResult_ {
    pub sst_info: SstInfo,
    pub sst_meta: SSTableMeta,
    pub flushed_sequence_range: (u64, u64),
    pub generation_id: u64,
    pub l0_count_after: usize,
}

pub struct FlushPipeline {
    sst_writer: SSTableWriter,
    config: FlushConfig,
    namespace: String,
}

impl FlushPipeline {
    pub fn new(config: FlushConfig, namespace: String, _base_path: String) -> Self {
        Self {
            sst_writer: SSTableWriter::new(config.sst_config.clone()),
            config,
            namespace,
        }
    }

    pub async fn flush<B: StorageBackend>(
        &self,
        frozen: Memtable,
        manifest_manager: &mut ManifestManager<B>,
        backend: &B,
        generation_id: u64,
    ) -> FlushResult<FlushResult_> {
        if frozen.is_empty() {
            return Err(FlushError::InvalidArgument {
                message: "cannot flush empty memtable".into(),
            });
        }

        // Step 1: Collect entries and compute sequence range
        let skiplist = frozen.into_skiplist();

        let mut min_seq = u64::MAX;
        let mut max_seq: u64 = 0;
        let mut record_id_count: u64 = 0;
        let mut entries: Vec<MemtableEntry> = Vec::new();

        let mut seen_records = std::collections::HashSet::new();
        for node in skiplist.iter() {
            if node.sequence_number < min_seq {
                min_seq = node.sequence_number;
            }
            if node.sequence_number > max_seq {
                max_seq = node.sequence_number;
            }
            if seen_records.insert(node.key.record_id().to_vec()) {
                record_id_count += 1;
            }
            entries.push(MemtableEntry::with_sequence(
                node.key.clone(),
                node.value.clone(),
                node.metadata.clone(),
                node.idempotency_key,
                node.sequence_number,
                node.entry_type,
            ));
        }

        // Step 2: Write SSTable
        let path = generate_sst_path(&self.namespace, 0);
        let sst_info = self
            .sst_writer
            .write(backend, &path, entries.into_iter())
            .await?;

        // Step 3: Build SSTableMeta
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let sst_meta = SSTableMeta::from_sst_info(
            &sst_info,
            (min_seq, max_seq),
            record_id_count,
            now_ms,
        );

        // Step 4: Update manifest via CAS
        let update = ManifestUpdate {
            trigger: ManifestUpdateTrigger::Flush,
            add_sstables: vec![(Level::L0, sst_meta.clone())],
            remove_sstables: vec![],
            new_last_flushed_sequence: Some(max_seq),
            writer_epoch: manifest_manager.writer_epoch,
            compactor_epoch: manifest_manager.compactor_epoch,
        };

        let manifest = manifest_manager.update(update).await?;
        let l0_count_after = manifest.l0_count();

        Ok(FlushResult_ {
            sst_info,
            sst_meta,
            flushed_sequence_range: (min_seq, max_seq),
            generation_id,
            l0_count_after,
        })
    }

    pub fn should_freeze(&self, memtable: &Memtable, max_age: Duration) -> bool {
        memtable.should_freeze_by_size() || memtable.should_freeze_by_age(max_age)
    }

    pub fn check_backpressure(&self, frozen_count: usize) -> FlushResult<()> {
        if frozen_count >= self.config.max_frozen_count {
            return Err(FlushError::ResourceExhausted {
                resource: "memtable".into(),
                message: format!(
                    "too many frozen memtables: {}/{}",
                    frozen_count, self.config.max_frozen_count
                ),
            });
        }
        Ok(())
    }
}
