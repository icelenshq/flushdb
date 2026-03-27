use std::path::Path;

use flushdb_types::{FlushError, FlushResult, StorageBackend};
use flushdb_wal::WalManager;

use crate::block_fetcher::BlockFetcher;
use crate::manifest::manager::ManifestManager;
use crate::manifest::types::{Level, ManifestConfig};
use crate::memtable::MemtableConfig;
use crate::memtable_list::MemtableList;
use crate::sstable_handle::LevelState;

#[derive(Debug, Clone, Default)]
pub struct RecoveryConfig {
    pub manifest_config: ManifestConfig,
    pub memtable_config: MemtableConfig,
}

pub struct RecoveryResult<B: StorageBackend> {
    pub manifest_manager: ManifestManager<B>,
    pub levels: Vec<LevelState>,
    pub memtable_list: MemtableList,
    pub next_sequence_number: u64,
    pub wal_entries_replayed: usize,
}

pub async fn recover<B: StorageBackend>(
    backend: B,
    wal_dir: &Path,
    namespace: &str,
    config: &RecoveryConfig,
    fetcher: &dyn BlockFetcher,
) -> FlushResult<RecoveryResult<B>> {
    // Step 1: Load manifest
    let mut manifest_manager = ManifestManager::new(
        backend,
        namespace.to_string(),
        config.manifest_config.clone(),
    );
    manifest_manager.load_latest().await?;
    let manifest = manifest_manager.current().clone();

    // Step 2: Rebuild in-memory SSTable state
    let mut levels = Vec::new();
    for level in Level::all() {
        let metas = manifest.sstables_at_level(*level);
        if metas.is_empty() {
            levels.push(LevelState::empty(*level));
        } else {
            let level_state =
                LevelState::open_all(*level, metas, namespace, &config.manifest_config, fetcher)
                    .await?;
            levels.push(level_state);
        }
    }

    // Step 3: Replay WAL
    let mut wal_entries_replayed = 0;
    let mut max_wal_sequence = manifest.last_flushed_sequence;

    let starting_seq = manifest.last_flushed_sequence + 1;
    let mut memtable_list = MemtableList::new(config.memtable_config.clone(), starting_seq);

    if wal_dir.exists() {
        let wal_entries = WalManager::recover_from(wal_dir, manifest.last_flushed_sequence)?;

        for wal_entry in wal_entries {
            let seq = wal_entry.sequence_number;
            if seq > max_wal_sequence {
                max_wal_sequence = seq;
            }

            let memtable_entry = wal_entry.to_memtable_entry()?;
            match memtable_list.insert(memtable_entry) {
                Ok(_) => {
                    wal_entries_replayed += 1;
                }
                Err(FlushError::DuplicateToken { token }) => {
                    tracing::warn!(
                        sequence_number = seq,
                        token = %token,
                        "skipping duplicate token during WAL replay"
                    );
                }
                Err(e) => return Err(e),
            }
        }
    }

    // Step 4: Compute next sequence number
    let next_sequence_number = max_wal_sequence + 1;

    Ok(RecoveryResult {
        manifest_manager,
        levels,
        memtable_list,
        next_sequence_number,
        wal_entries_replayed,
    })
}
