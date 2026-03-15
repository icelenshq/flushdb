use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use flushdb_types::{
    CompositeKey, EntryType, FlushError, FlushResult, IdempotencyToken, MemtableEntry,
    StorageBackend,
};
use flushdb_wal::{WalConfig, WalEntry, WalManager};

use crate::block_fetcher::{BlockFetcher, DirectBlockFetcher};
use crate::cache::{
    self, BlockCache, CacheConfig, CacheStats, CachingBlockFetcher, CoalescingFetcher,
    ContinuityTracker, NamespaceSizeEstimator, PinnedMetadataCache,
};
use crate::compaction::executor::{CompactionExecutor, CompactionResult};
use crate::compaction::scheduler::{CompactionConfig, CompactionScheduler, CompactionTask, WriteStallStatus};
use crate::flush::{FlushConfig, FlushPipeline, FlushResult_};
use crate::manifest::manager::ManifestManager;
use crate::manifest::types::{Level, Manifest, ManifestConfig, ManifestId};
use crate::memtable::MemtableConfig;
use crate::memtable_list::MemtableList;
use crate::read_path::{GetResult, RangeReadOptions, RangeReadResult, ReadPath};
use crate::recovery::{self, RecoveryConfig};
use crate::sstable_handle::{LevelState, SSTableHandle};

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub memtable_config: MemtableConfig,
    pub wal_config: WalConfig,
    pub flush_config: FlushConfig,
    pub compaction_config: CompactionConfig,
    pub manifest_config: ManifestConfig,
    pub cache_config: CacheConfig,
    pub namespace: String,
    pub local_dir: PathBuf,
}

pub struct Engine<B: StorageBackend> {
    memtable_list: MemtableList,
    wal_manager: WalManager,
    manifest_manager: ManifestManager<B>,
    flush_pipeline: FlushPipeline,
    compaction_scheduler: CompactionScheduler,
    compaction_executor: CompactionExecutor,
    levels: Vec<LevelState>,
    caching_fetcher: CachingBlockFetcher,
    coalescing_fetcher: CoalescingFetcher,
    block_cache: BlockCache,
    pinned_metadata: PinnedMetadataCache,
    continuity_tracker: ContinuityTracker,
    size_estimator: NamespaceSizeEstimator,
    config: EngineConfig,
    next_sequence: u64,
    generation_counter: u64,
    pending_deletions: Vec<String>,
}

impl<B: StorageBackend + Clone + 'static> Engine<B> {
    pub async fn open(backend: B, config: EngineConfig) -> FlushResult<Self> {
        let wal_dir = config.local_dir.join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        let block_cache = BlockCache::new(&config.cache_config);
        let direct_fetcher: Arc<dyn BlockFetcher> = Arc::new(DirectBlockFetcher::new(backend.clone()));
        let caching_fetcher = CachingBlockFetcher::new(direct_fetcher, block_cache.clone());
        let coalescing_fetcher = CoalescingFetcher::new(caching_fetcher.clone());
        let pinned_metadata = PinnedMetadataCache::new(&config.cache_config);
        let continuity_tracker = ContinuityTracker::new(&config.cache_config);
        let size_estimator = NamespaceSizeEstimator::new();

        let recovery_config = RecoveryConfig {
            manifest_config: config.manifest_config.clone(),
            memtable_config: config.memtable_config.clone(),
        };

        let recovery_result = recovery::recover(
            backend.clone(),
            &wal_dir,
            &config.namespace,
            &recovery_config,
            &caching_fetcher,
        )
        .await?;

        let mut manifest_manager = recovery_result.manifest_manager;
        manifest_manager.acquire_writer_epoch().await?;

        let wal_manager = WalManager::open(&wal_dir, config.wal_config.clone())?;

        let flush_pipeline = FlushPipeline::new(
            config.flush_config.clone(),
            config.namespace.clone(),
            config.manifest_config.base_path.clone(),
        );

        let compaction_scheduler = CompactionScheduler::new(config.compaction_config.clone());

        let compaction_executor = CompactionExecutor::new(
            config.flush_config.sst_config.clone(),
            config.compaction_config.clone(),
            config.namespace.clone(),
            config.manifest_config.base_path.clone(),
        );

        Ok(Self {
            memtable_list: recovery_result.memtable_list,
            wal_manager,
            manifest_manager,
            flush_pipeline,
            compaction_scheduler,
            compaction_executor,
            levels: recovery_result.levels,
            caching_fetcher,
            coalescing_fetcher,
            block_cache,
            pinned_metadata,
            continuity_tracker,
            size_estimator,
            config,
            next_sequence: recovery_result.next_sequence_number,
            generation_counter: 0,
            pending_deletions: Vec::new(),
        })
    }

    pub async fn close(&mut self) -> FlushResult<()> {
        // Flush active memtable if non-empty
        if self.memtable_list.active_entry_count() > 0 {
            self.memtable_list.freeze_active()?;
            self.generation_counter += 1;
            self.flush_frozen().await?;
        }

        // Flush remaining frozen memtables
        while self.memtable_list.has_frozen() {
            self.flush_frozen().await?;
        }

        Ok(())
    }

    // --- Write Methods ---

    pub async fn put(
        &mut self,
        record_id: &[u8],
        item_key: &[u8],
        value: Bytes,
        metadata: Bytes,
        idempotency_token: Option<IdempotencyToken>,
    ) -> FlushResult<u64> {
        self.check_write_stall().await?;

        let key = CompositeKey::new(record_id, item_key)?;
        let token = idempotency_token.unwrap_or_else(IdempotencyToken::none);

        let seq = self.next_sequence;
        self.next_sequence += 1;

        let entry = MemtableEntry::with_sequence(
            key,
            value.clone(),
            metadata.clone(),
            token,
            seq,
            EntryType::Put,
        );

        // Write to WAL
        let wal_entry = WalEntry::from_memtable_entry(&entry, self.config.namespace.as_bytes());
        self.wal_manager.append(wal_entry, self.generation_counter)?;

        self.memtable_list.insert(entry)?;
        self.continuity_tracker.invalidate_for_record(record_id);

        self.maybe_freeze_and_flush().await?;

        Ok(seq)
    }

    pub async fn delete(
        &mut self,
        record_id: &[u8],
        item_key: &[u8],
    ) -> FlushResult<u64> {
        self.check_write_stall().await?;

        let key = CompositeKey::new(record_id, item_key)?;
        let seq = self.next_sequence;
        self.next_sequence += 1;

        let entry = MemtableEntry::with_sequence(
            key,
            Bytes::new(),
            Bytes::new(),
            IdempotencyToken::none(),
            seq,
            EntryType::Delete,
        );

        let wal_entry = WalEntry::from_memtable_entry(&entry, self.config.namespace.as_bytes());
        self.wal_manager.append(wal_entry, self.generation_counter)?;
        self.memtable_list.insert(entry)?;
        self.continuity_tracker.invalidate_for_record(record_id);

        self.maybe_freeze_and_flush().await?;

        Ok(seq)
    }

    pub async fn delete_range(
        &mut self,
        record_id: &[u8],
        start_key: &[u8],
        end_key: &[u8],
    ) -> FlushResult<u64> {
        self.check_write_stall().await?;

        let key = CompositeKey::range_tombstone_key(record_id, start_key)?;
        let seq = self.next_sequence;
        self.next_sequence += 1;

        let entry = MemtableEntry::with_sequence(
            key,
            Bytes::copy_from_slice(end_key),
            Bytes::new(),
            IdempotencyToken::none(),
            seq,
            EntryType::RangeDelete,
        );

        let wal_entry = WalEntry::from_memtable_entry(&entry, self.config.namespace.as_bytes());
        self.wal_manager.append(wal_entry, self.generation_counter)?;
        self.memtable_list.insert(entry)?;
        self.continuity_tracker.invalidate_for_record(record_id);

        self.maybe_freeze_and_flush().await?;

        Ok(seq)
    }

    // --- Read Methods ---

    pub async fn get(
        &self,
        record_id: &[u8],
        item_key: &[u8],
    ) -> FlushResult<Option<GetResult>> {
        let key = CompositeKey::new(record_id, item_key)?;
        if self.continuity_tracker.is_known_absent(record_id, item_key, self.manifest_version()) {
            return Ok(None);
        }
        let read_path = ReadPath::new(&self.caching_fetcher);
        let result = read_path
            .point_read(&key, &self.memtable_list, &self.levels)
            .await?;
        Ok(result.map(|e| GetResult::from_merge_entry(&e)))
    }

    pub async fn scan(
        &self,
        record_id: &[u8],
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        options: RangeReadOptions,
    ) -> FlushResult<RangeReadResult> {
        let read_path = ReadPath::new(&self.caching_fetcher);
        read_path
            .range_read(
                record_id,
                start_key,
                end_key,
                options,
                &self.memtable_list,
                &self.levels,
            )
            .await
    }

    pub async fn multi_get(
        &self,
        record_id: &[u8],
        keys: &[&[u8]],
    ) -> FlushResult<Vec<Option<GetResult>>> {
        let read_path = ReadPath::new(&self.caching_fetcher);
        let results = read_path
            .multi_get(record_id, keys, &self.memtable_list, &self.levels)
            .await?;
        Ok(results
            .into_iter()
            .map(|opt| opt.map(|e| GetResult::from_merge_entry(&e)))
            .collect())
    }

    // --- Flush Orchestration ---

    pub async fn maybe_flush(&mut self) -> FlushResult<Option<FlushResult_>> {
        if self.memtable_list.has_frozen() {
            let result = self.flush_frozen().await?;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }

    async fn flush_frozen(&mut self) -> FlushResult<FlushResult_> {
        let frozen = self.memtable_list.pop_oldest_frozen().ok_or_else(|| {
            FlushError::InvalidArgument {
                message: "no frozen memtables to flush".into(),
            }
        })?;

        let generation_id = self.generation_counter;
        let backend = self.manifest_manager.backend().clone();

        let result = self
            .flush_pipeline
            .flush(frozen, &mut self.manifest_manager, &backend, generation_id)
            .await?;

        // WAL cleanup
        let deletable = self
            .wal_manager
            .mark_generation_flushed(generation_id)?;
        self.wal_manager.cleanup_segments(&deletable)?;

        let meta = result.sst_meta.clone();
        let path = meta.sst_path(&self.config.namespace, Level::L0);
        let handle = SSTableHandle::open_with_cache(
            meta, path, &self.caching_fetcher, &mut self.pinned_metadata,
        ).await?;

        // Add to L0 level state
        if self.levels.is_empty() {
            for level in Level::all() {
                self.levels.push(LevelState::empty(*level));
            }
        }
        self.levels[0].handles.push(handle);

        // Check compaction triggers
        self.maybe_compact().await?;

        Ok(result)
    }

    // --- Compaction Orchestration ---

    pub async fn maybe_compact(&mut self) -> FlushResult<Vec<CompactionResult>> {
        let tasks = self
            .compaction_scheduler
            .check_triggers(self.manifest_manager.current());
        let mut results = Vec::new();
        for task in tasks {
            let result = self.compact(task).await?;
            results.push(result);
        }
        Ok(results)
    }

    async fn compact(&mut self, task: CompactionTask) -> FlushResult<CompactionResult> {
        let backend = self.manifest_manager.backend().clone();
        let result = self
            .compaction_executor
            .execute(&task, &mut self.manifest_manager, &self.caching_fetcher, &backend)
            .await?;

        self.pending_deletions
            .extend(result.removed_sstable_ids.clone());

        self.rebuild_levels().await?;

        cache::evict_compaction_result(&self.block_cache, &mut self.pinned_metadata, &result);
        let new_manifest_id = self.manifest_version();
        self.continuity_tracker.invalidate_before_manifest(new_manifest_id);

        Ok(result)
    }

    async fn rebuild_levels(&mut self) -> FlushResult<()> {
        let manifest = self.manifest_manager.current().clone();
        self.levels.clear();

        for level in Level::all() {
            let metas = manifest.sstables_at_level(*level);
            if metas.is_empty() {
                self.levels.push(LevelState::empty(*level));
            } else {
                let mut handles = Vec::with_capacity(metas.len());
                for meta in metas {
                    let path = meta.sst_path(&self.config.namespace, *level);
                    let handle = SSTableHandle::open_with_cache(
                        meta.clone(), path, &self.caching_fetcher, &mut self.pinned_metadata,
                    ).await?;
                    handles.push(handle);
                }
                if !level.is_overlapping() {
                    handles.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));
                }
                self.levels.push(LevelState { level: *level, handles });
            }
        }

        Ok(())
    }

    // --- Status Methods ---

    pub fn write_stall_status(&self) -> WriteStallStatus {
        self.compaction_scheduler
            .write_stall_status(self.manifest_manager.current())
    }

    pub fn manifest(&self) -> &Manifest {
        self.manifest_manager.current()
    }

    pub fn l0_count(&self) -> usize {
        self.manifest_manager.current().l0_count()
    }

    pub fn level_sizes(&self) -> Vec<(Level, u64)> {
        Level::all()
            .iter()
            .map(|l| (*l, self.manifest_manager.current().level_size_bytes(*l)))
            .collect()
    }

    pub fn frozen_memtable_count(&self) -> usize {
        self.memtable_list.frozen_count()
    }

    pub fn manifest_version(&self) -> ManifestId {
        self.manifest_manager.current().manifest_id
    }

    pub fn consumed_sstable_ids(&self) -> &[String] {
        &self.pending_deletions
    }

    pub fn cache_stats(&self) -> CacheStats {
        self.block_cache.stats()
    }

    pub fn pinned_metadata_count(&self) -> usize {
        self.pinned_metadata.entry_count()
    }

    pub fn continuity_tracked_records(&self) -> usize {
        self.continuity_tracker.tracked_record_count()
    }

    pub fn coalescing_fetcher(&self) -> &CoalescingFetcher {
        &self.coalescing_fetcher
    }

    pub fn size_estimator(&self) -> &NamespaceSizeEstimator {
        &self.size_estimator
    }

    // --- Deferred Deletion ---

    pub fn schedule_deletion(&mut self, sst_ids: Vec<String>) {
        self.pending_deletions.extend(sst_ids);
    }

    pub async fn run_deletion(&mut self) -> FlushResult<usize> {
        let backend = self.manifest_manager.backend().clone();
        let mut deleted = 0;
        let to_delete: Vec<String> = self.pending_deletions.drain(..).collect();

        for path in to_delete {
            match backend.delete(&path).await {
                Ok(()) => deleted += 1,
                Err(FlushError::NotFound { .. }) => deleted += 1,
                Err(e) => {
                    self.pending_deletions.push(path);
                    return Err(e);
                }
            }
        }

        Ok(deleted)
    }

    // --- Internal Helpers ---

    async fn check_write_stall(&self) -> FlushResult<()> {
        let status = self.write_stall_status();
        match status {
            WriteStallStatus::Normal => Ok(()),
            WriteStallStatus::Slowdown { delay_ms, .. } => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                Ok(())
            }
            WriteStallStatus::Stopped { l0_count } => Err(FlushError::ResourceExhausted {
                resource: "L0 SSTables".into(),
                message: format!(
                    "write stalled: L0 count {} >= stop trigger, compact before writing",
                    l0_count
                ),
            }),
        }
    }

    async fn maybe_freeze_and_flush(&mut self) -> FlushResult<()> {
        if self.memtable_list.active().should_freeze_by_size()
            || self
                .memtable_list
                .active()
                .should_freeze_by_age(self.config.flush_config.flush_trigger_age)
        {
            self.memtable_list.freeze_active()?;
            self.generation_counter += 1;
            self.flush_frozen().await?;
        }
        Ok(())
    }
}
