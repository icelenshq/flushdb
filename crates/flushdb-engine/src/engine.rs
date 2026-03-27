use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use flushdb_types::{
    CompositeKey, EntryType, FlushError, FlushResult, IdempotencyToken, MemtableEntry,
    StorageBackend,
};
use flushdb_wal::{DurabilityNotification, WalConfig, WalEntry, WalManager};

use crate::block_fetcher::{BlockFetcher, DirectBlockFetcher};
use crate::cache::{
    self, BlockCache, CacheConfig, CacheStats, CachingBlockFetcher, CoalescingFetcher,
    ContinuityTracker, NamespaceSizeEstimator, PinnedMetadataCache, ReadBudget,
};
use crate::compaction::executor::{CompactionExecutor, CompactionResult};
use crate::compaction::scheduler::{
    CompactionConfig, CompactionScheduler, CompactionTask, WriteStallStatus,
};
use crate::flush::{FlushConfig, FlushPipeline, FlushResult_};
use crate::manifest::manager::ManifestManager;
use crate::manifest::types::{Level, Manifest, ManifestConfig, ManifestId};
use crate::memtable::MemtableConfig;
use crate::memtable_list::MemtableList;
use crate::merge_iterator::MergeEntry;
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

#[derive(Clone, Debug)]
pub struct PutBatchItem {
    pub item_key: Bytes,
    pub value: Bytes,
    pub metadata: Bytes,
    pub idempotency_token: IdempotencyToken,
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
        let direct_fetcher: Arc<dyn BlockFetcher> =
            Arc::new(DirectBlockFetcher::new(backend.clone()));
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
        eprintln!(
            "[DEBUG] Engine::open namespace={} l0_count={} total_sst={} manifest_id={}",
            config.namespace,
            manifest_manager.current().l0_count(),
            manifest_manager.current().total_sstable_count(),
            manifest_manager.current().manifest_id,
        );
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
            self.memtable_list
                .freeze_active_with_generation(self.generation_counter)?;
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

        // Check dedup BEFORE WAL append to prevent duplicate WAL entries
        self.memtable_list.check_dedup(&token)?;

        let seq = self.next_sequence;

        let entry = MemtableEntry::with_sequence(
            key,
            value.clone(),
            metadata.clone(),
            token,
            seq,
            EntryType::Put,
        );

        let wal_entry = WalEntry::from_memtable_entry(&entry, self.config.namespace.as_bytes());
        let wal_notification = self
            .wal_manager
            .append(wal_entry, self.generation_counter)
            .await?;
        Self::await_wal_durability(wal_notification).await?;

        self.next_sequence += 1;
        self.memtable_list.insert(entry)?;
        self.continuity_tracker.invalidate_for_record(record_id);

        self.maybe_freeze_and_flush().await?;

        Ok(seq)
    }

    pub async fn put_batch(
        &mut self,
        record_id: &[u8],
        items: Vec<PutBatchItem>,
    ) -> FlushResult<usize> {
        if items.is_empty() {
            return Ok(0);
        }

        self.check_write_stall().await?;

        let mut seen_tokens = HashSet::with_capacity(items.len());
        let mut entries = Vec::with_capacity(items.len());
        let mut wal_entries = Vec::with_capacity(items.len());
        let mut next_seq = self.next_sequence;
        let namespace = Bytes::copy_from_slice(self.config.namespace.as_bytes());
        let record_id_bytes = Bytes::copy_from_slice(record_id);

        for item in items {
            let PutBatchItem {
                item_key,
                value,
                metadata,
                idempotency_token,
            } = item;

            if !idempotency_token.is_none() {
                if !seen_tokens.insert(idempotency_token) {
                    continue;
                }
                match self.memtable_list.check_dedup(&idempotency_token) {
                    Ok(()) => {}
                    Err(FlushError::DuplicateToken { .. }) => continue,
                    Err(e) => return Err(e),
                }
            }

            let entry = MemtableEntry::with_sequence(
                CompositeKey::new(record_id, item_key.as_ref())?,
                value.clone(),
                metadata.clone(),
                idempotency_token,
                next_seq,
                EntryType::Put,
            );
            next_seq += 1;
            wal_entries.push(WalEntry {
                sequence_number: entry.sequence_number,
                entry_type: EntryType::Put,
                namespace: namespace.clone(),
                record_id: record_id_bytes.clone(),
                item_key,
                item_value: value,
                item_metadata: metadata,
                idempotency_token,
            });
            entries.push(entry);
        }

        if entries.is_empty() {
            return Ok(0);
        }

        let inserted_count = entries.len();

        let wal_notification = self
            .wal_manager
            .append_batch(wal_entries, self.generation_counter)
            .await?;
        Self::await_wal_durability(wal_notification).await?;

        self.next_sequence = next_seq;
        for entry in entries {
            self.memtable_list.insert_prechecked(entry)?;
            self.enforce_batch_memtable_limits().await?;
        }
        self.continuity_tracker.invalidate_for_record(record_id);

        Ok(inserted_count)
    }

    pub async fn delete(&mut self, record_id: &[u8], item_key: &[u8]) -> FlushResult<u64> {
        self.check_write_stall().await?;

        let key = CompositeKey::new(record_id, item_key)?;
        let seq = self.next_sequence;

        let entry = MemtableEntry::with_sequence(
            key,
            Bytes::new(),
            Bytes::new(),
            IdempotencyToken::none(),
            seq,
            EntryType::Delete,
        );

        let wal_entry = WalEntry::from_memtable_entry(&entry, self.config.namespace.as_bytes());
        let wal_notification = self
            .wal_manager
            .append(wal_entry, self.generation_counter)
            .await?;
        Self::await_wal_durability(wal_notification).await?;
        self.next_sequence += 1;
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

        let entry = MemtableEntry::with_sequence(
            key,
            Bytes::copy_from_slice(end_key),
            Bytes::new(),
            IdempotencyToken::none(),
            seq,
            EntryType::RangeDelete,
        );

        let wal_entry = WalEntry::from_memtable_entry(&entry, self.config.namespace.as_bytes());
        let wal_notification = self
            .wal_manager
            .append(wal_entry, self.generation_counter)
            .await?;
        Self::await_wal_durability(wal_notification).await?;
        self.next_sequence += 1;
        self.memtable_list.insert(entry)?;
        self.continuity_tracker.invalidate_for_record(record_id);

        self.maybe_freeze_and_flush().await?;

        Ok(seq)
    }

    // --- Read Methods ---

    pub async fn get(&self, record_id: &[u8], item_key: &[u8]) -> FlushResult<Option<GetResult>> {
        let key = CompositeKey::new(record_id, item_key)?;

        if self
            .continuity_tracker
            .is_known_absent(record_id, item_key, self.manifest_version())
        {
            return Ok(None);
        }

        let mut budget = ReadBudget::new(self.config.cache_config.get_budget_per_read);

        if let Some(entry) = self.memtable_list.get(&key) {
            let merge_entry = MergeEntry::from_memtable_entry(&entry);
            if merge_entry.is_tombstone() {
                return Ok(None);
            }
            if self.memtable_list.range_tombstone_covers(
                key.record_id(),
                key.item_key(),
                entry.sequence_number,
            ) {
                return Ok(None);
            }
            return Ok(Some(GetResult::from_merge_entry(&merge_entry)));
        }

        let mut best: Option<MergeEntry> = None;
        for level_state in &self.levels {
            let candidates = level_state.find_candidates_for_key(&key);
            for handle in candidates {
                match handle
                    .get_budgeted(&key, &self.caching_fetcher, &mut budget)
                    .await
                {
                    Ok(Some(block_entry)) => {
                        let merge_entry = MergeEntry::from_block_entry(block_entry);
                        match &best {
                            Some(b) if b.sequence_number >= merge_entry.sequence_number => {}
                            _ => best = Some(merge_entry),
                        }
                    }
                    Ok(None) => {}
                    Err(FlushError::ResourceExhausted { .. }) => break,
                    Err(e) => return Err(e),
                }
            }
            if budget.is_exhausted() {
                break;
            }
        }

        match best {
            Some(entry) if entry.is_tombstone() => Ok(None),
            Some(entry) => {
                if self.memtable_list.range_tombstone_covers(
                    key.record_id(),
                    key.item_key(),
                    entry.sequence_number,
                ) {
                    return Ok(None);
                }
                Ok(Some(GetResult::from_merge_entry(&entry)))
            }
            None => Ok(None),
        }
    }

    pub async fn scan(
        &self,
        record_id: &[u8],
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        options: RangeReadOptions,
    ) -> FlushResult<RangeReadResult> {
        let read_path = ReadPath::new(&self.caching_fetcher);
        // Budget integration for scan requires threading ReadBudget through ReadPath,
        // and NamespaceSizeEstimator / ContinuityTracker marking require &mut self
        // while scan takes &self. These will be wired when the server layer wraps
        // Engine with single-owner access patterns (no Arc<Mutex>).
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

    // --- Maintenance ---

    pub fn run_cache_maintenance(&self) {
        self.block_cache.run_pending_tasks();
    }

    pub async fn run_maintenance(&mut self) -> FlushResult<()> {
        if self.memtable_list.active_entry_count() > 0
            && self
                .memtable_list
                .active()
                .should_freeze_by_age(self.config.flush_config.flush_trigger_age)
        {
            self.memtable_list
                .freeze_active_with_generation(self.generation_counter)?;
            self.generation_counter += 1;
        }

        while self.memtable_list.has_frozen() {
            self.flush_frozen().await?;
        }

        self.run_cache_maintenance();

        Ok(())
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
        let (generation_id, frozen) = self
            .memtable_list
            .pop_oldest_frozen_with_generation()
            .ok_or_else(|| FlushError::InvalidArgument {
                message: "no frozen memtables to flush".into(),
            })?;

        let backend = self.manifest_manager.backend().clone();

        let result = self
            .flush_pipeline
            .flush(frozen, &mut self.manifest_manager, &backend, generation_id)
            .await?;

        // WAL cleanup
        let deletable = self.wal_manager.mark_generation_flushed(generation_id)?;
        self.wal_manager.cleanup_segments(&deletable)?;

        let meta = result.sst_meta.clone();
        let path = meta.sst_path(&self.config.namespace, Level::L0);
        let handle = SSTableHandle::open_with_cache(
            meta,
            path,
            &self.caching_fetcher,
            &mut self.pinned_metadata,
        )
        .await?;

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
            .execute(
                &task,
                &mut self.manifest_manager,
                &self.caching_fetcher,
                &backend,
            )
            .await?;

        self.pending_deletions
            .extend(result.removed_sstable_ids.clone());

        self.rebuild_levels().await?;

        cache::evict_compaction_result(&self.block_cache, &mut self.pinned_metadata, &result);
        let new_manifest_id = self.manifest_version();
        self.continuity_tracker
            .invalidate_before_manifest(new_manifest_id);

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
                        meta.clone(),
                        path,
                        &self.caching_fetcher,
                        &mut self.pinned_metadata,
                    )
                    .await?;
                    handles.push(handle);
                }
                if !level.is_overlapping() {
                    handles.sort_by(|a, b| a.meta.min_key.cmp(&b.meta.min_key));
                }
                self.levels.push(LevelState {
                    level: *level,
                    handles,
                });
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
        if status.is_stopped() {
            eprintln!(
                "[DEBUG] check_write_stall STOPPED namespace={} l0_count={} manifest_l0_count={} manifest_id={}",
                self.config.namespace,
                status.l0_count(),
                self.manifest_manager.current().l0_count(),
                self.manifest_manager.current().manifest_id,
            );
        }
        match status {
            WriteStallStatus::Normal => {}
            WriteStallStatus::Slowdown { delay_ms, .. } => {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            WriteStallStatus::Stopped { l0_count } => {
                return Err(FlushError::ResourceExhausted {
                    resource: "L0 SSTables".into(),
                    message: format!(
                        "write stalled: L0 count {} >= stop trigger, compact before writing",
                        l0_count
                    ),
                });
            }
        }

        if self.memtable_list.is_memory_backpressured() {
            return Err(FlushError::ResourceExhausted {
                resource: "memtable_memory".into(),
                message: format!(
                    "memtable memory {} exceeds limit {}",
                    self.memtable_list.total_memory_usage(),
                    self.config.memtable_config.memtable_memory_limit,
                ),
            });
        }

        Ok(())
    }

    async fn maybe_freeze_and_flush(&mut self) -> FlushResult<()> {
        if self.memtable_list.active().should_freeze_by_size()
            || self
                .memtable_list
                .active()
                .should_freeze_by_age(self.config.flush_config.flush_trigger_age)
        {
            self.memtable_list
                .freeze_active_with_generation(self.generation_counter)?;
            self.generation_counter += 1;
            self.flush_frozen().await?;
        }
        Ok(())
    }

    async fn enforce_batch_memtable_limits(&mut self) -> FlushResult<()> {
        let should_freeze_active = self.memtable_list.active_entry_count() > 0
            && (self.memtable_list.active().should_freeze_by_size()
                || self
                    .memtable_list
                    .active()
                    .should_freeze_by_age(self.config.flush_config.flush_trigger_age)
                || self.memtable_list.is_memory_backpressured());

        if should_freeze_active {
            self.memtable_list
                .freeze_active_with_generation(self.generation_counter)?;
            self.generation_counter += 1;
        }

        let mut should_flush = should_freeze_active;
        while self.memtable_list.has_frozen()
            && (should_flush || self.memtable_list.is_memory_backpressured())
        {
            self.flush_frozen().await?;
            should_flush = false;
        }

        Ok(())
    }

    async fn await_wal_durability(notification: DurabilityNotification) -> FlushResult<()> {
        notification.await.map_err(|_| {
            FlushError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "WAL durability notification channel closed",
            ))
        })??;
        Ok(())
    }
}
