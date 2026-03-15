use std::collections::HashSet;
use std::time::{SystemTime, UNIX_EPOCH};

use flushdb_types::{FlushResult, MemtableEntry, StorageBackend};

use crate::block_fetcher::BlockFetcher;
use crate::manifest::manager::ManifestManager;
use crate::manifest::types::{
    Level, ManifestUpdate, ManifestUpdateTrigger, SSTableMeta,
};
use crate::merge_iterator::{MergeEntry, MergeIterator, VecSource};
use crate::sstable::block_reader::BlockEntry;
use crate::sstable::writer::SSTableWriter;
use crate::sstable::types::SstConfig;
use crate::sstable_handle::SSTableHandle;
use crate::compaction::scheduler::{CompactionConfig, CompactionTask, CompactionType};

#[derive(Debug)]
pub struct CompactionResult {
    pub output_sstables: Vec<SSTableMeta>,
    pub removed_sstable_ids: Vec<String>,
    pub trivial_moves: usize,
    pub entries_written: u64,
    pub entries_dropped: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

pub struct CompactionExecutor {
    sst_config: SstConfig,
    compaction_config: CompactionConfig,
    namespace: String,
}

impl CompactionExecutor {
    pub fn new(
        sst_config: SstConfig,
        compaction_config: CompactionConfig,
        namespace: String,
        _base_path: String,
    ) -> Self {
        Self {
            sst_config,
            compaction_config,
            namespace,
        }
    }

    pub async fn execute<B: StorageBackend>(
        &self,
        task: &CompactionTask,
        manifest_manager: &mut ManifestManager<B>,
        fetcher: &dyn BlockFetcher,
        backend: &B,
    ) -> FlushResult<CompactionResult> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Check for trivial moves (only for LevelToLevel with empty target)
        let mut trivial_moves = 0;
        let mut trivial_metas: Vec<(Level, SSTableMeta)> = Vec::new();
        let mut non_trivial_inputs: Vec<SSTableMeta> = Vec::new();

        if task.task_type == CompactionType::LevelToLevel && task.target_sstables.is_empty() {
            // All inputs can be trivially moved
            for input in &task.input_sstables {
                let mut moved = input.clone();
                moved.created_at_ms = now_ms;
                trivial_metas.push((task.target_level, moved));
                trivial_moves += 1;
            }
        } else {
            non_trivial_inputs = task.input_sstables.clone();
        }

        if !trivial_metas.is_empty() && non_trivial_inputs.is_empty() {
            // All trivial — just update manifest
            let mut remove = Vec::new();
            for input in &task.input_sstables {
                remove.push((task.source_level, input.id.clone()));
            }

            let update = ManifestUpdate {
                trigger: ManifestUpdateTrigger::Compaction,
                add_sstables: trivial_metas.clone(),
                remove_sstables: remove,
                new_last_flushed_sequence: None,
                writer_epoch: manifest_manager.writer_epoch,
                compactor_epoch: manifest_manager.compactor_epoch,
            };
            manifest_manager.update(update).await?;

            let removed_ids: Vec<String> = task.input_sstables.iter().map(|m| m.id.clone()).collect();

            return Ok(CompactionResult {
                output_sstables: trivial_metas.into_iter().map(|(_, m)| m).collect(),
                removed_sstable_ids: removed_ids,
                trivial_moves,
                entries_written: 0,
                entries_dropped: 0,
                bytes_read: 0,
                bytes_written: 0,
            });
        }

        // Open all input SSTables
        let all_inputs: Vec<&SSTableMeta> = non_trivial_inputs
            .iter()
            .chain(task.target_sstables.iter())
            .collect();

        let mut handles = Vec::new();
        let mut bytes_read: u64 = 0;
        for meta in &all_inputs {
            let path = meta.sst_path(&self.namespace, if non_trivial_inputs.iter().any(|m| m.id == meta.id) { task.source_level } else { task.target_level });
            let handle = SSTableHandle::open((*meta).clone(), path, fetcher).await?;
            bytes_read += meta.size_bytes;
            handles.push(handle);
        }

        // Build merge sources — source-level inputs are newer (lower source_id)
        let mut sources: Vec<Box<dyn crate::merge_iterator::MergeSource>> = Vec::new();
        let mut source_id = 0;

        // Source level inputs first (newer)
        for (i, handle) in handles.iter().enumerate() {
            if i < non_trivial_inputs.len() {
                let entries = self.collect_all_entries(handle, fetcher).await?;
                let merge_entries: Vec<MergeEntry> = entries
                    .into_iter()
                    .map(MergeEntry::from_block_entry)
                    .collect();
                sources.push(Box::new(VecSource::new(merge_entries, source_id)));
                source_id += 1;
            }
        }

        // Target level inputs next (older)
        for (i, handle) in handles.iter().enumerate() {
            if i >= non_trivial_inputs.len() {
                let entries = self.collect_all_entries(handle, fetcher).await?;
                let merge_entries: Vec<MergeEntry> = entries
                    .into_iter()
                    .map(MergeEntry::from_block_entry)
                    .collect();
                sources.push(Box::new(VecSource::new(merge_entries, source_id)));
                source_id += 1;
            }
        }

        // Merge and write output fragments
        let mut iter = MergeIterator::new(sources);
        let writer = SSTableWriter::new(self.sst_config.clone());
        let run_id = ulid::Ulid::new().to_string();
        let mut fragment_index: u32 = 0;
        let mut output_metas: Vec<SSTableMeta> = Vec::new();
        let mut current_entries: Vec<MemtableEntry> = Vec::new();
        let mut current_size: u64 = 0;
        let mut entries_written: u64 = 0;
        let mut entries_dropped: u64 = 0;
        let mut bytes_written: u64 = 0;
        let mut min_seq = u64::MAX;
        let mut max_seq: u64 = 0;
        let mut record_ids: HashSet<Vec<u8>> = HashSet::new();

        let is_bottom = task.target_level.is_bottom();
        let tombstone_ttl_ms = self.compaction_config.tombstone_ttl.as_millis() as u64;

        while let Some(entry) = iter.next_deduped() {
            // Tombstone filtering at bottom level
            if is_bottom && entry.is_tombstone() {
                // Check if tombstone is expired
                // For simplicity, we drop tombstones at bottom level during compaction
                // In production, we'd check the tombstone's age against tombstone_ttl
                entries_dropped += 1;
                let _ = tombstone_ttl_ms; // will use in future refinement
                continue;
            }

            let entry_size = entry.composite_key.as_bytes().len()
                + entry.value.len()
                + entry.metadata.len()
                + 20; // overhead

            if entry.sequence_number < min_seq {
                min_seq = entry.sequence_number;
            }
            if entry.sequence_number > max_seq {
                max_seq = entry.sequence_number;
            }
            record_ids.insert(entry.composite_key.record_id().to_vec());

            let memtable_entry = MemtableEntry::with_sequence(
                entry.composite_key,
                entry.value,
                entry.metadata,
                flushdb_types::IdempotencyToken::none(),
                entry.sequence_number,
                entry.entry_type,
            );

            current_entries.push(memtable_entry);
            current_size += entry_size as u64;
            entries_written += 1;

            // Fragment boundary
            if current_size >= self.compaction_config.target_fragment_size {
                let path = crate::sstable::writer::generate_run_fragment_path(
                    &self.namespace,
                    task.target_level.as_u8() as u32,
                    &run_id,
                    fragment_index,
                );
                let sst_info = writer.write(backend, &path, current_entries.drain(..)).await?;

                let mut meta = SSTableMeta::from_sst_info(
                    &sst_info,
                    (min_seq, max_seq),
                    record_ids.len() as u64,
                    now_ms,
                );
                meta.run_id = Some(run_id.clone());
                meta.fragment_index = Some(fragment_index);

                bytes_written += sst_info.file_size;
                output_metas.push(meta);
                fragment_index += 1;
                current_size = 0;
                min_seq = u64::MAX;
                max_seq = 0;
                record_ids.clear();
            }
        }

        // Write remaining entries
        if !current_entries.is_empty() {
            let path = if fragment_index == 0 {
                // Single output — use simple path
                crate::sstable::writer::generate_sst_path(
                    &self.namespace,
                    task.target_level.as_u8() as u32,
                )
            } else {
                crate::sstable::writer::generate_run_fragment_path(
                    &self.namespace,
                    task.target_level.as_u8() as u32,
                    &run_id,
                    fragment_index,
                )
            };
            let sst_info = writer.write(backend, &path, current_entries.drain(..)).await?;

            let mut meta = SSTableMeta::from_sst_info(
                &sst_info,
                (min_seq, max_seq),
                record_ids.len() as u64,
                now_ms,
            );
            if fragment_index > 0 {
                meta.run_id = Some(run_id.clone());
                meta.fragment_index = Some(fragment_index);
            }

            bytes_written += sst_info.file_size;
            output_metas.push(meta);
        }

        // Build manifest update
        let mut remove_sstables = Vec::new();
        for input in &task.input_sstables {
            remove_sstables.push((task.source_level, input.id.clone()));
        }
        for target in &task.target_sstables {
            remove_sstables.push((task.target_level, target.id.clone()));
        }

        let add_sstables: Vec<(Level, SSTableMeta)> = output_metas
            .iter()
            .map(|m| (task.target_level, m.clone()))
            .chain(trivial_metas.clone())
            .collect();

        let update = ManifestUpdate {
            trigger: ManifestUpdateTrigger::Compaction,
            add_sstables,
            remove_sstables: remove_sstables.clone(),
            new_last_flushed_sequence: None,
            writer_epoch: manifest_manager.writer_epoch,
            compactor_epoch: manifest_manager.compactor_epoch,
        };

        manifest_manager.update(update).await?;

        let mut removed_ids: Vec<String> = task.input_sstables.iter().map(|m| m.id.clone()).collect();
        removed_ids.extend(task.target_sstables.iter().map(|m| m.id.clone()));

        Ok(CompactionResult {
            output_sstables: output_metas,
            removed_sstable_ids: removed_ids,
            trivial_moves,
            entries_written,
            entries_dropped,
            bytes_read,
            bytes_written,
        })
    }

    async fn collect_all_entries(
        &self,
        handle: &SSTableHandle,
        fetcher: &dyn BlockFetcher,
    ) -> FlushResult<Vec<BlockEntry>> {
        let mut all_entries = Vec::new();
        for block_idx in 0..handle.block_count() {
            let entries = handle.get_block(block_idx, fetcher).await?;
            all_entries.extend(entries);
        }
        Ok(all_entries)
    }
}
