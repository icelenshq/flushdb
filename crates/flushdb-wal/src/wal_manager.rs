use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use flushdb_types::{FlushError, FlushResult};

use crate::config::{parse_segment_number, segment_path, WalConfig};
use crate::dirty_tracker::DirtySegmentTracker;
use crate::entry::WalEntry;
use crate::group_commit::{DurabilityNotification, GroupCommitBuffer, GroupCommitHandle};
use crate::wal_reader::WalReader;
use crate::wal_writer::WalWriter;

#[derive(Debug, Clone)]
pub struct FlushTriggers {
    pub age_triggered_segments: Vec<u64>,
    pub size_pressure: bool,
    pub oldest_pinned_generation: Option<u64>,
    pub backpressure: bool,
}

pub struct WalManager {
    config: WalConfig,
    group_commit: GroupCommitBuffer,
    commit_handle: Option<GroupCommitHandle>,
    dirty_tracker: DirtySegmentTracker,
    partition_dir: PathBuf,
    current_segment_number: Arc<AtomicU64>,
}

impl WalManager {
    pub fn open(partition_dir: &Path, config: WalConfig) -> FlushResult<Self> {
        let writer = WalWriter::open(partition_dir, &config)?;
        let current_seg = Arc::new(AtomicU64::new(writer.current_segment_number()));

        let (group_commit, commit_handle) =
            GroupCommitBuffer::new(writer, config.clone(), Arc::clone(&current_seg));

        Ok(Self {
            config,
            group_commit,
            commit_handle: Some(commit_handle),
            dirty_tracker: DirtySegmentTracker::new(),
            partition_dir: partition_dir.to_path_buf(),
            current_segment_number: current_seg,
        })
    }

    pub fn append(
        &mut self,
        entry: WalEntry,
        generation_id: u64,
    ) -> FlushResult<DurabilityNotification> {
        let notification = self.group_commit.submit(entry)?;
        let seg_num = self.current_segment_number.load(Ordering::Relaxed);
        self.dirty_tracker.record_write(seg_num, generation_id, 0);
        Ok(notification)
    }

    pub fn append_if_not_full(
        &mut self,
        entry: WalEntry,
        generation_id: u64,
    ) -> FlushResult<DurabilityNotification> {
        let size = self.wal_size()?;
        if size > self.config.max_wal_size {
            return Err(FlushError::ResourceExhausted {
                resource: "WAL".to_string(),
                message: format!(
                    "WAL size {} exceeds limit {}",
                    size, self.config.max_wal_size
                ),
            });
        }
        self.append(entry, generation_id)
    }

    pub fn recover(partition_dir: &Path) -> FlushResult<Vec<WalEntry>> {
        let reader = WalReader::open(partition_dir)?;
        reader.replay_all()
    }

    pub fn recover_from(partition_dir: &Path, min_sequence: u64) -> FlushResult<Vec<WalEntry>> {
        let reader = WalReader::open(partition_dir)?;
        reader.replay_from(min_sequence)
    }

    pub fn mark_generation_flushed(&mut self, generation_id: u64) -> FlushResult<Vec<u64>> {
        let clean = self.dirty_tracker.mark_generation_flushed(generation_id);
        Ok(clean)
    }

    pub fn cleanup_segments(&mut self, segment_numbers: &[u64]) -> FlushResult<()> {
        for &seg_num in segment_numbers {
            if !self.dirty_tracker.is_segment_clean(seg_num) {
                continue;
            }
            let path = segment_path(&self.partition_dir, seg_num);
            match fs::remove_file(&path) {
                Ok(()) => {
                    self.dirty_tracker.remove_segment(seg_num);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    self.dirty_tracker.remove_segment(seg_num);
                }
                Err(e) => return Err(FlushError::Io(e)),
            }
        }
        Ok(())
    }

    pub fn wal_size(&self) -> FlushResult<u64> {
        let mut total = 0u64;
        for entry in fs::read_dir(&self.partition_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if parse_segment_number(&name.to_string_lossy()).is_some() {
                total += entry.metadata()?.len();
            }
        }
        Ok(total)
    }

    pub fn is_backpressured(&self) -> FlushResult<bool> {
        Ok(self.wal_size()? > self.config.max_wal_size)
    }

    pub fn flush_triggers(&self) -> FlushResult<FlushTriggers> {
        let age_triggered = self
            .dirty_tracker
            .segments_older_than(self.config.segment_max_age);

        let total_size = self.wal_size()?;
        let size_pressure = total_size > self.config.max_total_wal_bytes;
        let backpressure = total_size > self.config.max_wal_size;

        let oldest_pinned_generation = self
            .dirty_tracker
            .oldest_pinned_segment()
            .and_then(|seg| {
                self.dirty_tracker
                    .generations_for_segment(seg)
                    .into_iter()
                    .next()
            });

        Ok(FlushTriggers {
            age_triggered_segments: age_triggered,
            size_pressure,
            oldest_pinned_generation,
            backpressure,
        })
    }

    pub async fn shutdown(mut self) -> FlushResult<()> {
        // Drop the group commit buffer to signal the loop to drain
        drop(self.group_commit);
        if let Some(handle) = self.commit_handle.take() {
            handle.shutdown().await?;
        }
        Ok(())
    }
}
