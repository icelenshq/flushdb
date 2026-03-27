use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use flushdb_types::{FlushError, FlushResult};
use tokio::sync::{mpsc, oneshot};

use crate::config::{FsyncMode, WalConfig};
use crate::entry::WalEntry;
use crate::wal_writer::WalWriter;

pub type DurabilityNotification = oneshot::Receiver<FlushResult<()>>;

struct PendingWrite {
    entry: WalEntry,
    notifier: oneshot::Sender<FlushResult<()>>,
}

struct PendingBatch {
    entries: Vec<WalEntry>,
    notifier: oneshot::Sender<FlushResult<()>>,
}

enum PendingCommit {
    Single(PendingWrite),
    Batch(PendingBatch),
}

impl PendingCommit {
    fn into_parts(self) -> (Vec<WalEntry>, Vec<oneshot::Sender<FlushResult<()>>>, usize) {
        match self {
            Self::Single(pending) => {
                let bytes = pending.entry.total_size();
                (vec![pending.entry], vec![pending.notifier], bytes)
            }
            Self::Batch(batch) => {
                let bytes = batch.entries.iter().map(WalEntry::total_size).sum();
                (batch.entries, vec![batch.notifier], bytes)
            }
        }
    }
}

pub struct GroupCommitBuffer {
    sender: mpsc::Sender<PendingCommit>,
}

pub struct GroupCommitHandle {
    join_handle: tokio::task::JoinHandle<FlushResult<()>>,
}

impl GroupCommitBuffer {
    pub fn new(
        writer: WalWriter,
        config: WalConfig,
        current_segment: Arc<AtomicU64>,
    ) -> (Self, GroupCommitHandle) {
        let (tx, rx) = mpsc::channel::<PendingCommit>(4096);

        let join_handle = tokio::spawn(commit_loop(writer, rx, config, current_segment));

        let buffer = Self { sender: tx };
        let handle = GroupCommitHandle { join_handle };
        (buffer, handle)
    }

    pub async fn submit(&self, entry: WalEntry) -> FlushResult<DurabilityNotification> {
        let (tx, rx) = oneshot::channel();
        let pending = PendingCommit::Single(PendingWrite {
            entry,
            notifier: tx,
        });
        self.sender.send(pending).await.map_err(|_| {
            FlushError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "group commit loop has shut down",
            ))
        })?;
        Ok(rx)
    }

    pub async fn submit_batch(
        &self,
        entries: Vec<WalEntry>,
    ) -> FlushResult<DurabilityNotification> {
        if entries.is_empty() {
            let (tx, rx) = oneshot::channel();
            let _ = tx.send(Ok(()));
            return Ok(rx);
        }

        let (tx, rx) = oneshot::channel();
        let pending = PendingCommit::Batch(PendingBatch {
            entries,
            notifier: tx,
        });
        self.sender.send(pending).await.map_err(|_| {
            FlushError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "group commit loop has shut down",
            ))
        })?;
        Ok(rx)
    }
}

impl GroupCommitHandle {
    pub async fn shutdown(self) -> FlushResult<()> {
        // The sender is dropped externally (by WalManager dropping GroupCommitBuffer)
        // which signals the commit loop to drain and exit.
        self.join_handle
            .await
            .map_err(|e| FlushError::Io(std::io::Error::other(e.to_string())))?
    }
}

async fn commit_loop(
    mut writer: WalWriter,
    mut rx: mpsc::Receiver<PendingCommit>,
    config: WalConfig,
    current_segment: Arc<AtomicU64>,
) -> FlushResult<()> {
    let mut has_unsynced = false;

    loop {
        // For BATCH_SYNC mode, use timeout to ensure periodic sync
        let first = if config.fsync_mode == FsyncMode::BatchSync && has_unsynced {
            match tokio::time::timeout(config.batch_sync_interval, rx.recv()).await {
                Ok(val) => val,
                Err(_timeout) => {
                    // Timer fired - sync pending data
                    writer = do_sync(writer).await?;
                    has_unsynced = false;
                    continue;
                }
            }
        } else {
            rx.recv().await
        };

        let first = match first {
            Some(pending) => pending,
            None => {
                // Channel closed - sync remaining data and exit
                if has_unsynced {
                    do_sync(writer).await?;
                }
                return Ok(());
            }
        };

        // Collect batch
        let (mut entries, mut notifiers, mut batch_bytes) = first.into_parts();
        let batch_start = Instant::now();

        loop {
            if batch_bytes >= config.group_commit_max_bytes {
                break;
            }
            if batch_start.elapsed() >= config.group_commit_interval {
                break;
            }
            match rx.try_recv() {
                Ok(pending) => {
                    let (next_entries, next_notifiers, next_bytes) = pending.into_parts();
                    batch_bytes += next_bytes;
                    entries.extend(next_entries);
                    notifiers.extend(next_notifiers);
                }
                Err(_) => break,
            }
        }

        // Write batch via spawn_blocking (sync I/O)
        let do_fsync = config.fsync_mode == FsyncMode::Sync;
        let (returned_writer, write_result) = tokio::task::spawn_blocking(move || {
            let result = (|| -> FlushResult<()> {
                writer.append_batch(&mut entries)?;
                if do_fsync {
                    writer.sync()?;
                }
                Ok(())
            })();
            (writer, result)
        })
        .await
        .map_err(|e| FlushError::Io(std::io::Error::other(e.to_string())))?;

        writer = returned_writer;
        current_segment.store(writer.current_segment_number(), Ordering::Relaxed);

        match &write_result {
            Ok(()) => {
                for notifier in notifiers {
                    let _ = notifier.send(Ok(()));
                }
                if !do_fsync {
                    has_unsynced = true;
                }
            }
            Err(e) => {
                let msg = e.to_string();
                for notifier in notifiers {
                    let _ = notifier.send(Err(FlushError::Io(std::io::Error::other(msg.clone()))));
                }
                return write_result;
            }
        }
    }
}

async fn do_sync(mut writer: WalWriter) -> FlushResult<WalWriter> {
    let (w, result) = tokio::task::spawn_blocking(move || {
        let result = writer.sync();
        (writer, result)
    })
    .await
    .map_err(|e| FlushError::Io(std::io::Error::other(e.to_string())))?;
    result?;
    Ok(w)
}
