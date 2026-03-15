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

pub struct GroupCommitBuffer {
    sender: mpsc::Sender<PendingWrite>,
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
        let (tx, rx) = mpsc::channel::<PendingWrite>(4096);

        let join_handle = tokio::spawn(commit_loop(writer, rx, config, current_segment));

        let buffer = Self { sender: tx };
        let handle = GroupCommitHandle { join_handle };
        (buffer, handle)
    }

    pub fn submit(&self, entry: WalEntry) -> FlushResult<DurabilityNotification> {
        let (tx, rx) = oneshot::channel();
        let pending = PendingWrite {
            entry,
            notifier: tx,
        };
        self.sender.try_send(pending).map_err(|_| FlushError::Io(
            std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "group commit loop has shut down",
            ),
        ))?;
        Ok(rx)
    }
}

impl GroupCommitHandle {
    pub async fn shutdown(self) -> FlushResult<()> {
        // The sender is dropped externally (by WalManager dropping GroupCommitBuffer)
        // which signals the commit loop to drain and exit.
        self.join_handle
            .await
            .map_err(|e| {
                FlushError::Io(std::io::Error::other(
                    e.to_string(),
                ))
            })?
    }
}

async fn commit_loop(
    mut writer: WalWriter,
    mut rx: mpsc::Receiver<PendingWrite>,
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
            Some(pw) => pw,
            None => {
                // Channel closed - sync remaining data and exit
                if has_unsynced {
                    do_sync(writer).await?;
                }
                return Ok(());
            }
        };

        // Collect batch
        let mut entries = vec![first.entry];
        let mut notifiers = vec![first.notifier];
        let mut batch_bytes = entries[0].total_size();
        let batch_start = Instant::now();

        loop {
            if batch_bytes >= config.group_commit_max_bytes {
                break;
            }
            if batch_start.elapsed() >= config.group_commit_interval {
                break;
            }
            match rx.try_recv() {
                Ok(pw) => {
                    batch_bytes += pw.entry.total_size();
                    entries.push(pw.entry);
                    notifiers.push(pw.notifier);
                }
                Err(_) => break,
            }
        }

        // Write batch via spawn_blocking (sync I/O)
        let do_fsync = config.fsync_mode == FsyncMode::Sync;
        let (returned_writer, write_result) =
            tokio::task::spawn_blocking(move || {
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
            .map_err(|e| {
                FlushError::Io(std::io::Error::other(
                    e.to_string(),
                ))
            })?;

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
                    let _ = notifier.send(Err(FlushError::Io(std::io::Error::other(
                        msg.clone(),
                    ))));
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
    .map_err(|e| {
        FlushError::Io(std::io::Error::other(
            e.to_string(),
        ))
    })?;
    result?;
    Ok(w)
}
