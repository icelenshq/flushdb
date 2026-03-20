# Task 8: Group Commit

**Crate:** `flushdb-wal`
**File:** `src/group_commit.rs`
**Depends on:** Task 6 (WalWriter)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §5.3, §5.4; phase-2-wal.md §5, §6, Future Work Considerations

---

## Goal

Implement the group commit mechanism that batches multiple writes into a single fsync call, dramatically reducing I/O overhead. Each write gets a durability notification — the caller can `await` it to know when the write is safely on disk. This is the async bridge between the gRPC server (which awaits durability) and the sync WAL writer.

---

## What to Build

### 8.1 DurabilityNotification

A handle returned to the caller when they submit a write:

```
DurabilityNotification = tokio::sync::oneshot::Receiver<FlushResult<()>>
```

The caller awaits this to know when their write is durable. The commit loop sends `Ok(())` after successful fsync, or the relevant `Err(...)` on failure.

**Future Work note:** "The oneshot channel (or equivalent) returned from `append()` is how the gRPC server knows when a write is durable. Design the API so callers can `await` durability."

### 8.2 PendingWrite Struct

Internal struct for a buffered write waiting to be committed:

| Field | Type | Description |
|-------|------|-------------|
| `entry` | `WalEntry` | The WAL entry to write (sequence number not yet assigned) |
| `notifier` | `tokio::sync::oneshot::Sender<FlushResult<()>>` | Channel to notify caller of durability |

### 8.3 GroupCommitBuffer Struct

The submit-side handle that callers use to enqueue writes:

| Field | Type | Description |
|-------|------|-------------|
| `sender` | `tokio::sync::mpsc::Sender<PendingWrite>` | Channel for submitting writes to the commit loop |
| `config` | `WalConfig` | Configuration (commit interval, max bytes, fsync mode) |

### 8.4 GroupCommitHandle Struct

A handle for the background commit task:

| Field | Type | Description |
|-------|------|-------------|
| `join_handle` | `tokio::task::JoinHandle<FlushResult<()>>` | Handle to the background commit loop task |

| Method | Signature | Behavior |
|--------|-----------|----------|
| `shutdown` | `(self) -> FlushResult<()>` | Drops the sender (signals the loop to drain and exit), then awaits the join handle. |

### 8.5 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(writer: WalWriter, config: WalConfig) -> (GroupCommitBuffer, GroupCommitHandle)` | Creates the mpsc channel, spawns the commit loop as a tokio task. Returns the submit handle and the task handle. |
| `submit` | `(&self, entry: WalEntry) -> FlushResult<DurabilityNotification>` | Creates a oneshot channel, wraps the entry in PendingWrite, sends on mpsc. Returns the oneshot receiver. Returns error if the commit loop has shut down (mpsc send fails). |

### 8.6 Commit Loop (Background Task)

The commit loop runs on a spawned tokio task:

**SYNC mode algorithm:**

1. Wait for the first write to arrive on the mpsc channel (blocking wait — no busy loop)
2. Record the arrival time as `batch_start`
3. Collect additional writes from the channel until either:
   a. `group_commit_interval` has elapsed since `batch_start`, OR
   b. Accumulated entry sizes (`sum of total_size()`) exceed `group_commit_max_bytes`, OR
   c. Channel is drained (no more pending writes available via `try_recv`)
4. Move the batch into `tokio::task::spawn_blocking`:
   a. Assign sequence numbers to all entries via `WalWriter::append_batch`
   b. Call `WalWriter::sync()`
5. On success: send `Ok(())` to all notifiers in the batch
6. On failure: send the error (cloned) to all notifiers in the batch
7. Loop back to step 1

**BATCH_SYNC mode algorithm:**

1-3. Same as SYNC mode for collecting the batch
4. In `spawn_blocking`: assign sequence numbers and write entries via `append_batch` (**no fsync**)
5. Send `Ok(())` to all notifiers immediately (writes are in OS buffer, durable enough for BATCH_SYNC semantics)
6. A separate background timer task calls `WalWriter::sync()` every `batch_sync_interval` (default 10ms)
7. Loop back to step 1

**Ordering invariant (SYNC mode):** No notifier receives `Ok(())` until `sync()` completes. A write is only considered durable when the notification fires.

### 8.7 Batch Size Tracking

To determine when the byte threshold is reached, sum `total_size()` of all entries in the current batch. This is the encoded on-disk size and provides a good enough approximation for triggering the commit.

### 8.8 Current Segment Number Exposure

The commit loop owns the WalWriter and can query `current_segment_number()` after each batch. To expose this to the WalManager (for dirty tracking), the commit loop can update a shared `Arc<AtomicU64>` with the current segment number after each write. Alternatively, return the segment number alongside each `DurabilityNotification`.

---

## Tests

**File:** `crates/flushdb-wal/tests/group_commit_tests.rs`

All tests use `tempfile::tempdir()`.

### Basic Commit Tests
| Test | What It Validates |
|------|-------------------|
| `test_single_write_is_durable` | Submit one write, await notification → `Ok(())`, entry readable from WAL |
| `test_multiple_writes_all_notified` | Submit 10 writes, all notifications return `Ok(())` |
| `test_write_data_preserved` | Submit write with specific fields, read back from WAL → fields match |

### Batching Tests
| Test | What It Validates |
|------|-------------------|
| `test_batch_commit_fewer_syncs_than_writes` | Submit 100 writes rapidly → fewer fsync calls than writes (batching happened) |
| `test_timer_trigger_commits_batch` | Submit 1 write, wait > `group_commit_interval` → notification fires |
| `test_size_trigger_commits_batch` | Submit large writes exceeding `group_commit_max_bytes` → commit triggers before timer |
| `test_concurrent_writers` | 10 tokio tasks submit writes concurrently → all get `Ok(())` notifications, all entries readable |

### Ordering Tests
| Test | What It Validates |
|------|-------------------|
| `test_notification_after_sync_in_sync_mode` | In SYNC mode, entry is on disk when notification fires |
| `test_sequence_numbers_monotonic_across_batches` | Entries from different batches have monotonically increasing sequence numbers |

### FsyncMode Tests
| Test | What It Validates |
|------|-------------------|
| `test_sync_mode_fsyncs_every_batch` | SYNC mode: every committed batch is followed by fsync |
| `test_batch_sync_mode_deferred_fsync` | BATCH_SYNC mode: notification fires before fsync, fsync happens on background timer |

### Shutdown Tests
| Test | What It Validates |
|------|-------------------|
| `test_shutdown_drains_pending_writes` | Submit writes then shutdown → all pending writes are committed and notified |
| `test_submit_after_shutdown_returns_error` | `submit()` after shutdown → error |
| `test_shutdown_fsyncs_before_exit` | After shutdown completes, all committed writes are fsync'd to disk |

### Error Propagation Tests
| Test | What It Validates |
|------|-------------------|
| `test_io_error_propagates_to_all_waiters` | If WAL write fails, all notifiers in that batch receive the error |

---

## Done When

- [ ] Writes are batched and committed with a single fsync per batch
- [ ] Timer trigger fires after `group_commit_interval`
- [ ] Size trigger fires when batch exceeds `group_commit_max_bytes`
- [ ] DurabilityNotification fires only after fsync in SYNC mode
- [ ] BATCH_SYNC mode defers fsync to a background timer
- [ ] Concurrent writers all receive correct notifications
- [ ] Shutdown drains all pending writes and fsyncs
- [ ] Sequence numbers are monotonically increasing across all batches
- [ ] All tests pass
