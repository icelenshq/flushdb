# Task 10: WAL Manager & Backpressure

**Crate:** `flushdb-wal`
**File:** `src/wal_manager.rs`
**Depends on:** Task 6 (WalWriter), Task 7 (WalReader), Task 8 (GroupCommitBuffer), Task 9 (DirtySegmentTracker)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §5.3, §5.5; phase-2-wal.md §5, §6, §7

---

## Goal

Build the top-level WAL API that orchestrates all WAL components — the writer, group commit, dirty tracker, and reader. This is the interface that the storage engine (Phase 5) uses. It handles WAL lifecycle, backpressure, and segment cleanup.

---

## What to Build

### 10.1 WalManager Struct

| Field | Type | Description |
|-------|------|-------------|
| `config` | `WalConfig` | WAL configuration |
| `group_commit` | `GroupCommitBuffer` | Async write submission interface |
| `commit_handle` | `Option<GroupCommitHandle>` | Handle to the background commit task (Option for shutdown) |
| `dirty_tracker` | `DirtySegmentTracker` | Tracks segment cleanup eligibility |
| `partition_dir` | `PathBuf` | WAL directory path |
| `current_segment_number` | `Arc<AtomicU64>` | Current segment number, updated by the commit loop |

**Design note:** The WalWriter is owned by the GroupCommitBuffer's background task (moved into the commit loop in Task 8). The WalManager does not directly access the writer — all writes go through group commit.

### 10.2 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `(partition_dir: &Path, config: WalConfig) -> FlushResult<Self>` | Creates WAL directory, opens WalWriter, starts GroupCommitBuffer, initializes DirtySegmentTracker, sets up shared segment number tracking. |
| `append` | `(&mut self, entry: WalEntry, generation_id: u64) -> FlushResult<DurabilityNotification>` | Checks backpressure → submits to group commit → records write in dirty tracker using current segment number. Returns durability notification handle. |
| `append_if_not_full` | `(&self, entry: WalEntry, generation_id: u64) -> FlushResult<DurabilityNotification>` | Same as `append` but returns `FlushError::ResourceExhausted` if WAL size exceeds `config.max_wal_size`. |
| `recover` | `(partition_dir: &Path) -> FlushResult<Vec<WalEntry>>` | **Static method.** Opens WalReader and replays all entries. For use during startup before the manager is fully initialized. |
| `recover_from` | `(partition_dir: &Path, min_sequence: u64) -> FlushResult<Vec<WalEntry>>` | **Static method.** Replays entries with `sequence_number >= min_sequence`. |
| `mark_generation_flushed` | `(&mut self, generation_id: u64) -> FlushResult<Vec<u64>>` | Marks generation as flushed in dirty tracker. Returns segment numbers that became clean (eligible for deletion). Does NOT delete them — caller decides. |
| `cleanup_segments` | `(&mut self, segment_numbers: &[u64]) -> FlushResult<()>` | Deletes the specified segments from disk via WalWriter. Only deletes segments that the dirty tracker confirms are clean. Skips dirty segments silently. |
| `wal_size` | `(&self) -> FlushResult<u64>` | Returns total WAL size on disk across all segments. |
| `is_backpressured` | `(&self) -> FlushResult<bool>` | True if WAL size exceeds `config.max_wal_size`. |
| `flush_triggers` | `(&self) -> FlushResult<FlushTriggers>` | Returns which flush triggers are currently active (age, size pressure). The engine uses this to decide when to force-flush memtables. |
| `shutdown` | `(self) -> FlushResult<()>` | Shuts down group commit (drains pending writes, fsyncs), consumes self. |

### 10.3 FlushTriggers Struct

Returned by `flush_triggers()` to inform the engine about WAL pressure:

| Field | Type | Description |
|-------|------|-------------|
| `age_triggered_segments` | `Vec<u64>` | Dirty segments older than `config.segment_max_age` |
| `size_pressure` | `bool` | True if total WAL size > `config.max_total_wal_bytes` |
| `oldest_pinned_generation` | `Option<u64>` | The generation referencing the oldest pinned segment (flush this first) |
| `backpressure` | `bool` | True if WAL size > `config.max_wal_size` (writes should be rejected) |

Derives: `Debug`, `Clone`

### 10.4 Backpressure Protocol

From phase-2-wal.md §7:

1. Before accepting a write in `append_if_not_full`, check `wal_size()` against `config.max_wal_size`
2. If exceeded: return `FlushError::ResourceExhausted` with context describing the WAL size limit
3. The engine (Phase 5) is responsible for retrying after a flush completes
4. This prevents local disk exhaustion during prolonged S3 unavailability

### 10.5 Dirty Tracking Integration

When `append()` is called with a `generation_id`:

1. Submit write to group commit (gets DurabilityNotification)
2. Record the write in dirty tracker using `current_segment_number` (from the shared AtomicU64)
3. Return the notification to the caller

**Segment number tracking:** The commit loop (Task 8) updates the shared `Arc<AtomicU64>` with the current segment number after each batch. The WalManager reads this atomically when recording writes in the dirty tracker. This is a conservative approximation — if rotation happens mid-batch, some entries may be recorded against the old segment number. This is safe: the segment stays alive longer than strictly necessary, but never deleted prematurely.

### 10.6 Trait Does NOT Include

- **No memtable flush orchestration** — the WAL Manager reports flush triggers, but the engine decides when and how to flush memtables
- **No S3 interaction** — segment cleanup is local disk only
- **No WAL replication** — future cluster mode will add replication on top of this API

---

## Tests

**File:** `crates/flushdb-wal/tests/wal_manager_tests.rs`

All tests use `tempfile::tempdir()`.

### Lifecycle Tests
| Test | What It Validates |
|------|-------------------|
| `test_open_creates_wal_directory` | Opening WalManager creates the partition directory |
| `test_open_on_existing_wal` | Opening with existing segments resumes correctly |
| `test_shutdown_flushes_pending_writes` | All pending writes are durable after shutdown |

### Write Path Tests
| Test | What It Validates |
|------|-------------------|
| `test_append_returns_notification` | `append()` returns a DurabilityNotification handle |
| `test_append_notification_fires` | Awaiting notification returns `Ok(())` after write is durable |
| `test_append_multiple_writes` | 100 appends all succeed and are durable |
| `test_append_preserves_entry_data` | Written entries recoverable with correct field data |

### Recovery Tests
| Test | What It Validates |
|------|-------------------|
| `test_recover_reads_all_entries` | Write entries, shutdown, `recover()` → all entries returned |
| `test_recover_from_filters_by_sequence` | `recover_from(50)` returns only entries with seq >= 50 |
| `test_recover_empty_wal` | No entries written → empty vec |
| `test_recover_after_crash_simulation` | Write entries, don't call shutdown (simulate crash), `recover()` → valid entries recovered |

### Backpressure Tests
| Test | What It Validates |
|------|-------------------|
| `test_backpressure_rejects_writes` | Set `max_wal_size = 1024`, write until rejected → `ResourceExhausted` |
| `test_is_backpressured_reflects_wal_size` | `is_backpressured()` returns true when WAL size exceeds threshold |
| `test_backpressure_clears_after_cleanup` | Clean up segments → `is_backpressured()` returns false, writes accepted again |

### Dirty Tracking Integration Tests
| Test | What It Validates |
|------|-------------------|
| `test_mark_flushed_returns_clean_segments` | Write entries, mark generation flushed → clean segment numbers returned |
| `test_cleanup_deletes_clean_segments` | `cleanup_segments()` removes files from disk |
| `test_cleanup_skips_dirty_segments` | Attempting to clean a dirty segment → no deletion, no error |
| `test_full_lifecycle` | Write → rotate → write more → flush gen 1 → cleanup seg 1 → verify remaining data intact |

### Flush Trigger Tests
| Test | What It Validates |
|------|-------------------|
| `test_flush_triggers_age` | Old segment triggers age-based flush in `FlushTriggers` |
| `test_flush_triggers_size_pressure` | Large WAL triggers size pressure in `FlushTriggers` |
| `test_flush_triggers_none_when_healthy` | Small, fresh WAL → no triggers active |

---

## Done When

- [ ] WalManager opens, writes, recovers, and shuts down correctly
- [ ] Writes go through group commit and are durable on notification
- [ ] Backpressure rejects writes when WAL exceeds `max_wal_size`
- [ ] Recovery replays all valid entries after crash
- [ ] Dirty tracking + cleanup deletes only clean segments
- [ ] Flush triggers report age and size pressure correctly
- [ ] All tests pass
