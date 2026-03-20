# Task 12: Engine Orchestrator

**Crate:** `flushdb-engine`
**File:** `src/engine.rs`
**Depends on:** All previous tasks (1-11)
**Estimated complexity:** XL
**Design reference:** STORAGE_DESIGN.md §9, §10, §11, §22, Phase 5 Future Work (all downstream dependencies)

---

## Goal

Build the top-level `Engine` struct that wires together the memtable, WAL, flush pipeline, read path, compaction, and recovery into a cohesive embedded KV store. The Engine is the public API for Phase 5 — upstream consumers (the gRPC server in Phase 7) interact exclusively with the Engine. After this task, flushdb is a working embedded KV store that can write, read, flush, compact, recover, and handle concurrent operations.

---

## What to Build

### 12.1 EngineConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `memtable_config` | `MemtableConfig` | Default | Memtable sizing and freeze limits |
| `wal_config` | `WalConfig` | Default | WAL segment sizes, fsync modes |
| `flush_config` | `FlushConfig` | Default | Flush triggers and SSTable config |
| `compaction_config` | `CompactionConfig` | Default | Compaction triggers and thresholds |
| `manifest_config` | `ManifestConfig` | Default | Manifest paths and snapshot intervals |
| `namespace` | `String` | required | Namespace this engine operates on |
| `local_dir` | `PathBuf` | required | Local directory for WAL and temp files |

### 12.2 Engine

```
Engine<B: StorageBackend>
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `memtable_list` | `MemtableList` | Active + frozen memtables |
| `wal_manager` | `WalManager` | WAL writer and segment manager |
| `manifest_manager` | `ManifestManager<B>` | Manifest CAS and epoch management |
| `flush_pipeline` | `FlushPipeline<B>` | Flush executor |
| `compaction_scheduler` | `CompactionScheduler` | Compaction trigger detection |
| `compaction_executor` | `CompactionExecutor<B>` | Compaction merge executor |
| `levels` | `Vec<LevelState>` | Open SSTableHandles per level |
| `fetcher` | `DirectBlockFetcher<B>` | Block-level I/O (P6 cache wraps this) |
| `config` | `EngineConfig` | Engine configuration |
| `next_sequence` | `u64` | Next sequence number to assign |
| `generation_counter` | `u64` | WAL generation counter |

### 12.3 Lifecycle Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `async (backend: B, config: EngineConfig) -> FlushResult<Self>` | Opens the engine: (1) run recovery to load manifest + replay WAL, (2) acquire writer epoch, (3) initialize all components. |
| `close` | `async (&mut self) -> FlushResult<()>` | Graceful shutdown: (1) flush active memtable if non-empty, (2) wait for in-flight flushes, (3) sync WAL. |

### 12.4 Write Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `put` | `async (&mut self, record_id: &[u8], item_key: &[u8], value: Bytes, metadata: Bytes, idempotency_token: Option<IdempotencyToken>) -> FlushResult<u64>` | Write a single entry. Returns the assigned sequence number. |
| `delete` | `async (&mut self, record_id: &[u8], item_key: &[u8]) -> FlushResult<u64>` | Delete a single entry (writes a tombstone). Returns the assigned sequence number. |
| `delete_range` | `async (&mut self, record_id: &[u8], start_key: &[u8], end_key: &[u8]) -> FlushResult<u64>` | Delete a range of entries (writes a range tombstone). Returns the assigned sequence number. |

**Write path (shared by put/delete/delete_range):**

1. **Check write stall** — call `compaction_scheduler.write_stall_status(manifest)`:
   - `Normal` → proceed
   - `Slowdown` → sleep for `delay_ms` milliseconds, then proceed
   - `Stopped` → return `FlushError::ResourceExhausted` with L0 count and retry hint

2. **Check dedup** — if `idempotency_token` is Some, check `memtable_list.check_dedup(token)`:
   - If duplicate: return `FlushError::DuplicateToken`

3. **Create MemtableEntry** with:
   - `CompositeKey::new(record_id, item_key)`
   - `sequence_number = self.next_sequence` (then increment)
   - Appropriate `EntryType` (Put, Delete, or RangeDelete)

4. **Write to WAL** — `wal_manager.append(wal_entry, generation_id)`

5. **Insert into memtable** — `memtable_list.insert(entry)`

6. **Check freeze trigger** — if memtable should freeze:
   - Call `memtable_list.freeze_active()` to freeze the current memtable
   - Increment `generation_counter`
   - Trigger async flush (see 12.5)

7. **Return** sequence number

### 12.5 Flush Orchestration

| Method | Signature | Behavior |
|--------|-----------|----------|
| `maybe_flush` | `async (&mut self) -> FlushResult<Option<FlushResult_>>` | If there are frozen memtables, flush the oldest one. Returns flush result if a flush occurred. |
| `flush_frozen` | `async (&mut self) -> FlushResult<FlushResult_>` | Pops the oldest frozen memtable, calls `flush_pipeline.flush()`, then performs WAL cleanup and compaction trigger check. |

**After flush:**
1. Call `wal_manager.mark_generation_flushed(generation_id)` to get deletable segments
2. Call `wal_manager.cleanup_segments(&deletable)` to remove old WAL files
3. Update `self.levels` — open the new L0 SSTableHandle, add to L0's LevelState
4. Check compaction triggers via `compaction_scheduler.check_triggers(manifest)`
5. If compaction needed, call `maybe_compact`

### 12.6 Compaction Orchestration

| Method | Signature | Behavior |
|--------|-----------|----------|
| `maybe_compact` | `async (&mut self) -> FlushResult<Vec<CompactionResult>>` | Checks compaction triggers, executes any triggered compaction tasks. Returns results. |
| `compact` | `async (&mut self, task: CompactionTask) -> FlushResult<CompactionResult>` | Executes a single compaction task via `compaction_executor.execute()`. After completion: (1) update `self.levels` to reflect new SSTable layout, (2) schedule background deletion of consumed SSTables. |

**After compaction:**
1. Rebuild `LevelState` for affected levels (re-open new SSTableHandles)
2. Close old SSTableHandles for consumed SSTables
3. Mark consumed SSTable files for deferred deletion (background cleanup)

### 12.7 Read Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `get` | `async (&self, record_id: &[u8], item_key: &[u8]) -> FlushResult<Option<GetResult>>` | Point read via `ReadPath::point_read` |
| `scan` | `async (&self, record_id: &[u8], start_key: Option<&[u8]>, end_key: Option<&[u8]>, options: RangeReadOptions) -> FlushResult<RangeReadResult>` | Range read via `ReadPath::range_read` |
| `multi_get` | `async (&self, record_id: &[u8], keys: &[&[u8]]) -> FlushResult<Vec<Option<GetResult>>>` | Multi-key read via `ReadPath::multi_get` |

### 12.8 Status and Observability

| Method | Signature | Behavior |
|--------|-----------|----------|
| `write_stall_status` | `(&self) -> WriteStallStatus` | Current write stall status |
| `manifest` | `(&self) -> &Manifest` | Current manifest (for inspection) |
| `l0_count` | `(&self) -> usize` | Number of L0 SSTables |
| `level_sizes` | `(&self) -> Vec<(Level, u64)>` | Total bytes per level |
| `frozen_memtable_count` | `(&self) -> usize` | Number of frozen memtables pending flush |

### 12.9 Deferred SSTable Deletion

After compaction replaces SSTables, old files must be deleted from StorageBackend. This happens asynchronously to not block the compaction path:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `schedule_deletion` | `(&mut self, sst_ids: Vec<String>)` | Adds SSTable IDs to a pending deletion queue |
| `run_deletion` | `async (&mut self) -> FlushResult<usize>` | Deletes pending SSTable files from StorageBackend in batches, returns count deleted |

### 12.10 Manifest Version Change Notification

Expose a mechanism for Phase 6's cache to subscribe to manifest changes:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `manifest_version` | `(&self) -> ManifestId` | Current manifest version — cache can poll this |
| `consumed_sstable_ids` | `(&self) -> &[String]` | SSTable IDs consumed since last check (for cache eviction) |

---

## Tests

**File:** `crates/flushdb-engine/tests/engine_tests.rs`

### Lifecycle Tests
| Test | What It Validates |
|------|-------------------|
| `test_engine_open_fresh` | Open on empty storage → engine starts with no data |
| `test_engine_open_existing` | Open with existing manifest + SSTables → engine recovers state |
| `test_engine_close_flushes_active` | Close → active memtable flushed, WAL synced |

### Write Path Tests
| Test | What It Validates |
|------|-------------------|
| `test_put_single_entry` | Single put → entry readable via get |
| `test_put_multiple_entries` | Multiple puts with different keys → all readable |
| `test_put_overwrites_value` | Put same key twice → get returns latest value |
| `test_delete_entry` | Put then delete → get returns None |
| `test_delete_range` | Put 10 entries, delete range covering 5 → 5 remain |
| `test_put_returns_sequence_number` | Each put returns a monotonically increasing sequence |
| `test_dedup_rejects_duplicate_token` | Same idempotency token → `DuplicateToken` error |

### Read Path Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_from_active_memtable` | Entry in active memtable → found |
| `test_get_from_frozen_memtable` | Freeze memtable → entry still found |
| `test_get_from_flushed_sstable` | Flush → entry found via SSTable |
| `test_get_after_multiple_flushes` | Multiple flushes → entries from all SSTables found |
| `test_scan_single_record` | Scan all items in a record → correct results |
| `test_scan_bounded_range` | Scan with start/end → only items in range |
| `test_scan_pagination` | Large record → paginated results with no gaps or duplicates |
| `test_multi_get` | Multiple keys → batch results |

### Flush Tests
| Test | What It Validates |
|------|-------------------|
| `test_auto_freeze_on_size` | Fill memtable past threshold → auto-freeze and flush |
| `test_wal_segments_cleaned_after_flush` | After flush, WAL segments for flushed generation are removed |
| `test_multiple_flushes_grow_l0` | N flushes → N L0 SSTables in manifest |

### Compaction Tests
| Test | What It Validates |
|------|-------------------|
| `test_l0_compaction_triggered` | 5+ L0 SSTables → compaction runs, L0 reduced |
| `test_compaction_preserves_data` | After compaction, all data still readable |
| `test_compaction_deduplicates` | Same key in multiple SSTables → only newest in L1 |
| `test_compaction_drops_tombstoned_entries` | Tombstone + covered Put compacted to bottom level → both dropped |

### Write Stalling Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_stall_at_hard_limit` | 12+ L0 SSTables → writes return `ResourceExhausted` |
| `test_write_resumes_after_compaction` | L0 compacted → writes resume |

### Recovery Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_crash_recover_read` | Write 1000 entries → simulate crash (drop engine) → reopen → all entries readable |
| `test_flush_crash_recover` | Write → flush → write more → crash → recover → all data present |
| `test_recovery_replays_unflushed` | Entries in WAL but not flushed → replayed into memtable after recovery |

### End-to-End Tests
| Test | What It Validates |
|------|-------------------|
| `test_full_lifecycle` | Open → write 10K entries → flush → compact → read → close → reopen → read → all correct |
| `test_mixed_puts_and_deletes` | Interleaved puts and deletes → reads return correct state |
| `test_concurrent_reads_and_writes` | Reads during flush/compaction return correct data |
| `test_large_scale_write_read` | Write 100K entries across 1K records → scan each record → correct |

---

## Done When

- [ ] `Engine::open` performs full recovery (manifest + WAL replay)
- [ ] `Engine::close` flushes active memtable and syncs WAL
- [ ] `put`/`delete`/`delete_range` write to WAL + memtable with sequence numbers
- [ ] Write stalling rejects writes at L0 hard limit, provides delay at soft limit
- [ ] Dedup via idempotency tokens works across memtable boundaries
- [ ] `get` reads across all layers (memtable → frozen → L0-L3)
- [ ] `scan` merge-sorts across all layers with pagination
- [ ] Auto-freeze triggers flush when memtable reaches size threshold
- [ ] WAL segments cleaned after flush completes
- [ ] Compaction triggered when L0 exceeds threshold
- [ ] Compaction preserves all non-tombstoned data
- [ ] Recovery after crash produces correct state — zero data loss for ACK'd writes
- [ ] This is a **working embedded KV store**
- [ ] All tests pass
