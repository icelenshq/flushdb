# Task 6: Flush Pipeline

**Crate:** `flushdb-engine`
**File:** `src/flush.rs`
**Depends on:** Task 1 (Manifest types), Task 2 (ManifestManager CAS protocol)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §9.1 (End-to-End Flush Sequence), §9.2 (Failure Modes), §9.3 (Flush Backpressure)

---

## Goal

Implement the full flush pipeline that converts a frozen memtable into an SSTable on StorageBackend and atomically registers it in the manifest. This is the bridge between in-memory writes (WAL + memtable) and durable storage. The pipeline must be idempotent across crash recovery — the WAL is the safety net, the manifest CAS is the commit point.

---

## What to Build

### 6.1 FlushConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `sst_config` | `SstConfig` | Default SSTable config | Block size, compression, bloom filter settings |
| `max_frozen_count` | `usize` | `3` | Write stall threshold — reject writes if this many frozen memtables accumulate |
| `flush_trigger_size` | `usize` | `64 * 1024 * 1024` | Freeze memtable when it reaches this size (64 MB) |
| `flush_trigger_age` | `Duration` | `5 minutes` | Freeze memtable after this age even if not full |

### 6.2 FlushPipeline

```
FlushPipeline<B: StorageBackend>
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `sst_writer` | `SSTableWriter` | Existing writer from Phase 4 |
| `config` | `FlushConfig` | Flush configuration |
| `namespace` | `String` | Namespace for path construction |
| `base_path` | `String` | Base path prefix |

**Methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: FlushConfig, namespace: String, base_path: String) -> Self` | Creates a flush pipeline |
| `flush` | `async (&self, frozen: Memtable, manifest_manager: &mut ManifestManager<B>, backend: &B, generation_id: u64) -> FlushResult<FlushResult_>` | Executes the full flush sequence (see 6.3) |
| `should_freeze` | `(&self, memtable: &Memtable, max_age: Duration) -> bool` | Returns true if memtable should be frozen (size or age threshold) |
| `check_backpressure` | `(&self, frozen_count: usize) -> FlushResult<()>` | Returns `FlushError::ResourceExhausted` if `frozen_count >= max_frozen_count` |

### 6.3 Flush Sequence

The `flush` method executes these steps:

**Step 1: Build SSTable**
- Iterate the frozen memtable in sorted order via `memtable.into_skiplist().into_iter()`
- Convert `SkipNode` entries to `MemtableEntry` for `SSTableWriter::write`
- The SSTableWriter produces an `SstInfo` with file metadata

**Step 2: Upload to StorageBackend**
- Generate L0 path: `{base_path}/{namespace}/sstables/L0/{ulid}.sst`
- SSTableWriter already writes to StorageBackend in its `write` method — the "upload" is implicit

**Step 3: Build SSTableMeta**
- Convert `SstInfo` to `SSTableMeta` with:
  - `sequence_range` from memtable's min/max sequence numbers
  - `record_id_count` from bloom filter builder's count
  - `created_at_ms` from current time
  - `run_id = None`, `fragment_index = None` (L0 SSTables are not run fragments)

**Step 4: Update Manifest (CAS)**
- Create `ManifestUpdate` with:
  - `trigger = ManifestUpdateTrigger::Flush`
  - `add_sstables = [(Level::L0, sstable_meta)]`
  - `remove_sstables = []`
  - `new_last_flushed_sequence = Some(max_sequence)`
  - `writer_epoch = manifest_manager.writer_epoch`
  - `compactor_epoch = manifest_manager.compactor_epoch`
- Call `manifest_manager.update(update)` — this handles CAS and epoch validation

**Step 5: Return flush result**

### 6.4 FlushResult_ (Flush Output)

The result of a successful flush (named `FlushResult_` to avoid collision with `FlushResult<T>`):

| Field | Type | Description |
|-------|------|-------------|
| `sst_info` | `SstInfo` | SSTable file info |
| `sst_meta` | `SSTableMeta` | Manifest metadata |
| `flushed_sequence_range` | `(u64, u64)` | Range of sequence numbers flushed |
| `generation_id` | `u64` | WAL generation that was flushed |
| `l0_count_after` | `usize` | Number of L0 SSTables after this flush (for compaction trigger check) |

### 6.5 WAL Cleanup Integration

After a successful flush, the caller (Engine orchestrator, Task 12) is responsible for:
1. Calling `wal_manager.mark_generation_flushed(generation_id)` to get deletable segments
2. Calling `wal_manager.cleanup_segments(&deletable)` to remove old WAL segments

This separation keeps the flush pipeline focused on the SSTable + manifest path, while WAL cleanup is orchestrated by the Engine.

### 6.6 Compaction Trigger Check

The `FlushResult_` includes `l0_count_after` so the Engine can check:
- If `l0_count_after > 4`: schedule L0→L1 compaction (Task 10/11)

### 6.7 Failure Mode Handling

| Failure Point | Behavior |
|---------------|----------|
| SSTable build fails | No state changed. Return error. WAL replay will recreate the memtable. |
| SSTable upload fails | Partial file may exist on StorageBackend. Return error. Orphan GC (future) cleans it. WAL replay recreates memtable. |
| Manifest CAS fails with EpochFenced | Return `EpochFenced`. This node is a zombie — caller must halt. |
| Manifest CAS fails and retries exhausted | Return error. SSTable is uploaded but not referenced (orphan). WAL replay recreates memtable. |

---

## Tests

**File:** `crates/flushdb-engine/tests/flush_pipeline_tests.rs`

### Happy Path Tests
| Test | What It Validates |
|------|-------------------|
| `test_flush_creates_sstable_and_updates_manifest` | Freeze memtable → flush → SSTable exists on storage, manifest has new L0 entry |
| `test_flush_sstable_contains_all_entries` | Read back SSTable → all entries from frozen memtable present |
| `test_flush_updates_last_flushed_sequence` | Manifest's `last_flushed_sequence` equals max sequence from memtable |
| `test_flush_l0_path_format` | SSTable written to `{base}/{ns}/sstables/L0/{id}.sst` |
| `test_flush_returns_correct_l0_count` | `l0_count_after` reflects the actual L0 count in updated manifest |

### Sequence Tests
| Test | What It Validates |
|------|-------------------|
| `test_multiple_flushes_increment_manifest` | Two flushes → manifest_id increments twice, both SSTables in L0 |
| `test_flush_preserves_entry_order` | Entries in SSTable are sorted by CompositeKey (memtable's skip list order) |
| `test_flush_includes_tombstones` | Tombstone entries from memtable appear in SSTable |
| `test_flush_includes_range_tombstones` | Range tombstone entries are included |

### Backpressure Tests
| Test | What It Validates |
|------|-------------------|
| `test_backpressure_rejects_when_frozen_limit_reached` | `check_backpressure(3)` with `max_frozen_count=3` → `ResourceExhausted` |
| `test_backpressure_allows_below_limit` | `check_backpressure(2)` with `max_frozen_count=3` → Ok |

### Freeze Trigger Tests
| Test | What It Validates |
|------|-------------------|
| `test_should_freeze_by_size` | Memtable exceeding `flush_trigger_size` → true |
| `test_should_freeze_by_age` | Memtable older than `flush_trigger_age` → true |
| `test_should_not_freeze_below_thresholds` | Small, young memtable → false |

### Error Handling Tests
| Test | What It Validates |
|------|-------------------|
| `test_flush_epoch_fenced` | Manifest has higher writer_epoch → `EpochFenced` |
| `test_flush_empty_memtable` | Flushing an empty memtable → either no-op or error (not a crash) |

---

## Done When

- [ ] Frozen memtable → SSTable → manifest update works end-to-end
- [ ] SSTable contains all entries from the frozen memtable in sorted order
- [ ] Manifest CAS atomically adds the new L0 SSTable
- [ ] `last_flushed_sequence` updated to max sequence in flushed memtable
- [ ] Backpressure check rejects writes when too many frozen memtables accumulate
- [ ] Freeze triggers work by size and age
- [ ] Epoch fencing prevents zombie flushes
- [ ] FlushResult_ contains enough info for WAL cleanup and compaction trigger check
- [ ] All tests pass
