# Task 9: Recovery

**Crate:** `flushdb-engine`
**File:** `src/recovery.rs`
**Depends on:** Task 1 (Manifest types), Task 2 (ManifestManager), Task 4 (BlockFetcher, SSTableHandle, LevelState)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §22 (Recovery), Phase 5 §5d

---

## Goal

Implement the recovery procedure that reconstructs a partition's full state from the manifest and WAL after a crash or restart. Recovery loads the manifest from StorageBackend (source of truth for flushed state), rebuilds in-memory SSTable metadata (bloom filters, index blocks), and replays WAL entries beyond the manifest's `last_flushed_sequence`. After recovery, the partition resumes normal operation with zero data loss for any ACK'd write.

---

## What to Build

### 9.1 RecoveryResult

The output of a successful recovery:

| Field | Type | Description |
|-------|------|-------------|
| `manifest` | `Manifest` | The loaded manifest |
| `levels` | `Vec<LevelState>` | Open SSTableHandles for all levels |
| `memtable_list` | `MemtableList` | Rebuilt memtable with replayed WAL entries |
| `next_sequence_number` | `u64` | The next sequence number to assign (max of manifest + WAL + 1) |
| `wal_entries_replayed` | `usize` | Number of entries replayed from WAL |

### 9.2 Recovery Function

| Method | Signature | Behavior |
|--------|-----------|----------|
| `recover` | `async (backend: &B, wal_dir: &Path, namespace: &str, config: &RecoveryConfig, fetcher: &dyn BlockFetcher) -> FlushResult<RecoveryResult>` | Full recovery procedure (see 9.3) |

### 9.3 Recovery Procedure

**Step 1: Load manifest**
- Create `ManifestManager::new(backend, namespace, config)`
- Call `manifest_manager.load_latest()` to discover and load the current manifest
- If no manifests exist: this is a fresh partition, create empty manifest

**Step 2: Rebuild in-memory SSTable state**
- For each level (L0-L3):
  - For each `SSTableMeta` in the manifest's level:
    - Construct the StorageBackend path from `sst_path(namespace, level)`
    - Call `SSTableHandle::open(meta, path, fetcher)` to load bloom filter, index block, and footer
  - Build `LevelState` for the level
- Open SSTables concurrently within each level (all L0 in parallel, all L1 in parallel, etc.)

**Step 3: Replay WAL**
- Call `WalManager::recover_from(wal_dir, manifest.last_flushed_sequence)` to get WAL entries with sequence > `last_flushed_sequence`
- These are entries that were written to the WAL but not yet flushed to an SSTable
- Create a fresh `MemtableList` with starting sequence = `manifest.last_flushed_sequence + 1`
- Insert each replayed entry into the memtable via `memtable_list.insert(entry)`
- Track the highest sequence number seen

**Step 4: Compute next sequence number**
- `next_sequence_number = max(manifest.last_flushed_sequence, max_wal_sequence) + 1`
- If no WAL entries replayed: `next_sequence_number = manifest.last_flushed_sequence + 1`

**Step 5: Return RecoveryResult**

### 9.4 RecoveryConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `manifest_config` | `ManifestConfig` | Default | Manifest configuration |
| `memtable_config` | `MemtableConfig` | Default | Memtable configuration for rebuilt memtable |

### 9.5 Per-Partition Recovery

Recovery is designed to be **per-partition and independent** — no global state is shared between partitions during recovery. This is critical for Phase 7 where the server recovers N partitions in parallel.

The `recover` function takes a single partition's WAL directory and namespace. The Engine orchestrator (Task 12) calls this for each partition independently.

### 9.6 Idempotency

Recovery is idempotent:
- WAL entries that were already flushed (sequence ≤ `last_flushed_sequence`) are filtered out by `WalManager::recover_from`
- Replaying the same WAL entries into a fresh memtable produces the same state
- The manifest is immutable (read-only during recovery) — no side effects

### 9.7 Error Handling

| Error Case | Behavior |
|------------|----------|
| Manifest not found on StorageBackend | Fresh partition — start with empty manifest, replay all WAL entries |
| Manifest JSON corrupted | Return `FlushError::CorruptedData` — manual intervention required |
| SSTable not found on StorageBackend (referenced by manifest) | Return `FlushError::NotFound` — the manifest references an SSTable that's been GC'd prematurely or lost |
| WAL segment corrupted | `WalManager::recover_from` handles partial segments — recovers entries up to the corruption point, logs warning |
| WAL directory doesn't exist | No WAL entries to replay — partition starts with only flushed data |

---

## Tests

**File:** `crates/flushdb-engine/tests/recovery_tests.rs`

### Fresh Partition Tests
| Test | What It Validates |
|------|-------------------|
| `test_recover_fresh_partition_no_manifest_no_wal` | No manifest, no WAL → empty state, sequence 1 |
| `test_recover_fresh_partition_no_manifest_with_wal` | No manifest but WAL has entries → memtable populated from WAL |

### Manifest-Only Recovery Tests
| Test | What It Validates |
|------|-------------------|
| `test_recover_manifest_only_no_wal` | Manifest with SSTables, no WAL → SSTables loaded, empty memtable |
| `test_recover_manifest_loads_all_levels` | Manifest with SSTables at L0, L1, L2 → all levels loaded with correct handles |
| `test_recover_manifest_bloom_filters_loaded` | After recovery, bloom filter checks work on recovered SSTableHandles |
| `test_recover_manifest_index_blocks_loaded` | After recovery, index lookups work on recovered SSTableHandles |

### WAL Replay Tests
| Test | What It Validates |
|------|-------------------|
| `test_recover_replays_wal_entries_above_flushed_sequence` | WAL has entries at seq 100-200, manifest.last_flushed_sequence = 150 → only 151-200 replayed |
| `test_recover_wal_entries_in_memtable` | Replayed entries are queryable via `memtable_list.get()` |
| `test_recover_preserves_entry_order` | Replayed entries maintain correct sort order in memtable |
| `test_recover_includes_tombstones` | Tombstone entries in WAL are replayed into memtable |
| `test_recover_next_sequence_number` | `next_sequence_number` equals max(all sequences) + 1 |

### End-to-End Recovery Tests
| Test | What It Validates |
|------|-------------------|
| `test_recover_full_state` | Manifest + WAL → can read both flushed (SSTable) and unflushed (memtable) data |
| `test_recover_after_flush_then_crash` | Write → flush → write more → crash → recover → all data accessible |
| `test_recover_idempotent` | Running recovery twice produces identical state |

### Error Handling Tests
| Test | What It Validates |
|------|-------------------|
| `test_recover_corrupted_manifest` | Corrupted JSON → `CorruptedData` error |
| `test_recover_missing_sstable` | Manifest references nonexistent SSTable → `NotFound` error |
| `test_recover_no_wal_dir` | WAL directory missing → recovery succeeds with manifest-only state |

---

## Done When

- [ ] Recovery loads latest manifest from StorageBackend
- [ ] All SSTableHandles opened with bloom filters and index blocks in memory
- [ ] WAL entries with sequence > `last_flushed_sequence` replayed into memtable
- [ ] Next sequence number correctly computed from max of manifest + WAL
- [ ] Recovery is per-partition with no shared global state
- [ ] Recovery is idempotent — running twice produces same state
- [ ] Fresh partition (no manifest, no WAL) starts cleanly
- [ ] Error handling covers corrupted manifest, missing SSTables, missing WAL
- [ ] All tests pass
