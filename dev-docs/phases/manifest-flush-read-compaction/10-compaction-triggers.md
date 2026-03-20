# Task 10: Compaction Triggers & Write Stalling

**Crate:** `flushdb-engine`
**File:** `src/compaction/scheduler.rs`, `src/compaction/mod.rs`
**Depends on:** Task 1 (Manifest types, Level)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §10.1 (Compaction Triggers), Phase 5 §5e (Write Stalling)

---

## Goal

Implement the compaction trigger logic that determines when compaction is needed and the progressive write stalling mechanism that provides backpressure when L0 fills up. The scheduler examines the current manifest state and produces compaction tasks — it does not execute them (that's Task 11).

---

## What to Build

### 10.1 CompactionConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `l0_compaction_trigger` | `usize` | `4` | Trigger L0→L1 compaction when L0 has more than this many SSTables |
| `l0_slowdown_trigger` | `usize` | `8` | Throttle writes when L0 reaches this count |
| `l0_stop_trigger` | `usize` | `12` | Fully stall writes when L0 reaches this count |
| `l1_max_bytes` | `u64` | `256 * 1024 * 1024` | Max total bytes at L1 (256 MB) |
| `l2_max_bytes` | `u64` | `2_560 * 1024 * 1024` | Max total bytes at L2 (2.56 GB) |
| `l3_max_bytes` | `u64` | `25_600 * 1024 * 1024` | Max total bytes at L3 (25.6 GB) |
| `level_size_ratio` | `f64` | `10.0` | Size ratio between levels |
| `tombstone_ttl` | `Duration` | `7 days` | TTL for tombstones at bottom level |
| `tombstone_gc_interval` | `Duration` | `1 hour` | How often to check for expired tombstones |
| `target_fragment_size` | `u64` | `64 * 1024 * 1024` | Target fragment size for compaction output (64 MB) |

### 10.2 CompactionTask

Describes a compaction operation to be executed:

| Field | Type | Description |
|-------|------|-------------|
| `task_type` | `CompactionType` | Type of compaction |
| `source_level` | `Level` | Level to read from |
| `target_level` | `Level` | Level to write to |
| `input_sstables` | `Vec<SSTableMeta>` | SSTables to compact (from source level) |
| `target_sstables` | `Vec<SSTableMeta>` | Existing SSTables at target level that overlap with inputs |

**CompactionType enum:**

```
L0ToL1          — All L0 SSTables → L1
LevelToLevel    — One SSTable from Ln → Ln+1
TombstoneGC     — Bottom-level compaction to drop expired tombstones
```

### 10.3 CompactionScheduler

Examines the manifest and produces compaction tasks:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: CompactionConfig) -> Self` | Creates a scheduler |
| `check_triggers` | `(&self, manifest: &Manifest) -> Vec<CompactionTask>` | Examines manifest, returns all triggered compaction tasks. Checks in priority order: L0→L1 first, then L1→L2, then L2→L3, then tombstone GC. |
| `check_l0_trigger` | `(&self, manifest: &Manifest) -> Option<CompactionTask>` | Returns L0→L1 task if L0 count > l0_compaction_trigger |
| `check_level_trigger` | `(&self, manifest: &Manifest, level: Level) -> Option<CompactionTask>` | Returns Ln→Ln+1 task if level size exceeds max |
| `write_stall_status` | `(&self, manifest: &Manifest) -> WriteStallStatus` | Checks L0 count against stall thresholds |

### 10.4 WriteStallStatus

The write stalling signal returned by the scheduler:

```
enum WriteStallStatus {
    Normal,
    Slowdown { l0_count: usize, delay_ms: u64 },
    Stopped { l0_count: usize },
}
```

| Variant | Condition | Behavior |
|---------|-----------|----------|
| `Normal` | L0 count ≤ `l0_compaction_trigger` | Writes proceed at full speed |
| `Slowdown` | L0 count > `l0_slowdown_trigger` | Writes delayed. `delay_ms = (l0_count - l0_slowdown_trigger) * 1` |
| `Stopped` | L0 count ≥ `l0_stop_trigger` | Writes fully rejected with `ResourceExhausted` |

**Methods on WriteStallStatus:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `is_normal` | `(&self) -> bool` | True for Normal |
| `is_stopped` | `(&self) -> bool` | True for Stopped |
| `delay_ms` | `(&self) -> u64` | 0 for Normal, calculated for Slowdown, 0 for Stopped (irrelevant) |
| `l0_count` | `(&self) -> usize` | Current L0 count |

### 10.5 L0→L1 Input Selection

When L0 compaction is triggered:
1. Take ALL L0 SSTables as input (they overlap — must all be included)
2. Compute the combined key range of all L0 inputs
3. Find all L1 SSTables whose key range overlaps with the combined L0 range
4. These L1 SSTables become `target_sstables` in the CompactionTask

### 10.6 Ln→Ln+1 Input Selection

When level overflow is triggered:
1. Pick ONE SSTable from the overflowing level (the oldest by `created_at_ms`, or the one with the largest key range for best space reclamation)
2. Compute its key range
3. Find all SSTables at the target level whose key range overlaps
4. These become `target_sstables`

---

## Tests

**File:** `crates/flushdb-engine/tests/compaction_triggers_tests.rs`

### L0 Trigger Tests
| Test | What It Validates |
|------|-------------------|
| `test_l0_trigger_fires_at_threshold` | 5 L0 SSTables (threshold 4) → L0→L1 task returned |
| `test_l0_trigger_not_fired_below_threshold` | 3 L0 SSTables → no L0 task |
| `test_l0_trigger_includes_all_l0` | Task's input_sstables contains all L0 SSTables |
| `test_l0_trigger_finds_overlapping_l1` | L0 key range overlapping with 2 of 5 L1 SSTables → those 2 in target_sstables |
| `test_l0_trigger_no_l1_overlap` | L0 key range doesn't overlap any L1 → empty target_sstables |

### Level Trigger Tests
| Test | What It Validates |
|------|-------------------|
| `test_l1_trigger_fires_when_oversize` | L1 total > 256 MB → L1→L2 task |
| `test_l1_trigger_not_fired_below_max` | L1 total < 256 MB → no task |
| `test_l2_trigger_fires_when_oversize` | L2 total > 2.56 GB → L2→L3 task |
| `test_level_trigger_picks_oldest_input` | Selected input SSTable has earliest `created_at_ms` |
| `test_level_trigger_finds_overlapping_targets` | Input key range overlapping with target level SSTables → correct target set |

### Write Stall Tests
| Test | What It Validates |
|------|-------------------|
| `test_write_stall_normal` | 4 L0 SSTables → Normal |
| `test_write_stall_slowdown` | 9 L0 SSTables → Slowdown with delay_ms = 1 |
| `test_write_stall_slowdown_progressive` | 10 L0 → delay 2ms, 11 L0 → delay 3ms |
| `test_write_stall_stopped` | 12 L0 SSTables → Stopped |
| `test_write_stall_l0_count` | Status includes correct L0 count |

### Priority Tests
| Test | What It Validates |
|------|-------------------|
| `test_check_triggers_priority_order` | L0 compaction returned before L1 compaction |
| `test_check_triggers_multiple_levels` | Both L0 and L1 over threshold → both tasks returned |
| `test_check_triggers_no_triggers` | No levels over threshold → empty task list |

---

## Done When

- [ ] L0 trigger fires when L0 count exceeds threshold, includes all L0 SSTables
- [ ] Level overflow triggers fire for L1, L2 when size exceeds max
- [ ] Input selection correctly finds overlapping target SSTables
- [ ] Write stall status returns Normal/Slowdown/Stopped with correct thresholds
- [ ] Slowdown delay is progressive: `(l0_count - threshold) * 1ms`
- [ ] Stopped status at L0 count ≥ 12
- [ ] `check_triggers` returns tasks in priority order
- [ ] All tests pass
