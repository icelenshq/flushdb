# Task 6: Partition Instance & Lifecycle

**Crate:** `flushdb-server`
**File:** `src/partition.rs`
**Depends on:** Task 3 (NamespaceConfig)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §4 (Shard-Per-Core), Phase 7 §4

---

## Goal

Define the `Partition` struct that wraps an `Engine<B>` instance with partition-specific identity and lifecycle management. Each partition is an independently startable/stoppable unit with its own WAL directory, manifest, and compaction lifecycle. The lifecycle design supports future partition migration between nodes.

---

## What to Build

### 6.1 PartitionState Enum

```
PartitionState {
    Starting,
    Active,
    Frozen,       // accepting reads, rejecting writes (pre-drain)
    Draining,     // completing in-flight requests
    Stopped,
}
```

**Derives:** `Clone, Copy, Debug, PartialEq, Eq`

State transitions:
```
Starting → Active → Frozen → Draining → Stopped
                  ↘ Stopped (forced shutdown)
```

### 6.2 Partition Struct

```
Partition<B: StorageBackend> {
    partition_id: u32,
    namespace:    String,
    engine:       Engine<B>,
    state:        PartitionState,
    wal_dir:      PathBuf,
    created_at:   Instant,
}
```

**Note:** The `Engine<B>` already encapsulates the WAL, memtable list, manifest, cache, flush pipeline, and compaction scheduler internally. The `Partition` struct adds identity, state, and lifecycle coordination on top.

### 6.3 Construction

| Method | Signature | Behavior |
|--------|-----------|----------|
| `open` | `async (partition_id: u32, namespace: String, backend: B, base_data_dir: &Path, config: &NamespaceConfig) -> FlushResult<Self>` | Creates WAL directory at `{base_data_dir}/{namespace}/partition-{partition_id}/wal/`, opens Engine with backend and EngineConfig derived from NamespaceConfig. Sets state to `Active`. |

**WAL directory convention:**
```
{base_data_dir}/{namespace}/partition-{partition_id:04}/wal/
```

### 6.4 Lifecycle Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `freeze` | `(&mut self) -> FlushResult<()>` | Transitions `Active → Frozen`. Rejects if not `Active`. After freeze, `put`/`delete` return `FlushError::ResourceExhausted`. Reads continue. |
| `drain` | `async (&mut self) -> FlushResult<()>` | Transitions `Frozen → Draining`. Flushes active memtable. Waits for in-flight compactions to complete. |
| `stop` | `async (&mut self) -> FlushResult<()>` | Transitions to `Stopped`. Calls `engine.close()`. Usable from any state for forced shutdown. |
| `state` | `(&self) -> PartitionState` | Returns current state |
| `is_writable` | `(&self) -> bool` | Returns `true` only in `Active` state |
| `is_readable` | `(&self) -> bool` | Returns `true` in `Active` or `Frozen` states |

### 6.5 Engine Delegation

Forward engine operations with state checks:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `put` | `async (&mut self, record_id: &[u8], item_key: &[u8], value: Bytes, metadata: Bytes, idempotency_token: IdempotencyToken) -> FlushResult<u64>` | Checks `is_writable()`, delegates to `engine.put()` |
| `delete` | `async (&mut self, record_id: &[u8], item_key: &[u8]) -> FlushResult<u64>` | Checks `is_writable()`, delegates to `engine.delete()` |
| `delete_range` | `async (&mut self, record_id: &[u8], start_key: &[u8], end_key: &[u8]) -> FlushResult<u64>` | Checks `is_writable()`, delegates to `engine.delete_range()` |
| `get` | `async (&mut self, record_id: &[u8], item_key: &[u8]) -> FlushResult<Option<GetResult>>` | Checks `is_readable()`, delegates to `engine.get()` |
| `scan` | `async (&mut self, record_id: &[u8], start_key: Option<&[u8]>, end_key: Option<&[u8]>, options: RangeReadOptions) -> FlushResult<RangeReadResult>` | Checks `is_readable()`, delegates to `engine.scan()` |
| `multi_get` | `async (&mut self, record_id: &[u8], keys: &[&[u8]]) -> FlushResult<Vec<Option<GetResult>>>` | Checks `is_readable()`, delegates to `engine.multi_get()` |
| `maybe_flush` | `async (&mut self) -> FlushResult<Option<FlushResult_>>` | Delegates unconditionally (maintenance operation) |
| `maybe_compact` | `async (&mut self) -> FlushResult<Vec<CompactionResult>>` | Delegates unconditionally (maintenance operation) |

**State check failures:**
- Write to non-writable partition → `FlushError::ResourceExhausted { reason: "partition {id} is not accepting writes (state: {state})" }`
- Read from non-readable partition → `FlushError::ResourceExhausted { reason: "partition {id} is not accepting reads (state: {state})" }`

### 6.6 Status Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `partition_id` | `(&self) -> u32` | Returns partition ID |
| `namespace` | `(&self) -> &str` | Returns namespace name |
| `write_stall_status` | `(&self) -> WriteStallStatus` | Delegates to engine |
| `manifest_version` | `(&self) -> ManifestId` | Delegates to engine |
| `cache_stats` | `(&self) -> CacheStats` | Delegates to engine |

### 6.7 Design Decisions

- **Partition wraps Engine, does not own WAL/Memtable separately** — The phase 7 doc shows separate fields for wal, memtable, etc., but the Engine from phase 5 already encapsulates all of these. Duplicating ownership would break Engine's internal invariants. The Partition adds identity and lifecycle, not reimplemented internals.
- **State checks at Partition level, not Engine level** — Engine doesn't know about partition lifecycle. The Partition enforces read/write eligibility based on state.
- **`stop()` from any state** — Supports both graceful (Active → Frozen → Draining → Stopped) and forced (any → Stopped) shutdown paths.

---

## Tests

**File:** `crates/flushdb-server/tests/partition_tests.rs`

### Lifecycle Tests
| Test | What It Validates |
|------|-------------------|
| `test_open_partition_is_active` | Newly opened partition is in `Active` state |
| `test_freeze_transitions_state` | `freeze()` moves Active → Frozen |
| `test_freeze_rejects_non_active` | `freeze()` on Frozen/Draining/Stopped → error |
| `test_drain_transitions_state` | `drain()` moves Frozen → Draining |
| `test_stop_from_active` | `stop()` from Active → Stopped |
| `test_stop_from_frozen` | `stop()` from Frozen → Stopped |
| `test_stop_from_draining` | `stop()` from Draining → Stopped |

### Write State Guard Tests
| Test | What It Validates |
|------|-------------------|
| `test_put_active_succeeds` | `put()` on Active partition succeeds |
| `test_put_frozen_rejected` | `put()` on Frozen partition → `ResourceExhausted` |
| `test_put_stopped_rejected` | `put()` on Stopped partition → `ResourceExhausted` |
| `test_delete_active_succeeds` | `delete()` on Active partition succeeds |
| `test_delete_frozen_rejected` | `delete()` on Frozen partition → `ResourceExhausted` |

### Read State Guard Tests
| Test | What It Validates |
|------|-------------------|
| `test_get_active_succeeds` | `get()` on Active partition works |
| `test_get_frozen_succeeds` | `get()` on Frozen partition works (reads allowed) |
| `test_get_stopped_rejected` | `get()` on Stopped partition → `ResourceExhausted` |
| `test_scan_frozen_succeeds` | `scan()` on Frozen partition works |

### WAL Directory Tests
| Test | What It Validates |
|------|-------------------|
| `test_wal_directory_created` | Opening a partition creates the expected WAL directory |
| `test_wal_directory_convention` | WAL dir matches `{base}/{namespace}/partition-{id:04}/wal/` |

### Integration Tests
| Test | What It Validates |
|------|-------------------|
| `test_put_get_round_trip` | Write and read back through partition |
| `test_graceful_shutdown_sequence` | Active → freeze → drain → stop completes without error |
| `test_drain_flushes_memtable` | After drain, active memtable has been flushed |

---

## Done When

- [ ] `Partition` wraps `Engine<B>` with partition identity and state
- [ ] State transitions follow the defined state machine
- [ ] Writes rejected in non-Active states with `ResourceExhausted`
- [ ] Reads allowed in Active and Frozen states, rejected in Stopped
- [ ] WAL directory follows naming convention
- [ ] `stop()` callable from any state for forced shutdown
- [ ] Graceful shutdown sequence (freeze → drain → stop) works end-to-end
- [ ] All tests pass
