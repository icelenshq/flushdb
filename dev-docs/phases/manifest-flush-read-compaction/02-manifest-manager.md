# Task 2: Manifest Manager

**Crate:** `flushdb-engine`
**File:** `src/manifest/manager.rs`
**Depends on:** Task 1 (Manifest types, ManifestId, path helpers)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §8.3 (Manifest Update Protocol), §8.4 (Epoch-Based Fencing), §8.7 (Concurrent Operations)

---

## Goal

Implement the ManifestManager that loads, saves, and atomically updates manifests via the CAS protocol on StorageBackend. This is the correctness backbone — every flush and compaction must go through CAS-protected manifest updates. Epoch-based fencing prevents zombie writers from corrupting state after ownership changes.

---

## What to Build

### 2.1 ManifestManager

A generic struct parameterized over `StorageBackend`:

```
ManifestManager<B: StorageBackend>
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `backend` | `B` | StorageBackend for manifest persistence |
| `current` | `Manifest` | In-memory copy of the current manifest |
| `config` | `ManifestConfig` | Configuration (base_path, snapshot_interval, etc.) |
| `namespace` | `String` | Namespace this manager operates on |
| `writer_epoch` | `u64` | This node's writer epoch |
| `compactor_epoch` | `u64` | This node's compactor epoch |

**Construction and loading methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(backend: B, namespace: String, config: ManifestConfig) -> Self` | Creates a manager with an empty manifest. Does NOT load from storage. |
| `load_latest` | `async (&mut self) -> FlushResult<&Manifest>` | Discovers and loads the latest manifest from StorageBackend. Lists `{base}/{namespace}/manifests/`, picks the highest ID, deserializes. If no manifests exist, stores the empty manifest as the initial version via `conditional_put`. Returns error if listing or deserialization fails. |
| `load_specific` | `async (&mut self, id: ManifestId) -> FlushResult<&Manifest>` | Loads a specific manifest version by ID. |
| `current` | `(&self) -> &Manifest` | Returns the in-memory manifest |

### 2.2 CAS Update Protocol

The core manifest update method implementing STORAGE_DESIGN.md §8.3:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `update` | `async (&mut self, update: ManifestUpdate) -> FlushResult<&Manifest>` | Full CAS protocol: (1) validate epoch, (2) apply update to current manifest, (3) serialize, (4) `conditional_put` to StorageBackend at next manifest ID, (5) on success: update in-memory `current`, (6) on `PreconditionFailed`: re-read latest, re-validate update, retry. Max 5 retries. |

**CAS protocol steps in detail:**

1. **Epoch validation** — before computing the new manifest:
   - If `update.trigger == Flush` and `self.current.writer_epoch > self.writer_epoch`: return `FlushError::EpochFenced`
   - If `update.trigger == Compaction` and `self.current.compactor_epoch > self.compactor_epoch`: return `FlushError::EpochFenced`

2. **Apply update** — call `update.apply(&self.current)` to produce the new manifest

3. **Serialize** — JSON encode the new manifest

4. **Conditional write** — `backend.conditional_put(manifest_path, serialized_bytes)`

5. **Handle result:**
   - `Ok(())` → update `self.current` to the new manifest, return success
   - `PreconditionFailed` → conflict detected:
     - Re-read latest manifest via `load_latest`
     - Re-validate: check that input SSTables from the update's `remove_sstables` still exist in the re-read manifest (they may have been consumed by another compaction)
     - If inputs are still valid: re-apply the update against the new base, retry CAS
     - If inputs are gone (consumed by concurrent operation): return `FlushError::InvalidArgument` with message explaining the conflict
   - Other errors: propagate

6. **Retry limit** — max 5 CAS attempts before returning `FlushError::ResourceExhausted`

### 2.3 Epoch Management

| Method | Signature | Behavior |
|--------|-----------|----------|
| `acquire_writer_epoch` | `async (&mut self) -> FlushResult<u64>` | Reads current manifest, increments writer_epoch, writes new manifest via CAS. Returns the acquired epoch. Sets `self.writer_epoch`. |
| `acquire_compactor_epoch` | `async (&mut self) -> FlushResult<u64>` | Same for compactor_epoch. |
| `check_writer_epoch` | `(&self) -> FlushResult<()>` | Returns `EpochFenced` if `self.current.writer_epoch > self.writer_epoch` |
| `check_compactor_epoch` | `(&self) -> FlushResult<()>` | Returns `EpochFenced` if `self.current.compactor_epoch > self.compactor_epoch` |

### 2.4 Refresh

| Method | Signature | Behavior |
|--------|-----------|----------|
| `refresh` | `async (&mut self) -> FlushResult<&Manifest>` | Re-reads the latest manifest from StorageBackend and updates `self.current`. Used after receiving gossip invalidation (future), or when CAS retry detects staleness. |

### 2.5 Manifest Discovery Protocol

The `load_latest` method implements this discovery:

1. Call `backend.list_prefix(manifest_prefix(base, namespace))`
2. Results are lexicographically sorted (StorageBackend contract)
3. If the list is empty: this is a fresh namespace, return the empty manifest
4. Otherwise, parse the last entry's filename to get the highest ManifestId
5. Call `backend.get(manifest_path(base, namespace, &highest_id))`
6. Deserialize the manifest
7. Validate `format_version == 1`

**Snapshot-aware loading** (interacts with Task 3):
- If the latest manifest has `is_snapshot == true`, it's self-contained — use it directly
- If not, scan backwards to find the most recent snapshot, load it, then apply all subsequent non-snapshot manifests on top
- For Phase 5, start simple: each manifest is self-contained (full state), not incremental. Snapshot optimization in Task 3 adds the delta-application logic.

### 2.6 Validation During Update

The `update` method must validate the ManifestUpdate before applying:

| Check | Error |
|-------|-------|
| `remove_sstables` contains an ID not present in current manifest at the specified level | `FlushError::InvalidArgument { message: "SSTable {id} not found at level {level}" }` |
| `add_sstables` contains an ID already present in current manifest | `FlushError::InvalidArgument { message: "SSTable {id} already exists at level {level}" }` |
| `new_last_flushed_sequence` is less than current `last_flushed_sequence` | `FlushError::InvalidArgument { message: "last_flushed_sequence cannot decrease" }` |

---

## Tests

**File:** `crates/flushdb-engine/tests/manifest_manager_tests.rs`

### Load Tests
| Test | What It Validates |
|------|-------------------|
| `test_load_latest_empty_namespace` | No manifests exist → creates and stores initial empty manifest |
| `test_load_latest_single_manifest` | One manifest on storage → loads it correctly |
| `test_load_latest_multiple_manifests` | Multiple versions → picks highest ManifestId |
| `test_load_specific_version` | Loads exact version by ID |
| `test_load_corrupted_manifest` | Invalid JSON on storage → `CorruptedData` error |

### CAS Update Tests
| Test | What It Validates |
|------|-------------------|
| `test_update_success` | Apply update → manifest stored at next ID, in-memory state updated |
| `test_update_increments_manifest_id` | After update, manifest_id = previous + 1 |
| `test_update_sets_previous_id` | `previous_manifest_id` points to the pre-update version |
| `test_update_add_l0_sstable` | Flush pattern: add one SSTable to L0 |
| `test_update_compaction_add_remove` | Compaction pattern: add to L1, remove from L0 |
| `test_update_updates_last_flushed_sequence` | Flush sets `last_flushed_sequence` to new value |

### Epoch Fencing Tests
| Test | What It Validates |
|------|-------------------|
| `test_acquire_writer_epoch` | Increments writer_epoch in manifest, returns new epoch |
| `test_flush_update_rejected_by_higher_epoch` | Writer epoch in manifest > our epoch → `EpochFenced` |
| `test_compaction_update_rejected_by_higher_epoch` | Compactor epoch in manifest > our epoch → `EpochFenced` |
| `test_epoch_check_passes_for_current` | Same epoch as manifest → passes |

### Conflict and Retry Tests
| Test | What It Validates |
|------|-------------------|
| `test_update_conflict_retry` | Simulate conflict by pre-writing the next manifest ID, verify retry succeeds at ID+2 |
| `test_update_conflict_inputs_consumed` | Conflict where a concurrent compaction already consumed our inputs → `InvalidArgument` error, no infinite retry |
| `test_update_max_retries_exhausted` | 5+ consecutive conflicts → `ResourceExhausted` |

### Validation Tests
| Test | What It Validates |
|------|-------------------|
| `test_update_rejects_removing_nonexistent_sstable` | Remove ID not in manifest → error |
| `test_update_rejects_adding_duplicate_sstable` | Add ID already in manifest → error |
| `test_update_rejects_decreasing_flushed_sequence` | `new_last_flushed_sequence < current` → error |

### Refresh Tests
| Test | What It Validates |
|------|-------------------|
| `test_refresh_picks_up_external_update` | Another writer updates manifest, refresh sees the new version |

---

## Done When

- [ ] `load_latest` discovers manifests via `list_prefix` and picks highest ID
- [ ] `update` implements full CAS protocol with `conditional_put`
- [ ] Epoch validation rejects zombie writers/compactors with `EpochFenced`
- [ ] Conflict retry re-reads, re-validates inputs, re-applies (up to 5 attempts)
- [ ] Consumed inputs during conflict are detected and reported, not retried forever
- [ ] `acquire_writer_epoch` / `acquire_compactor_epoch` work via CAS
- [ ] All validation checks produce specific error messages
- [ ] All tests pass
