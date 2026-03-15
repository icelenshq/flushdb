# Task 3: Manifest Snapshots & Pruning

**Crate:** `flushdb-engine`
**File:** `src/manifest/snapshots.rs`
**Depends on:** Task 1 (Manifest types), Task 2 (ManifestManager)
**Estimated complexity:** S
**Design reference:** STORAGE_DESIGN.md §8.6 (Manifest Compaction)

---

## Goal

Implement periodic manifest snapshots and old version pruning. Without this, the manifest directory on StorageBackend grows unboundedly — one file per flush and compaction operation. Snapshots allow recovery to load a single file instead of replaying the full manifest history. Pruning deletes versions that are no longer needed.

---

## What to Build

### 3.1 Snapshot Logic in ManifestManager

Add snapshot awareness to ManifestManager's `update` method:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `should_snapshot` | `(&self) -> bool` | Returns true if: (1) `manifest_id % snapshot_interval == 0`, or (2) serialized manifest size > `max_manifest_size` |
| `mark_as_snapshot` | `(&mut self, manifest: &mut Manifest)` | Sets `manifest.is_snapshot = true` |

When `update` produces a new manifest and `should_snapshot()` is true, set `is_snapshot = true` on the manifest before writing. The manifest is already self-contained (full state), so the only difference is the flag — this tells `load_latest` that no delta-replay is needed.

### 3.2 Pruning

Pruning deletes old manifest versions that are no longer needed for rollback.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `prune_old_manifests` | `async (&self) -> FlushResult<usize>` | (1) List all manifests, (2) find the two most recent snapshots, (3) delete all versions older than the second-most-recent snapshot, (4) delete in batches of `pruning_batch_size`, (5) return count of deleted manifests. |

**Pruning safety rules:**
- Always retain the two most recent snapshot manifests (for rollback)
- Always retain all manifests newer than the second-most-recent snapshot
- Never delete the current manifest
- If fewer than 2 snapshots exist, do not prune anything

### 3.3 Snapshot-Aware Loading

Enhance `load_latest` from Task 2:

1. List all manifests
2. Find the highest ID — that's the current manifest
3. Load it. If `is_snapshot == true`, done.
4. If not a snapshot: scan backwards through the list to find the most recent snapshot
5. Load the snapshot, then load and apply each subsequent manifest as a delta
6. The final result is the fully materialized current state

**For Phase 5, the "delta apply" step is trivial** because each manifest version is already self-contained (contains full state). The snapshot flag optimization becomes meaningful when we add incremental manifests in future phases. For now, the loading logic should be correct but the fast path (latest is self-contained) always applies.

---

## Tests

**File:** `crates/flushdb-engine/tests/manifest_snapshots_tests.rs`

### Snapshot Tests
| Test | What It Validates |
|------|-------------------|
| `test_snapshot_created_at_interval` | After 100 updates, the 100th manifest has `is_snapshot = true` |
| `test_snapshot_forced_by_size` | Manifest exceeding `max_manifest_size` triggers snapshot regardless of interval |
| `test_non_snapshot_manifests` | Manifests between snapshots have `is_snapshot = false` |
| `test_snapshot_is_self_contained` | Loading a snapshot manifest directly (without delta replay) produces correct state |

### Pruning Tests
| Test | What It Validates |
|------|-------------------|
| `test_prune_deletes_old_versions` | With 3 snapshots (at 100, 200, 300), pruning deletes versions < 100 |
| `test_prune_retains_two_snapshots` | The two most recent snapshots survive pruning |
| `test_prune_retains_versions_between_snapshots` | Versions between the two most recent snapshots are retained |
| `test_prune_noop_with_fewer_than_two_snapshots` | With 0 or 1 snapshots, pruning deletes nothing |
| `test_prune_batch_size` | Pruning respects `pruning_batch_size` (deletes in batches) |
| `test_prune_never_deletes_current` | The current (highest) manifest is never deleted even if it predates snapshots |

### Load with Snapshots Tests
| Test | What It Validates |
|------|-------------------|
| `test_load_latest_snapshot_fast_path` | Latest manifest is a snapshot → loaded directly without scanning backwards |
| `test_load_latest_non_snapshot_with_snapshot_history` | Latest is not a snapshot → finds most recent snapshot, applies deltas |

---

## Done When

- [ ] Snapshots created every `snapshot_interval` manifest versions
- [ ] Size-based forced snapshots work when manifest exceeds `max_manifest_size`
- [ ] Pruning deletes all versions older than the second-most-recent snapshot
- [ ] Pruning always retains the two most recent snapshots and the current manifest
- [ ] Snapshot-aware loading works correctly
- [ ] Pruning respects batch size to avoid S3 DELETE throttling
- [ ] All tests pass
