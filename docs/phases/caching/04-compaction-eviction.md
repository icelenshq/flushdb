# Task 4: Compaction-Aware Cache Eviction

**Crate:** `flushdb-engine`
**File:** `src/cache/eviction.rs`
**Depends on:** Task 1 (BlockCache), Task 2 (CachingBlockFetcher), Task 3 (PinnedMetadataCache)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §12 (Compaction-aware eviction), Phase 6 §6 (Compaction-Aware Eviction)

---

## Goal

After compaction replaces old SSTables with new ones, cached blocks from the consumed SSTables become stale — they reference data that no longer exists in the active manifest. This task provides standalone eviction functions that operate on `BlockCache` and `PinnedMetadataCache` to batch-evict all cached data for consumed SSTables, preventing stale reads and freeing cache capacity for the new SSTables' blocks.

---

## What to Build

### 4.1 Eviction Functions

Standalone functions (not a struct) that coordinate eviction across cache layers. Engine owns `BlockCache` and `PinnedMetadataCache` directly — these functions accept references to both:

| Function | Signature | Behavior |
|----------|-----------|----------|
| `evict_sstable` | `(block_cache: &BlockCache, pinned_metadata: &mut PinnedMetadataCache, sst_id: &str)` | Removes all data blocks for the SSTable from `BlockCache` via `invalidate_sst`, and releases pinned metadata via `PinnedMetadataCache::release`. |
| `evict_sstables` | `(block_cache: &BlockCache, pinned_metadata: &mut PinnedMetadataCache, sst_ids: &[String])` | Batch eviction for multiple SSTables. Calls `BlockCache::invalidate_sst` for each ID, then `PinnedMetadataCache::release_batch` for all IDs at once. |
| `evict_compaction_result` | `(block_cache: &BlockCache, pinned_metadata: &mut PinnedMetadataCache, result: &CompactionResult)` | Convenience function that calls `evict_sstables` with `result.removed_sstable_ids`. This is the primary integration point called from `Engine::compact()`. |

### 4.2 Integration Point

In `Engine::compact()`, after `CompactionExecutor::execute()` returns a `CompactionResult` and the manifest is updated:

```
// Current code in Engine::compact():
let result = self.compaction_executor.execute(...).await?;
self.pending_deletions.extend(result.removed_sstable_ids.clone());
self.rebuild_levels().await?;

// After this task, add:
evict_compaction_result(&self.block_cache, &mut self.pinned_metadata, &result);
```

This integration is wired in Task 9 (Engine Integration). This task only builds the eviction functions.

### 4.3 Integration with ManifestManager (Future-Proofing)

The current integration is called explicitly from `Engine::compact()`. A cleaner future design would have `ManifestManager::update()` emit a `ManifestDelta` containing the list of removed SSTable IDs. For now, `CompactionResult::removed_sstable_ids` is the signal.

The functions are designed to accept any `&[String]` for SSTable IDs — they are not coupled to `CompactionResult`.

### 4.4 Design Decisions

- **Standalone functions, not a wrapper struct:** Since Engine owns both `BlockCache` and `PinnedMetadataCache` directly, there's no need for a struct that bundles them. Functions that accept references are simpler and avoid ownership gymnastics.
- **Eviction is asynchronous to the write path:** Eviction happens after compaction completes and the manifest is updated. There is a brief window where cached blocks from old SSTables could be served — but the read path checks the manifest's SSTable list, so stale SSTable handles are not consulted after `rebuild_levels()`. The eviction is about freeing memory, not correctness.
- **No locking between eviction and reads:** `BlockCache` (moka) is concurrent-safe. An ongoing read that already retrieved a cached block before eviction will use that block — this is safe because the read was initiated against a manifest version that included the old SSTable.
- **Batch eviction over individual:** Compaction often replaces 4-10 SSTables at once. `evict_sstables` batches the work and calls `release_batch` once for pinned metadata.

---

## Tests

**File:** `crates/flushdb-engine/tests/compaction_eviction_tests.rs`

### Single SSTable Eviction
| Test | What It Validates |
|------|-------------------|
| `test_evict_sstable_clears_block_cache` | Insert 5 blocks for SST "A" into BlockCache, evict_sstable, all 5 blocks return None on get |
| `test_evict_sstable_clears_pinned_metadata` | Pin metadata for SST "A", evict_sstable, pinned_metadata.get("A") returns None |
| `test_evict_sstable_preserves_other_data` | Insert blocks for SST "A" and "B", pin metadata for both, evict_sstable("A"), "B" data intact in both caches |
| `test_evict_nonexistent_sstable` | evict_sstable("unknown") — no panic, no side effects |

### Batch Eviction
| Test | What It Validates |
|------|-------------------|
| `test_evict_sstables_batch` | Insert blocks and metadata for SST "A", "B", "C", evict_sstables(["A", "B"]), only "C" remains |
| `test_evict_sstables_empty_list` | evict_sstables([]) — no panic, no side effects |
| `test_evict_sstables_partial_existence` | evict_sstables(["A", "unknown"]) — "A" evicted, no panic on "unknown" |

### CompactionResult Integration
| Test | What It Validates |
|------|-------------------|
| `test_evict_compaction_result` | Create CompactionResult with removed_sstable_ids ["X", "Y"], call evict_compaction_result, verify both evicted from both caches |

### Memory Reclamation
| Test | What It Validates |
|------|-------------------|
| `test_eviction_reduces_cache_size` | Fill BlockCache with blocks for 3 SSTables, measure weighted_size. Evict 1 SSTable, verify weighted_size decreased |
| `test_eviction_reduces_pinned_size` | Pin metadata for 3 SSTables, measure total_size_bytes. Evict 1, verify total_size_bytes decreased |

---

## Done When

- [ ] Eviction functions remove entries from both BlockCache and PinnedMetadataCache
- [ ] `evict_sstables` batch-evicts multiple SSTables efficiently
- [ ] `evict_compaction_result` integrates directly with `CompactionResult`
- [ ] Eviction of one SSTable does not affect other SSTables' cached data
- [ ] Cache sizes decrease after eviction
- [ ] All tests pass
