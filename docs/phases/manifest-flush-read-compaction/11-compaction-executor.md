# Task 11: Compaction Executor

**Crate:** `flushdb-engine`
**File:** `src/compaction/executor.rs`
**Depends on:** Task 1 (Manifest types), Task 2 (ManifestManager), Task 4 (BlockFetcher, SSTableHandle), Task 5 (MergeIterator), Task 10 (CompactionTask)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §10.2 (SSTable Run Fragments), §10.3 (Compaction Merge Process), §10.4 (Tombstone Lifecycle)

---

## Goal

Implement the compaction executor that takes a `CompactionTask` and merges input SSTables into new output SSTables at the target level. This includes the merge process, fragment-based output, trivial move optimization, tombstone lifecycle management, and atomic manifest update. Compaction is the key mechanism that bounds L0 size, reclaims space from deleted keys, and organizes data for efficient reads.

---

## What to Build

### 11.1 CompactionExecutor

```
CompactionExecutor<B: StorageBackend>
```

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `sst_writer` | `SSTableWriter` | Reuse existing SSTable writer |
| `config` | `CompactionConfig` | Compaction configuration |
| `namespace` | `String` | Namespace for path construction |
| `base_path` | `String` | Base path prefix |

**Methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(config: CompactionConfig, namespace: String, base_path: String) -> Self` | Creates executor |
| `execute` | `async (&self, task: CompactionTask, manifest_manager: &mut ManifestManager<B>, backend: &B, fetcher: &dyn BlockFetcher) -> FlushResult<CompactionResult>` | Full compaction execution (see 11.3) |

### 11.2 CompactionResult

| Field | Type | Description |
|-------|------|-------------|
| `output_sstables` | `Vec<SSTableMeta>` | New SSTables written to target level |
| `removed_sstable_ids` | `Vec<String>` | IDs of consumed input SSTables (for cache eviction in P6) |
| `trivial_moves` | `usize` | Count of SSTables moved by manifest-only update |
| `entries_written` | `u64` | Total entries in output |
| `entries_dropped` | `u64` | Entries dropped (expired tombstones, superseded values) |
| `bytes_read` | `u64` | Total bytes read from input SSTables |
| `bytes_written` | `u64` | Total bytes written to output SSTables |

### 11.3 Compaction Execution Sequence

**Step 1: Check for trivial moves**
- For each input SSTable from the source level:
  - If its key range does NOT overlap with ANY SSTable at the target level:
    - This is a trivial move — update manifest only, no I/O
    - Remove from input list, add to trivial move list
- If all inputs are trivial moves: update manifest in one CAS and return

**Step 2: Open input SSTables**
- For each remaining input SSTable (source + overlapping target):
  - Open `SSTableHandle` via `SSTableHandle::open(meta, path, fetcher)`
  - Scan all entries from each handle

**Step 3: Build MergeIterator**
- Create `MergeSource` for each input SSTable's entries
- Source IDs ordered by: source level inputs first (newer), then target level inputs (older)
- Within same level: ordered by most recent `sequence_range` first
- Create `MergeIterator` from all sources

**Step 4: Merge and filter**
- Iterate via `next_deduped()`:
  - **Duplicate keys:** Already handled by dedup (highest sequence wins)
  - **Point tombstones at bottom level:** If `target_level.is_bottom()` and tombstone is expired (see 11.4), drop both the tombstone and covered entry
  - **Point tombstones at non-bottom level:** Keep the tombstone — older entries may exist at deeper levels
  - **Range tombstones:** Propagate to output if they still potentially cover keys at deeper levels (see 11.5)

**Step 5: Build output SSTable fragments**
- Generate a run_id: `run-{ulid}`
- Write entries into SSTable fragments:
  - Each fragment targets `target_fragment_size` bytes
  - When current fragment reaches the size target, finalize it and start a new one
  - Each fragment gets its own bloom filter, index block, footer
  - Upload each fragment as completed: `{base}/{ns}/sstables/{level}/{run_id}/frag-{index:04}.sst`
- Collect `SstInfo` for each fragment, convert to `SSTableMeta` with `run_id` and `fragment_index`

**Step 6: Update manifest (CAS)**
- Create `ManifestUpdate` with:
  - `trigger = ManifestUpdateTrigger::Compaction`
  - `add_sstables`: all output fragments + trivial-move SSTables (at target level)
  - `remove_sstables`: all input SSTables from source level + consumed target SSTables
  - `compactor_epoch = manifest_manager.compactor_epoch`
- Call `manifest_manager.update(update)`

**Step 7: Mark old SSTables for deferred deletion**
- Return `removed_sstable_ids` in the result so the Engine can schedule background deletion of old SSTable files
- Do NOT delete old SSTables immediately — they may still be in use by concurrent reads

### 11.4 Tombstone TTL

Point tombstones are eligible for deletion when:
1. The compaction target is the **bottom level** (L3), AND
2. The tombstone's age exceeds `tombstone_ttl` (default 7 days)

**Age calculation:**
- Tombstone age = `now_ms - tombstone_created_at_ms`
- `tombstone_created_at_ms` comes from the SSTable metadata's `created_at_ms` field (the SSTable that first contained this tombstone)
- Add jitter: per-tombstone random delay of 0-24 hours, seeded from hash of the composite key (deterministic, stable across retries)

**When a tombstone is dropped:**
- The tombstone entry itself is not written to the output
- Any covered Put entries with lower sequence numbers are also dropped

### 11.5 Range Tombstone Watermark Protocol

Range tombstones require special handling during compaction:

1. **Propagation:** Range tombstones at non-bottom levels must be copied to the output — they may still cover entries at deeper levels
2. **Watermark update:** When compaction at level `Li` completes, update `tombstone_compaction_watermarks[Li]` to `min(oldest_tombstone_created_at in compaction input)`
3. **Deletion eligibility at bottom level:** A range tombstone at `Li` is eligible for deletion only when `tombstone_compaction_watermarks[Lj] >= tombstone.created_at` for ALL levels `Lj > Li`
4. **Partial coverage:** Range tombstones are copied into each compaction output fragment that overlaps their range

### 11.6 Trivial Move Optimization

When an input SSTable's key range does NOT overlap with any SSTable at the target level:
- No data needs to be read or written
- Only the manifest is updated: remove from source level, add to target level (with updated level metadata)
- The SSTable file stays at its current StorageBackend path (no file rename needed — the manifest tracks the path)

For time-ordered keys, most compactions are trivial moves because newer data has non-overlapping key ranges.

### 11.7 Delete-Only Compaction

If an SSTable's **entire key range** is covered by a range tombstone with a higher sequence number:
- Remove the SSTable from the manifest without reading it — pure metadata operation
- Check this during Step 1 alongside trivial move detection

---

## Tests

**File:** `crates/flushdb-engine/tests/compaction_executor_tests.rs`

### L0→L1 Compaction Tests
| Test | What It Validates |
|------|-------------------|
| `test_l0_to_l1_basic` | 5 L0 SSTables → compacted into L1 fragments, L0 cleared in manifest |
| `test_l0_to_l1_dedup` | Same key in multiple L0 SSTables → only newest sequence in output |
| `test_l0_to_l1_with_existing_l1` | L0 compacted with overlapping L1 SSTables → merged output |
| `test_l0_to_l1_no_l1_overlap` | L0 key range doesn't overlap L1 → output goes to L1 cleanly |

### Level-to-Level Compaction Tests
| Test | What It Validates |
|------|-------------------|
| `test_l1_to_l2_basic` | One L1 SSTable + overlapping L2 SSTables → merged into L2 fragments |
| `test_l1_to_l2_partial_overlap` | Non-overlapping L2 SSTables left in place |

### Trivial Move Tests
| Test | What It Validates |
|------|-------------------|
| `test_trivial_move_no_overlap` | Input SSTable doesn't overlap target → manifest-only update, no I/O |
| `test_trivial_move_mixed` | Some inputs trivially movable, others need merge → both handled correctly |
| `test_trivial_move_count_in_result` | `CompactionResult.trivial_moves` reflects correct count |

### Fragment Output Tests
| Test | What It Validates |
|------|-------------------|
| `test_output_fragments_at_size_boundary` | Large compaction output → split into multiple fragments at target size |
| `test_fragment_paths` | Fragments written to `sstables/{level}/{run_id}/frag-{index}.sst` |
| `test_each_fragment_has_bloom_and_index` | Each output fragment has its own bloom filter and index |
| `test_fragments_non_overlapping` | Output fragments have non-overlapping key ranges |

### Tombstone Tests
| Test | What It Validates |
|------|-------------------|
| `test_tombstone_preserved_at_non_bottom_level` | Delete tombstone at L1 compaction → kept in output |
| `test_tombstone_dropped_at_bottom_level_when_expired` | Delete tombstone at L3 + TTL expired → dropped |
| `test_tombstone_kept_at_bottom_level_when_not_expired` | Delete tombstone at L3 + TTL not expired → kept |
| `test_tombstone_drops_covered_put` | Point tombstone at higher seq → covered Put not in output |
| `test_range_tombstone_propagated` | Range tombstone at non-bottom level → appears in output |
| `test_range_tombstone_watermark_updated` | After compaction, watermark updated in manifest |

### Manifest Update Tests
| Test | What It Validates |
|------|-------------------|
| `test_manifest_updated_after_compaction` | Input SSTables removed, output SSTables added |
| `test_manifest_epoch_fencing` | Higher compactor_epoch in manifest → `EpochFenced` |
| `test_manifest_cas_conflict_retry` | Concurrent manifest update → retry succeeds |

### Delete-Only Compaction Tests
| Test | What It Validates |
|------|-------------------|
| `test_delete_only_compaction` | SSTable entirely covered by range tombstone → removed without reading |

### Statistics Tests
| Test | What It Validates |
|------|-------------------|
| `test_result_entries_written_count` | `entries_written` matches output entry count |
| `test_result_entries_dropped_count` | `entries_dropped` reflects dedup + tombstone drops |
| `test_result_removed_ids` | `removed_sstable_ids` lists all consumed input IDs |

---

## Done When

- [ ] L0→L1 compaction merges all L0 SSTables with overlapping L1 range
- [ ] Ln→Ln+1 compaction works for L1→L2, L2→L3
- [ ] Trivial move optimization skips I/O for non-overlapping inputs
- [ ] Output split into fragments at size boundaries
- [ ] Each fragment has its own bloom filter, index, footer
- [ ] Tombstones preserved at non-bottom levels, dropped at bottom when TTL expired
- [ ] Range tombstones propagated with watermark protocol
- [ ] Manifest atomically updated via CAS (inputs removed, outputs added)
- [ ] Epoch fencing prevents zombie compactors
- [ ] Delete-only compaction removes fully-tombstoned SSTables without reading
- [ ] CompactionResult includes consumed SSTable IDs for cache eviction (P6)
- [ ] Compaction instrumented with `tracing` spans for future metrics
- [ ] All tests pass
