# Task 5: Merge Iterator

**Crate:** `flushdb-engine`
**File:** `src/merge_iterator.rs`
**Depends on:** Nothing (uses existing types: CompositeKey, BlockEntry, EntryType)
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §10.3 (Compaction Merge Process), §11.1 (Merge-Read Pattern)

---

## Goal

Build the core merge-sort iterator that combines entries from multiple sorted sources (memtables, SSTables) into a single sorted stream. This is the shared algorithm used by both the read path (merging across layers) and the compaction executor (merging input SSTables). The iterator handles sequence-number-based deduplication and tombstone awareness.

---

## What to Build

### 5.1 MergeEntry

A unified entry type that can represent entries from any source (memtable or SSTable):

| Field | Type | Description |
|-------|------|-------------|
| `composite_key` | `CompositeKey` | The entry's key |
| `value` | `Bytes` | Entry value (empty for tombstones) |
| `metadata` | `Bytes` | Entry metadata |
| `entry_type` | `EntryType` | Put, Delete, or RangeDelete |
| `sequence_number` | `u64` | Sequence number for ordering |

**Conversion methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `from_memtable_entry` | `(entry: &MemtableEntry) -> Self` | Converts a memtable entry (cloning the data) |
| `from_skip_node` | `(node: &SkipNode) -> Self` | Converts a skip list node |
| `from_block_entry` | `(entry: BlockEntry) -> Self` | Converts an SSTable block entry. For `EntryValue::Inline`, extracts the bytes. For `EntryValue::BlobRef`, stores the serialized blob ref (future phases handle blob reads). |
| `is_tombstone` | `(&self) -> bool` | Returns true for Delete or RangeDelete |
| `is_put` | `(&self) -> bool` | Returns true for Put |

### 5.2 MergeSource Trait

An abstraction over sorted entry sources:

```
trait MergeSource {
    fn peek(&self) -> Option<&MergeEntry>;
    fn advance(&mut self);
    fn source_id(&self) -> usize;
}
```

| Method | Signature | Behavior |
|--------|-----------|----------|
| `peek` | `(&self) -> Option<&MergeEntry>` | Returns the current entry without consuming it. None if exhausted. |
| `advance` | `(&mut self)` | Moves to the next entry |
| `source_id` | `(&self) -> usize` | A unique identifier for this source (used for ordering when keys are equal: lower source_id = newer data) |

### 5.3 VecSource

A simple `MergeSource` backed by a `Vec<MergeEntry>` — used for memtable entries and pre-fetched SSTable block entries.

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(entries: Vec<MergeEntry>, source_id: usize) -> Self` | Entries must be pre-sorted by CompositeKey. source_id determines priority (lower = newer). |

### 5.4 MergeIterator

The core k-way merge-sort iterator using a min-heap.

**Fields:**

| Field | Type | Description |
|-------|------|-------------|
| `heap` | `BinaryHeap<Reverse<HeapItem>>` | Min-heap ordered by (composite_key, source_id) |
| `sources` | `Vec<Box<dyn MergeSource>>` | Owned sources |

**HeapItem ordering:**
1. Primary: `composite_key` ascending (lexicographic byte comparison)
2. Secondary: `source_id` ascending (lower = newer source, checked first for dedup)

**Methods:**

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `(sources: Vec<Box<dyn MergeSource>>) -> Self` | Initializes heap with the first entry from each non-empty source |
| `next_entry` | `(&mut self) -> Option<MergeEntry>` | Returns the next entry in sorted order. Does NOT dedup — caller decides dedup policy. |
| `next_deduped` | `(&mut self) -> Option<MergeEntry>` | Returns the next entry, skipping entries with the same composite_key but lower sequence_number. For equal keys: keeps the entry with the highest sequence_number (newest wins). |
| `is_exhausted` | `(&self) -> bool` | Returns true if heap is empty |

### 5.5 Deduplication Rules

When `next_deduped` encounters entries with the same `composite_key`:
1. Keep the entry with the **highest `sequence_number`** (newest wins)
2. Skip all other entries with the same key
3. If the kept entry is a tombstone (`Delete`), still return it — the caller decides whether to suppress it (read path suppresses, compaction may propagate)

### 5.6 Source Ordering Convention

Sources are added in newest-to-oldest order:
- Source 0: Active memtable
- Source 1: Frozen memtable (most recent)
- Source 2: Frozen memtable (older)
- Source 3: L0 SSTable (most recent)
- Source 4: L0 SSTable (older)
- Source 5+: L1, L2, L3 SSTables

Lower `source_id` = newer data. When sequence numbers are equal (shouldn't happen in practice, but defensive), lower source_id wins.

---

## Tests

**File:** `crates/flushdb-engine/tests/merge_iterator_tests.rs`

### Basic Merge Tests
| Test | What It Validates |
|------|-------------------|
| `test_merge_single_source` | One source → entries returned in order |
| `test_merge_two_sorted_sources` | Two non-overlapping sources merge correctly |
| `test_merge_interleaved_keys` | Keys from multiple sources interleave correctly |
| `test_merge_empty_sources` | All-empty sources → iterator immediately exhausted |
| `test_merge_one_empty_one_full` | Empty source doesn't affect output |

### Deduplication Tests
| Test | What It Validates |
|------|-------------------|
| `test_dedup_same_key_different_sequences` | Same key, different sequences → highest wins |
| `test_dedup_same_key_newer_source_wins` | Same key, same sequence, lower source_id wins |
| `test_dedup_keeps_tombstone` | If newest entry for a key is a tombstone, it's returned (not suppressed) |
| `test_dedup_skips_older_put_after_delete` | Delete at seq 10 + Put at seq 5 → only Delete returned |
| `test_dedup_multiple_duplicates` | Same key appears in 5 sources → only newest returned |

### Sort Order Tests
| Test | What It Validates |
|------|-------------------|
| `test_merge_preserves_composite_key_order` | Output is strictly ascending by CompositeKey |
| `test_merge_same_record_different_items` | Items within same record sorted by item_key |
| `test_merge_cross_record_ordering` | All items for record "aaa" before any for "aab" |

### Multi-Source Tests
| Test | What It Validates |
|------|-------------------|
| `test_merge_five_sources` | Simulates active + frozen + 3 L0 SSTables |
| `test_merge_large_dataset` | 10 sources × 1000 entries each → correct merge |
| `test_merge_all_same_key` | Every source has the same key → only one entry returned via dedup |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_merge_single_entry_per_source` | Each source has exactly one entry |
| `test_merge_iterator_next_after_exhaustion` | Calling next on exhausted iterator returns None repeatedly |
| `test_merge_entry_from_memtable_entry` | Conversion from MemtableEntry preserves all fields |
| `test_merge_entry_from_block_entry` | Conversion from BlockEntry preserves all fields including inline value |

---

## Done When

- [ ] `MergeEntry` converts from both `MemtableEntry`/`SkipNode` and `BlockEntry`
- [ ] `MergeSource` trait and `VecSource` implementation work
- [ ] `MergeIterator` performs correct k-way merge-sort via min-heap
- [ ] `next_deduped` keeps highest-sequence entry per key
- [ ] Tombstones are returned (not suppressed) — caller handles tombstone policy
- [ ] Source ordering convention: lower source_id = newer data
- [ ] Merge output is strictly ascending by CompositeKey
- [ ] All tests pass
