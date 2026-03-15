# Task 2: Skip List Core

**Crate:** `flushdb-engine`
**File:** `src/skiplist.rs`
**Depends on:** Task 1 (Arena Allocator)
**Estimated complexity:** L
**Design reference:** STORAGE_DESIGN.md §6.1

---

## Goal

Implement the core skip list data structure — insert and point lookup — that serves as the memtable's sorted index. The skip list is single-owner (no concurrent access), uses arena-backed allocation for cache locality, and sorts entries by `(CompositeKey, sequence_number descending)` so the most recent version of any key is found first.

---

## What to Build

### 2.1 Constants

```
MAX_HEIGHT: usize = 12          // supports ~4 billion entries at p=1/4
BRANCHING_FACTOR: u32 = 4       // promotion probability = 1/BRANCHING_FACTOR = 1/4
```

### 2.2 Node Representation

Since the project forbids `unsafe` Rust, skip list nodes cannot use raw pointers. Use an index-based approach where nodes are stored in a `Vec` and linked by index:

```
SkipNode {
    key: CompositeKey
    sequence_number: u64
    value: Bytes                 // item value
    metadata: Bytes              // item metadata
    entry_type: EntryType
    height: usize                // number of levels this node participates in (1..=MAX_HEIGHT)
    next: [Option<usize>; MAX_HEIGHT]   // forward pointers as indices into the node vec
}
```

**Design decisions:**
- **Index-based linking** instead of raw pointers — each node has an index in a `Vec<SkipNode>`. Forward pointers are `Option<usize>` indices. This avoids all `unsafe` while preserving O(log n) skip list performance.
- **Fixed-size next array** (`[Option<usize>; MAX_HEIGHT]`) — avoids per-node heap allocation for the pointer array. MAX_HEIGHT=12 means 96 bytes per node for the next array, acceptable for a structure holding ~100K-1M entries.
- **`sequence_number` stored in node** — the skip list sorts by `(CompositeKey, sequence_number DESC)`. When the same CompositeKey appears multiple times, higher sequence numbers appear first during traversal.

### 2.3 SkipList Struct

```
SkipList {
    nodes: Vec<SkipNode>         // node storage — index 0 is the sentinel head
    head: usize                  // always 0 (sentinel node)
    height: usize                // current max height across all inserted nodes
    len: usize                   // number of entries (not counting sentinel)
    arena: Arena                 // arena for bulk memory tracking (total_allocated)
    rng: SmallRng                // deterministic RNG for height generation (from rand crate)
}
```

**Design decisions:**
- **Sentinel head node** at index 0 — simplifies insert/search logic by eliminating null-check for head. Created during `SkipList::new()` with a dummy key.
- **`SmallRng`** from `rand` crate — fast, non-cryptographic RNG for skip list height generation. Seeded from thread-local entropy.
- **Arena integration** — the arena tracks `total_allocated` for freeze threshold. Each insert adds the entry's approximate size to the arena. The arena's block allocator is not used for node storage (nodes live in `Vec<SkipNode>`), but the arena tracks the logical memory footprint.

### 2.4 Random Height Generation

```
random_height(rng) -> usize:
  height = 1
  while height < MAX_HEIGHT && rng.gen_range(0..BRANCHING_FACTOR) == 0:
    height += 1
  return height
```

Distribution: height 1 = 75%, height 2 = 18.75%, height 3 = 4.69%, ... height 12 = ~0.000006%.

### 2.5 Entry Ordering

The skip list orders entries by `(CompositeKey ASC, sequence_number DESC)`:

```
compare(a, b) -> Ordering:
  match a.key.cmp(&b.key):
    Less => Less
    Greater => Greater
    Equal => b.sequence_number.cmp(&a.sequence_number)   // DESC — higher seq first
```

**Why sequence_number DESC:** When scanning forward from a key, the most recent version (highest sequence number) appears first. Point lookups return the first match, which is automatically the newest.

### 2.6 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates empty skip list with sentinel head, default arena |
| `with_arena` | `(arena: Arena) -> Self` | Creates skip list with provided arena (for custom block sizes in tests) |
| `insert` | `(&mut self, entry: MemtableEntry)` | Inserts entry into skip list at correct sorted position. If an entry with the same `(CompositeKey, sequence_number)` already exists, the new entry replaces it (last-writer-wins within same sequence — should not happen in practice). Updates arena total_allocated. |
| `get` | `(&self, key: &CompositeKey) -> Option<&SkipNode>` | Point lookup — finds the node with the given CompositeKey and the highest sequence number. Returns `None` if key not found. |
| `contains_key` | `(&self, key: &CompositeKey) -> bool` | Returns true if any entry with this key exists |
| `len` | `(&self) -> usize` | Number of entries |
| `is_empty` | `(&self) -> bool` | True if no entries |
| `approximate_memory_usage` | `(&self) -> usize` | Returns arena's total_allocated — used for freeze threshold |

### 2.7 Insert Algorithm

```
insert(entry):
  height = random_height(rng)
  if height > self.height:
    self.height = height

  // Find predecessors at each level
  update = [head; MAX_HEIGHT]    // predecessors to splice after
  current = head
  for level in (0..self.height).rev():
    while let Some(next_idx) = nodes[current].next[level]:
      if compare(nodes[next_idx], new_entry) == Less:
        current = next_idx
      else:
        break
    update[level] = current

  // Create new node
  new_idx = nodes.len()
  nodes.push(SkipNode { key, seq, value, metadata, entry_type, height, next: [None; MAX_HEIGHT] })

  // Splice into each level
  for level in 0..height:
    nodes[new_idx].next[level] = nodes[update[level]].next[level]
    nodes[update[level]].next[level] = Some(new_idx)

  len += 1
  arena.total_allocated += entry_approximate_size
```

### 2.8 Point Lookup Algorithm

```
get(key):
  current = head
  for level in (0..self.height).rev():
    while let Some(next_idx) = nodes[current].next[level]:
      match nodes[next_idx].key.cmp(key):
        Less => current = next_idx
        Equal => // Found a match; continue down to find highest seq
        Greater => break

  // At level 0, scan forward for first match
  if let Some(next_idx) = nodes[current].next[0]:
    if nodes[next_idx].key == *key:
      return Some(&nodes[next_idx])   // highest seq due to DESC ordering

  None
```

### 2.9 Approximate Entry Size

For arena memory tracking, each entry's size is approximated as:

```
entry_size = size_of::<SkipNode>()
           + entry.composite_key.as_bytes().len()
           + entry.value.len()
           + entry.metadata.len()
```

This doesn't need to be exact — it's used for the 64 MB freeze threshold, which is a soft limit.

---

## Tests

**File:** `crates/flushdb-engine/tests/skiplist_tests.rs`

### Insert & Lookup Tests
| Test | What It Validates |
|------|-------------------|
| `test_insert_single_entry` | Insert one entry, get returns it |
| `test_insert_multiple_entries` | Insert 10 entries with different keys, all findable |
| `test_insert_duplicate_key_different_sequence` | Same CompositeKey with seq 1 and seq 5 — get returns seq 5 (highest) |
| `test_insert_overwrites_same_key_same_sequence` | Same (key, seq) pair — second insert replaces first |
| `test_get_nonexistent_key` | get on missing key returns None |
| `test_contains_key` | contains_key returns true for present, false for absent |

### Ordering Tests
| Test | What It Validates |
|------|-------------------|
| `test_entries_sorted_by_composite_key` | Iterate level-0 chain — keys are in ascending CompositeKey order |
| `test_same_key_sorted_by_sequence_desc` | Multiple versions of same key — highest sequence comes first at level 0 |
| `test_cross_record_ordering` | All entries for record "aaa" appear before all entries for record "aab" |

### Size Tracking Tests
| Test | What It Validates |
|------|-------------------|
| `test_len_increments` | len() increases by 1 per insert |
| `test_is_empty` | Empty skip list returns true, non-empty returns false |
| `test_approximate_memory_usage_increases` | approximate_memory_usage grows with inserts |

### Scale Tests
| Test | What It Validates |
|------|-------------------|
| `test_100k_entries_sorted_correctly` | Insert 100K random entries, iterate level-0 — verify sorted order matches BTreeMap oracle |
| `test_100k_entries_point_lookup` | Insert 100K entries, look up 1000 random keys — all found with correct highest sequence |
| `test_random_height_distribution` | Generate 10K heights — verify ~75% are height 1, <1% are height 5+ |

### Edge Cases
| Test | What It Validates |
|------|-------------------|
| `test_insert_with_empty_value` | DELETE entries (empty value) insert and retrieve correctly |
| `test_insert_with_max_length_key` | 256-byte record_id + 4096-byte item_key works |
| `test_single_entry_skiplist` | Skip list with one entry — get, len, is_empty all correct |

---

## Done When

- [ ] Insert places entries in correct sorted position `(CompositeKey ASC, sequence_number DESC)`
- [ ] Point lookup returns the entry with the highest sequence number for a given key
- [ ] 100K random entries maintain correct sorted order (verified against BTreeMap oracle)
- [ ] Arena tracks approximate memory usage for freeze threshold
- [ ] No `unsafe` code — index-based node linking only
- [ ] `len()`, `is_empty()`, `contains_key()` work correctly
- [ ] All tests pass
