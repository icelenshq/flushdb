# Task 1: Arena Allocator

**Crate:** `flushdb-engine`
**File:** `src/arena.rs`
**Depends on:** Nothing
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §6.2

---

## Goal

Build a bump allocator that owns contiguous memory blocks for skip list node allocation. The arena provides fast, cache-friendly allocation with O(1) bulk deallocation when the memtable is dropped after SSTable flush. No per-entry freeing, no atomics — single-writer model.

---

## What to Build

### 1.1 Constants

```
DEFAULT_BLOCK_SIZE: usize = 1_048_576   // 1 MB
```

### 1.2 Arena Struct

```
Arena {
    blocks: Vec<Vec<u8>>        // 1 MB blocks (Vec<u8> instead of Box<[u8; N]> for flexible sizing)
    current_offset: usize       // offset within current block (plain integer — single-writer)
    total_allocated: usize      // running total across all blocks — used for freeze threshold
    block_size: usize           // configurable block size (default 1 MB)
}
```

**Design decisions:**
- `Vec<Vec<u8>>` instead of `Box<[u8; 1_048_576]>` — allows configurable block sizes in tests (small blocks for edge case testing) without const generics
- `current_offset` and `total_allocated` are plain `usize` — no atomics needed in single-writer model
- Blocks are never freed individually — the entire `Arena` is dropped at once when the memtable is released
- Arena does NOT implement `Clone` — each memtable gets its own arena, and arenas are never shared

### 1.3 Methods

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `() -> Self` | Creates arena with default 1 MB block size, allocates first block |
| `with_block_size` | `(block_size: usize) -> Self` | Creates arena with custom block size (for testing). Panics if `block_size == 0`. |
| `allocate` | `(&mut self, size: usize) -> *mut u8` | Bump-allocates `size` bytes from current block. If current block can't fit, allocates a new block. Returns raw pointer to the start of the allocated region. Panics if `size > block_size` (single allocation can't span blocks). |
| `total_allocated` | `(&self) -> usize` | Returns running total of bytes allocated — used by memtable freeze threshold check. |
| `block_count` | `(&self) -> usize` | Returns number of blocks allocated — useful for diagnostics. |
| `reset` | `(&mut self)` | Drops all blocks and resets state. Used when recycling arenas (future optimization). |

### 1.4 Allocation Protocol

```
allocate(size):
  if current_offset + size <= block_size:
    ptr = &blocks[last][current_offset]
    current_offset += size
    total_allocated += size
    return ptr
  else:
    // Current block can't fit — allocate new block
    // For oversized allocations (size > block_size), allocate a block of exactly `size`
    new_block_size = max(block_size, size)
    blocks.push(vec![0u8; new_block_size])
    current_offset = size
    total_allocated += size
    return &blocks[last][0]
```

**Oversized allocation handling:** If a single allocation request exceeds `block_size`, allocate a dedicated block of exactly that size. This handles the rare case of very large keys or values without failing.

### 1.5 Safety Considerations

- `allocate` returns `*mut u8` — callers must ensure the pointer is used within the arena's lifetime. This is safe because the skip list and arena share the same lifetime (both owned by the memtable).
- The arena never moves blocks after allocation — `Vec<Vec<u8>>` guarantees that existing block contents don't move when new blocks are appended (inner `Vec<u8>` is heap-allocated).
- No `unsafe` code in the arena itself. The `*mut u8` return type is used by the skip list (which will need `unsafe` for pointer operations — but per project rules, we must find safe alternatives). See Task 2 for the safe skip list design.

### 1.6 Future Phase Considerations

- **Shard-per-core scheduler (future):** Arena memory is pinned to the owning core. Keep allocations core-local — no shared arenas across memtables.
- **Flush pipeline (Phase 5b):** When the memtable freezes, the arena goes with it. The arena must be `Send` so the frozen memtable can move to a flush task. `Vec<Vec<u8>>` is `Send`.

---

## Tests

**File:** `crates/flushdb-engine/tests/arena_tests.rs`

### Basic Allocation Tests
| Test | What It Validates |
|------|-------------------|
| `test_arena_new_starts_with_one_block` | New arena has exactly 1 block, 0 total_allocated |
| `test_arena_allocate_returns_valid_pointer` | Allocate N bytes, write to pointer, read back correctly |
| `test_arena_allocate_increments_total` | After allocating 100 bytes, total_allocated == 100 |
| `test_arena_multiple_allocations_same_block` | Multiple small allocations stay in same block |
| `test_arena_block_boundary_triggers_new_block` | Allocation that exceeds current block capacity triggers new block |

### Block Management Tests
| Test | What It Validates |
|------|-------------------|
| `test_arena_new_block_on_overflow` | When current block is full, next allocation creates new block |
| `test_arena_block_count_increments` | block_count increases as blocks are added |
| `test_arena_total_allocated_across_blocks` | total_allocated is sum across all blocks, not just current |
| `test_arena_custom_block_size` | `with_block_size(4096)` creates 4 KB blocks |
| `test_arena_zero_block_size_panics` | `with_block_size(0)` panics |

### Oversized Allocation Tests
| Test | What It Validates |
|------|-------------------|
| `test_arena_oversized_allocation` | Single allocation larger than block_size succeeds with dedicated block |
| `test_arena_oversized_then_normal` | After oversized allocation, normal allocations work in fresh block |

### Reset Tests
| Test | What It Validates |
|------|-------------------|
| `test_arena_reset_clears_state` | After reset, total_allocated == 0, block_count == 1 (fresh block) |
| `test_arena_reset_then_allocate` | Allocations work normally after reset |

### Send Bound Test
| Test | What It Validates |
|------|-------------------|
| `test_arena_is_send` | `fn assert_send<T: Send>() {}; assert_send::<Arena>();` compiles |

---

## Done When

- [ ] Arena allocates from contiguous blocks with bump pointer
- [ ] New block allocated when current block is exhausted
- [ ] Oversized allocations get dedicated blocks
- [ ] `total_allocated` accurately tracks all bytes across all blocks
- [ ] `reset` drops all blocks and reinitializes
- [ ] Arena is `Send` (for frozen memtable transfer to flush task)
- [ ] No `unsafe` code in arena implementation
- [ ] All tests pass
