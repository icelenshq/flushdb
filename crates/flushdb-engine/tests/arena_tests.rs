use flushdb_engine::arena::{Arena, ArenaSlice};

// === Basic Allocation Tests ===

#[test]
fn test_arena_new_starts_with_one_block() {
    let arena = Arena::new();
    assert_eq!(arena.block_count(), 1);
    assert_eq!(arena.total_allocated(), 0);
}

#[test]
fn test_arena_allocate_returns_valid_slice() {
    let mut arena = Arena::with_block_size(4096);
    let slice = arena.allocate(64);
    assert_eq!(slice.len(), 64);

    let pattern: Vec<u8> = (0..64).map(|i| (i * 3 + 7) as u8).collect();
    arena.write(&slice, &pattern);

    let readback = arena.read(&slice);
    assert_eq!(readback, pattern.as_slice());
}

#[test]
fn test_arena_allocate_increments_total() {
    let mut arena = Arena::with_block_size(4096);
    arena.allocate(100);
    assert_eq!(arena.total_allocated(), 100);
}

#[test]
fn test_arena_multiple_allocations_same_block() {
    let mut arena = Arena::with_block_size(4096);

    let s1 = arena.allocate(100);
    let s2 = arena.allocate(200);
    let s3 = arena.allocate(300);

    assert_eq!(arena.block_count(), 1);
    assert_eq!(arena.total_allocated(), 600);

    arena.write(&s1, &[0xAA; 100]);
    arena.write(&s2, &[0xBB; 200]);
    arena.write(&s3, &[0xCC; 300]);

    assert!(arena.read(&s1).iter().all(|&b| b == 0xAA));
    assert!(arena.read(&s2).iter().all(|&b| b == 0xBB));
    assert!(arena.read(&s3).iter().all(|&b| b == 0xCC));
}

#[test]
fn test_arena_block_boundary_triggers_new_block() {
    let mut arena = Arena::with_block_size(256);

    arena.allocate(200);
    assert_eq!(arena.block_count(), 1);

    // 200 + 100 = 300 > 256, so this triggers a new block
    arena.allocate(100);
    assert_eq!(arena.block_count(), 2);
    assert_eq!(arena.total_allocated(), 300);
}

// === Block Management Tests ===

#[test]
fn test_arena_new_block_on_overflow() {
    let mut arena = Arena::with_block_size(128);

    // Fill the block exactly
    let s1 = arena.allocate(128);
    assert_eq!(arena.block_count(), 1);

    // Next allocation must go to a new block
    let s2 = arena.allocate(1);
    assert_eq!(arena.block_count(), 2);

    // Verify both allocations hold independent data
    arena.write(&s1, &[0xFF; 128]);
    arena.write(&s2, &[0x01; 1]);
    assert!(arena.read(&s1).iter().all(|&b| b == 0xFF));
    assert_eq!(arena.read(&s2), &[0x01]);
}

#[test]
fn test_arena_block_count_increments() {
    let mut arena = Arena::with_block_size(64);

    assert_eq!(arena.block_count(), 1);
    arena.allocate(64); // fills block 0
    assert_eq!(arena.block_count(), 1);

    arena.allocate(1); // triggers block 1
    assert_eq!(arena.block_count(), 2);

    arena.allocate(64); // fills block 1, triggers block 2
    assert_eq!(arena.block_count(), 3);
}

#[test]
fn test_arena_total_allocated_across_blocks() {
    let mut arena = Arena::with_block_size(100);

    arena.allocate(80);
    arena.allocate(80); // crosses into block 2
    arena.allocate(80); // crosses into block 3

    assert_eq!(arena.total_allocated(), 240);
    assert!(arena.block_count() >= 3);
}

#[test]
fn test_arena_custom_block_size() {
    let mut arena = Arena::with_block_size(4096);
    assert_eq!(arena.block_count(), 1);
    assert_eq!(arena.total_allocated(), 0);

    // Should fit in a single 4096-byte block
    arena.allocate(2048);
    arena.allocate(2048);
    assert_eq!(arena.block_count(), 1);
    assert_eq!(arena.total_allocated(), 4096);

    // Next byte spills to a new block
    arena.allocate(1);
    assert_eq!(arena.block_count(), 2);
}

#[test]
#[should_panic(expected = "block_size must be greater than 0")]
fn test_arena_zero_block_size_panics() {
    Arena::with_block_size(0);
}

// === Oversized Allocation Tests ===

#[test]
fn test_arena_oversized_allocation() {
    let mut arena = Arena::with_block_size(64);

    // Allocate more than block_size
    let slice = arena.allocate(256);
    assert_eq!(slice.len(), 256);
    assert_eq!(arena.total_allocated(), 256);

    // Write and verify the full oversized allocation
    let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
    arena.write(&slice, &data);
    assert_eq!(arena.read(&slice), data.as_slice());
}

#[test]
fn test_arena_oversized_then_normal() {
    let mut arena = Arena::with_block_size(64);

    // Oversized allocation creates a dedicated block
    let big = arena.allocate(200);
    let blocks_after_big = arena.block_count();

    // Normal allocation should still work in a new standard-sized block
    let small = arena.allocate(32);
    assert!(arena.block_count() > blocks_after_big);

    arena.write(&big, &[0xAA; 200]);
    arena.write(&small, &[0xBB; 32]);

    assert!(arena.read(&big).iter().all(|&b| b == 0xAA));
    assert!(arena.read(&small).iter().all(|&b| b == 0xBB));
    assert_eq!(arena.total_allocated(), 232);
}

// === Reset Tests ===

#[test]
fn test_arena_reset_clears_state() {
    let mut arena = Arena::with_block_size(256);

    arena.allocate(100);
    arena.allocate(200); // triggers second block
    assert!(arena.total_allocated() > 0);
    assert!(arena.block_count() >= 2);

    arena.reset();

    assert_eq!(arena.total_allocated(), 0);
    assert_eq!(arena.block_count(), 1);
}

#[test]
fn test_arena_reset_then_allocate() {
    let mut arena = Arena::with_block_size(256);

    arena.allocate(100);
    arena.reset();

    let slice = arena.allocate(50);
    assert_eq!(arena.total_allocated(), 50);
    assert_eq!(arena.block_count(), 1);

    arena.write(&slice, &[0xDE; 50]);
    assert!(arena.read(&slice).iter().all(|&b| b == 0xDE));
}

// === Send Bound Test ===

#[test]
fn test_arena_is_send() {
    fn assert_send<T: Send>() {}
    assert_send::<Arena>();
    assert_send::<ArenaSlice>();
}

// === Data Integrity Tests ===

#[test]
fn test_arena_allocations_do_not_overlap() {
    let mut arena = Arena::with_block_size(1024);

    let slices: Vec<ArenaSlice> = (0..10).map(|_| arena.allocate(64)).collect();

    // Write a unique pattern to each slice
    for (i, slice) in slices.iter().enumerate() {
        arena.write(slice, &[i as u8; 64]);
    }

    // Verify each slice still contains its unique pattern
    for (i, slice) in slices.iter().enumerate() {
        let data = arena.read(slice);
        assert!(
            data.iter().all(|&b| b == i as u8),
            "slice {} was corrupted: expected all 0x{:02X}, got {:?}",
            i,
            i as u8,
            &data[..8]
        );
    }
}

#[test]
fn test_arena_data_survives_new_block_allocation() {
    let mut arena = Arena::with_block_size(128);

    // Allocate and write in block 0
    let s1 = arena.allocate(64);
    arena.write(&s1, b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    assert_eq!(arena.block_count(), 1);

    // Fill block 0 and force new block
    let _s2 = arena.allocate(64);
    let s3 = arena.allocate(32); // new block
    assert_eq!(arena.block_count(), 2);

    arena.write(&s3, &[0xCC; 32]);

    // Verify block 0 data is still intact after block 1 was allocated
    assert_eq!(arena.read(&s1), b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
    assert!(arena.read(&s3).iter().all(|&b| b == 0xCC));
}

#[test]
fn test_arena_exact_block_boundary_allocation() {
    let mut arena = Arena::with_block_size(128);

    // Allocate exactly one full block
    let s1 = arena.allocate(128);
    assert_eq!(arena.block_count(), 1);
    assert_eq!(arena.total_allocated(), 128);

    // Next allocation of any size must go to a new block
    let s2 = arena.allocate(128);
    assert_eq!(arena.block_count(), 2);
    assert_eq!(arena.total_allocated(), 256);

    arena.write(&s1, &[0x11; 128]);
    arena.write(&s2, &[0x22; 128]);

    assert!(arena.read(&s1).iter().all(|&b| b == 0x11));
    assert!(arena.read(&s2).iter().all(|&b| b == 0x22));
}

#[test]
fn test_arena_zero_size_allocation() {
    let mut arena = Arena::with_block_size(256);

    let s = arena.allocate(0);
    assert_eq!(s.len(), 0);
    assert_eq!(arena.total_allocated(), 0);
    assert_eq!(arena.read(&s).len(), 0);
}

#[test]
fn test_arena_many_small_allocations() {
    let mut arena = Arena::with_block_size(1024);

    let mut slices = Vec::new();
    for i in 0u8..100 {
        let s = arena.allocate(10);
        arena.write(&s, &[i; 10]);
        slices.push(s);
    }

    assert_eq!(arena.total_allocated(), 1000);

    for (i, s) in slices.iter().enumerate() {
        let data = arena.read(s);
        assert!(data.iter().all(|&b| b == i as u8));
    }
}

#[test]
fn test_arena_slice_is_empty() {
    let mut arena = Arena::with_block_size(256);

    let zero_slice = arena.allocate(0);
    assert!(zero_slice.is_empty());

    let nonzero_slice = arena.allocate(10);
    assert!(!nonzero_slice.is_empty());
}

#[test]
fn test_arena_default_same_as_new() {
    let default_arena = Arena::default();
    let new_arena = Arena::new();
    assert_eq!(default_arena.block_count(), new_arena.block_count());
    assert_eq!(default_arena.total_allocated(), new_arena.total_allocated());
}

#[test]
fn test_arena_default_block_size_is_1mb() {
    let arena = Arena::new();
    assert_eq!(arena.block_count(), 1);

    // Verify we can allocate up to 1MB in a single block
    let mut arena = Arena::new();
    arena.allocate(1_048_576);
    assert_eq!(arena.block_count(), 1);
    assert_eq!(arena.total_allocated(), 1_048_576);

    // One more byte triggers a new block
    arena.allocate(1);
    assert_eq!(arena.block_count(), 2);
}
