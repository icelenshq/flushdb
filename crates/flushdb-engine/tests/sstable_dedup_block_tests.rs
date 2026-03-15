use bytes::Bytes;
use flushdb_engine::sstable::dedup_block::{DedupBlock, DedupBlockBuilder};
use flushdb_types::{FlushError, IdempotencyToken};

fn make_token(i: u64) -> IdempotencyToken {
    let shifted = i.wrapping_add(1);
    let mut token_bytes = [0u8; 16];
    token_bytes[..8].copy_from_slice(&shifted.to_le_bytes());
    token_bytes[8..16].copy_from_slice(&shifted.wrapping_mul(0x9E3779B97F4A7C15).to_le_bytes());
    IdempotencyToken::from_parts(shifted, token_bytes)
}

// ─── Lookup ─────────────────────────────────────────────────────────

#[test]
fn test_contains_present_token() {
    let mut builder = DedupBlockBuilder::new();
    let token = make_token(42);
    builder.add(&token);
    let block = builder.build();

    assert!(block.contains(&token));
}

#[test]
fn test_contains_absent_token() {
    let mut builder = DedupBlockBuilder::new();
    builder.add(&make_token(1));
    let block = builder.build();

    assert!(!block.contains(&make_token(2)));
}

#[test]
fn test_contains_none_token_returns_false() {
    let mut builder = DedupBlockBuilder::new();
    builder.add(&make_token(100));
    let block = builder.build();

    assert!(!block.contains(&IdempotencyToken::none()));
}

#[test]
fn test_contains_none_token_returns_false_empty_block() {
    let builder = DedupBlockBuilder::new();
    let block = builder.build();

    assert!(!block.contains(&IdempotencyToken::none()));
}

#[test]
fn test_multiple_tokens_all_found() {
    let mut builder = DedupBlockBuilder::new();
    let tokens: Vec<IdempotencyToken> = (0..100).map(make_token).collect();
    for t in &tokens {
        builder.add(t);
    }
    let block = builder.build();

    for (i, t) in tokens.iter().enumerate() {
        assert!(block.contains(t), "token {i} must be found");
    }
}

// ─── Builder ─────────────────────────────────────────────────────────

#[test]
fn test_builder_skips_none_tokens() {
    let mut builder = DedupBlockBuilder::new();
    builder.add(&IdempotencyToken::none());
    builder.add(&IdempotencyToken::none());

    assert_eq!(builder.len(), 0);
    assert!(builder.is_empty());
}

#[test]
fn test_builder_deduplicates() {
    let token = make_token(7);
    let mut builder = DedupBlockBuilder::new();
    builder.add(&token);
    builder.add(&token);

    assert_eq!(builder.len(), 1);
}

#[test]
fn test_builder_add_all() {
    let tokens: Vec<IdempotencyToken> = (0..30).map(make_token).collect();

    let mut builder_individual = DedupBlockBuilder::new();
    for t in &tokens {
        builder_individual.add(t);
    }
    let block_individual = builder_individual.build();

    let mut builder_batch = DedupBlockBuilder::new();
    builder_batch.add_all(&tokens);
    let block_batch = builder_batch.build();

    assert_eq!(
        block_individual.serialize(),
        block_batch.serialize(),
        "add_all must produce the same block as individual add calls"
    );
}

// ─── Serialization ──────────────────────────────────────────────────

#[test]
fn test_serialize_deserialize_round_trip() {
    let tokens: Vec<IdempotencyToken> = (0..50).map(make_token).collect();
    let mut builder = DedupBlockBuilder::new();
    for t in &tokens {
        builder.add(t);
    }
    let block = builder.build();
    let serialized = block.serialize();
    let restored = DedupBlock::deserialize(&serialized).expect("deserialize must succeed");

    for (i, t) in tokens.iter().enumerate() {
        assert!(
            restored.contains(t),
            "token {i} must survive serialization round-trip"
        );
    }
    assert_eq!(block.len(), restored.len());
}

#[test]
fn test_serialize_empty_block() {
    let builder = DedupBlockBuilder::new();
    let block = builder.build();
    let serialized = block.serialize();

    assert_eq!(serialized.len(), 4, "empty dedup block must be 4 bytes (count=0)");
    assert_eq!(&serialized[..], &[0, 0, 0, 0]);

    let restored = DedupBlock::deserialize(&serialized).expect("deserialize must succeed");
    assert!(restored.is_empty());
    assert_eq!(restored.len(), 0);
}

#[test]
fn test_deserialize_rejects_invalid_length() {
    // 4 bytes header + 15 bytes = 19 bytes total, not aligned to 16-byte hashes
    let mut data = vec![0u8; 19];
    // Set count to 1 so the deserializer tries to read hash data
    data[0] = 1;
    let result = DedupBlock::deserialize(&data);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "data length not aligned to 16 must yield CorruptedData"
    );
}

#[test]
fn test_deserialize_rejects_count_mismatch() {
    // Header says count=2 but only 1 hash (16 bytes) follows → count mismatch
    let mut data = vec![0u8; 4 + 16];
    data[0] = 2; // count = 2
    let result = DedupBlock::deserialize(&data);
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "count mismatch should yield CorruptedData"
    );
}

#[test]
fn test_size_bytes_consistent_with_serialized() {
    let tokens: Vec<IdempotencyToken> = (0..25).map(make_token).collect();
    let mut builder = DedupBlockBuilder::new();
    for t in &tokens {
        builder.add(t);
    }
    let block = builder.build();

    assert_eq!(
        block.size_bytes(),
        block.serialize().len(),
        "size_bytes() must match actual serialized length"
    );
}

#[test]
fn test_deserialize_rejects_too_short_header() {
    let result = DedupBlock::deserialize(&Bytes::from_static(&[0u8; 3]));
    assert!(
        matches!(result, Err(FlushError::CorruptedData { .. })),
        "data shorter than 4-byte header must yield CorruptedData"
    );
}

// ─── Ordering ───────────────────────────────────────────────────────

#[test]
fn test_hashes_sorted_after_build() {
    let tokens: Vec<IdempotencyToken> = (0..100).map(make_token).collect();
    let mut builder = DedupBlockBuilder::new();
    for t in &tokens {
        builder.add(t);
    }
    let block = builder.build();
    let serialized = block.serialize();

    let count = u32::from_le_bytes(serialized[0..4].try_into().unwrap()) as usize;
    let mut prev = [0u8; 16];
    for i in 0..count {
        let offset = 4 + i * 16;
        let mut current = [0u8; 16];
        current.copy_from_slice(&serialized[offset..offset + 16]);
        if i > 0 {
            assert!(
                prev <= current,
                "hash at index {i} is not >= previous hash (not sorted)"
            );
        }
        prev = current;
    }
}

#[test]
fn test_binary_search_correctness_large() {
    let tokens: Vec<IdempotencyToken> = (0..1000).map(make_token).collect();
    let mut builder = DedupBlockBuilder::new();
    for t in &tokens {
        builder.add(t);
    }
    let block = builder.build();

    for (i, t) in tokens.iter().enumerate() {
        assert!(block.contains(t), "inserted token {i} must be found");
    }

    for i in 2000..3000u64 {
        let absent = make_token(i);
        assert!(
            !block.contains(&absent),
            "absent token {i} must not be found"
        );
    }
}
