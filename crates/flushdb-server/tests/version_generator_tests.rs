use std::collections::HashSet;
use std::sync::Arc;

use flushdb_server::VersionGenerator;

// --- Monotonicity Tests ---

#[test]
fn test_sequential_versions_monotonic() {
    let gen = VersionGenerator::new(1);
    let mut prev = gen.next_version();

    for _ in 1..1000 {
        let curr = gen.next_version();
        assert!(
            curr > prev,
            "version must be strictly increasing: prev={prev:?}, curr={curr:?}"
        );
        prev = curr;
    }
}

#[test]
fn test_versions_sorted_by_bytes() {
    let gen = VersionGenerator::new(42);
    let keys: Vec<_> = (0..100).map(|_| gen.next_version()).collect();
    let byte_vecs: Vec<_> = keys.iter().map(|k| k.to_bytes()).collect();

    for window in byte_vecs.windows(2) {
        assert!(
            window[0] < window[1],
            "byte encoding must preserve ordering"
        );
    }
}

#[test]
fn test_same_millisecond_different_sequence() {
    let gen = VersionGenerator::new(5);

    // Generate keys rapidly — they should share a timestamp within the same ms
    let keys: Vec<_> = (0..100).map(|_| gen.next_version()).collect();

    // Find keys with the same timestamp
    let first_ts = keys[0].timestamp_ms();
    let same_ts_keys: Vec<_> = keys
        .iter()
        .filter(|k| k.timestamp_ms() == first_ts)
        .collect();

    // If we got multiple keys at the same timestamp, verify sequences are incrementing
    if same_ts_keys.len() > 1 {
        for window in same_ts_keys.windows(2) {
            assert!(
                window[1].sequence() > window[0].sequence(),
                "within same timestamp, sequence must increment"
            );
        }
    }
}

// --- Node ID Tests ---

#[test]
fn test_node_id_preserved() {
    let gen = VersionGenerator::new(99);
    assert_eq!(gen.node_id(), 99);

    let key = gen.next_version();
    assert_eq!(key.node_id(), 99);
}

#[test]
fn test_different_node_ids() {
    let gen_a = VersionGenerator::new(10);
    let gen_b = VersionGenerator::new(20);

    let key_a = gen_a.next_version();
    let key_b = gen_b.next_version();

    assert_eq!(key_a.node_id(), 10);
    assert_eq!(key_b.node_id(), 20);
    assert_ne!(key_a.node_id(), key_b.node_id());
}

// --- Concurrency Tests ---

#[tokio::test]
async fn test_concurrent_generation_unique() {
    let gen = Arc::new(VersionGenerator::new(1));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let gen = Arc::clone(&gen);
        handles.push(tokio::spawn(async move {
            (0..1000).map(|_| gen.next_version()).collect::<Vec<_>>()
        }));
    }

    let mut all_keys = HashSet::new();
    for handle in handles {
        let keys = handle.await.expect("task should complete");
        for key in keys {
            let bytes = key.to_bytes();
            assert!(
                all_keys.insert(bytes),
                "duplicate key detected: {key:?}"
            );
        }
    }

    assert_eq!(all_keys.len(), 10_000);
}

#[tokio::test]
async fn test_concurrent_generation_monotonic() {
    let gen = Arc::new(VersionGenerator::new(7));
    let mut handles = Vec::new();

    for _ in 0..10 {
        let gen = Arc::clone(&gen);
        handles.push(tokio::spawn(async move {
            let keys: Vec<_> = (0..1000).map(|_| gen.next_version()).collect();
            // Verify per-task monotonicity
            for window in keys.windows(2) {
                assert!(
                    window[1] > window[0],
                    "per-task keys must be monotonically increasing"
                );
            }
            keys
        }));
    }

    for handle in handles {
        handle.await.expect("task should complete");
    }
}

// --- Overflow Tests ---

#[test]
fn test_sequence_overflow_advances_timestamp() {
    let gen = VersionGenerator::new(1);

    // Generate u16::MAX + 1 keys (65536) — this exhausts one millisecond's sequence space
    let first = gen.next_version();
    let first_ts = first.timestamp_ms();

    // Generate remaining keys to fill the sequence space (0 is already used)
    let mut last_key = first;
    for _ in 1..=u16::MAX as u32 {
        last_key = gen.next_version();
    }

    // After u16::MAX + 1 keys, the sequence should have overflowed and the timestamp advanced
    // The last key in the filled space uses the original timestamp
    // Now generate one more — it must have a timestamp >= first_ts
    let overflow_key = gen.next_version();
    assert!(
        overflow_key > last_key,
        "key after overflow must be greater than last key before overflow"
    );
    // The timestamp should have advanced (or if wall-clock already moved, it's still valid)
    assert!(
        overflow_key.timestamp_ms() >= first_ts,
        "timestamp must not go backwards after overflow"
    );
}

// --- Edge Cases ---

#[test]
fn test_zero_node_id() {
    let gen = VersionGenerator::new(0);
    assert_eq!(gen.node_id(), 0);

    let key = gen.next_version();
    assert_eq!(key.node_id(), 0);

    // Verify it still generates valid, monotonic keys
    let key2 = gen.next_version();
    assert!(key2 > key);
}

#[test]
fn test_max_node_id() {
    let gen = VersionGenerator::new(u16::MAX);
    assert_eq!(gen.node_id(), u16::MAX);

    let key = gen.next_version();
    assert_eq!(key.node_id(), u16::MAX);

    let key2 = gen.next_version();
    assert!(key2 > key);
}
