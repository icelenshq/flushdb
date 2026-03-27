use std::collections::HashSet;

use flushdb_server::namespace_config::{NamespaceConfig, PartitionKeyStrategy};
use flushdb_server::partition_router::{LocalPartitionRouter, PartitionRouter};

fn simple_config(partition_count: u32) -> NamespaceConfig {
    NamespaceConfig::new("test".to_string(), partition_count).expect("valid config")
}

fn composite_config(
    delimiter: &str,
    field_indices: Vec<usize>,
    partition_count: u32,
) -> NamespaceConfig {
    NamespaceConfig::with_strategy(
        "test".to_string(),
        PartitionKeyStrategy::Composite {
            delimiter: delimiter.to_string(),
            field_indices,
        },
        partition_count,
    )
    .expect("valid config")
}

fn prefix_config(length: usize, partition_count: u32) -> NamespaceConfig {
    NamespaceConfig::with_strategy(
        "test".to_string(),
        PartitionKeyStrategy::Prefix { length },
        partition_count,
    )
    .expect("valid config")
}

fn custom_hash_config(hash_name: &str, partition_count: u32) -> NamespaceConfig {
    NamespaceConfig::with_strategy(
        "test".to_string(),
        PartitionKeyStrategy::CustomHash {
            hash_name: hash_name.to_string(),
        },
        partition_count,
    )
    .expect("valid config")
}

// ─── Simple Strategy Tests ───

#[test]
fn test_simple_deterministic() {
    let config = simple_config(16);
    let router = LocalPartitionRouter::new(&config);
    let record_id = "user:12345";
    let p1 = router.route(record_id).expect("route succeeds");
    let p2 = router.route(record_id).expect("route succeeds");
    let p3 = router.route(record_id).expect("route succeeds");
    assert_eq!(p1, p2);
    assert_eq!(p2, p3);
}

#[test]
fn test_simple_distribution_uniform() {
    let config = simple_config(16);
    let router = LocalPartitionRouter::new(&config);
    let mut counts = vec![0u32; 16];
    for i in 0..10_000 {
        let record_id = format!("record-{i}");
        let partition = router.route(&record_id).expect("route succeeds");
        counts[partition as usize] += 1;
    }
    for (i, &count) in counts.iter().enumerate() {
        assert!(
            count >= 100,
            "partition {i} got only {count} hits, expected >= 100"
        );
    }
}

#[test]
fn test_simple_single_partition() {
    let config = simple_config(1);
    let router = LocalPartitionRouter::new(&config);
    for i in 0..100 {
        let record_id = format!("key-{i}");
        let partition = router.route(&record_id).expect("route succeeds");
        assert_eq!(partition, 0);
    }
}

#[test]
fn test_simple_different_records_different_partitions() {
    let config = simple_config(16);
    let router = LocalPartitionRouter::new(&config);
    let mut seen = HashSet::new();
    for i in 0..1000 {
        let record_id = format!("record-{i}");
        let partition = router.route(&record_id).expect("route succeeds");
        seen.insert(partition);
    }
    assert!(
        seen.len() >= 2,
        "expected at least 2 different partitions, got {}",
        seen.len()
    );
}

// ─── Composite Strategy Tests ───

#[test]
fn test_composite_extracts_correct_fields() {
    let config = composite_config(":", vec![0, 1], 16);
    let router = LocalPartitionRouter::new(&config);
    let p1 = router.route("tenant:region:id1").expect("route succeeds");
    let p2 = router.route("tenant:region:other").expect("route succeeds");
    assert_eq!(p1, p2);
}

#[test]
fn test_composite_same_prefix_same_partition() {
    let config = composite_config(":", vec![0, 1], 16);
    let router = LocalPartitionRouter::new(&config);
    let p1 = router.route("acme:us:123").expect("route succeeds");
    let p2 = router.route("acme:us:456").expect("route succeeds");
    assert_eq!(p1, p2);
}

#[test]
fn test_composite_different_prefix_can_differ() {
    let config = composite_config(":", vec![0, 1], 256);
    let router = LocalPartitionRouter::new(&config);
    let p1 = router.route("acme:us:123").expect("route succeeds");
    let p2 = router.route("beta:eu:789").expect("route succeeds");
    // They *can* differ — with 256 partitions and different inputs, they almost certainly do,
    // but we only assert they're valid partitions.
    assert!(p1 < 256);
    assert!(p2 < 256);
    // In practice these hash to different values, so let's also verify that.
    assert_ne!(
        p1, p2,
        "different composite keys should likely route differently"
    );
}

#[test]
fn test_composite_insufficient_fields() {
    let config = composite_config(":", vec![0, 1], 16);
    let router = LocalPartitionRouter::new(&config);
    let result = router.route("single");
    assert!(result.is_err());
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("expected at least 2 fields, got 1"),
        "unexpected error: {msg}"
    );
}

// ─── Prefix Strategy Tests ───

#[test]
fn test_prefix_extracts_correct_length() {
    let config = prefix_config(7, 16);
    let router = LocalPartitionRouter::new(&config);
    let p1 = router.route("us-east-user123").expect("route succeeds");
    let p2 = router.route("us-east-user456").expect("route succeeds");
    assert_eq!(p1, p2);
}

#[test]
fn test_prefix_same_prefix_same_partition() {
    let config = prefix_config(4, 16);
    let router = LocalPartitionRouter::new(&config);
    let p1 = router.route("abcd-one").expect("route succeeds");
    let p2 = router.route("abcd-two").expect("route succeeds");
    assert_eq!(p1, p2);
}

#[test]
fn test_prefix_short_record_id() {
    let config = prefix_config(20, 16);
    let router = LocalPartitionRouter::new(&config);
    let result = router.route("hi");
    assert!(result.is_ok());
    let partition = result.expect("route succeeds");
    assert!(partition < 16);
}

// ─── Custom Hash Tests ───

#[test]
fn test_custom_hash_crc32() {
    let config = custom_hash_config("crc32", 16);
    let router = LocalPartitionRouter::new(&config);
    let partition = router.route("test-key").expect("route succeeds");
    assert!(partition < 16);
    // Deterministic
    let partition2 = router.route("test-key").expect("route succeeds");
    assert_eq!(partition, partition2);
}

#[test]
fn test_custom_hash_fnv() {
    let config = custom_hash_config("fnv", 16);
    let router = LocalPartitionRouter::new(&config);
    let partition = router.route("test-key").expect("route succeeds");
    assert!(partition < 16);
    // Deterministic
    let partition2 = router.route("test-key").expect("route succeeds");
    assert_eq!(partition, partition2);
}

#[test]
fn test_custom_hash_unknown_rejected() {
    let config = custom_hash_config("sha256", 16);
    let router = LocalPartitionRouter::new(&config);
    let result = router.route("test-key");
    assert!(result.is_err());
    let msg = result.unwrap_err().to_string();
    assert!(
        msg.contains("unknown hash function: sha256"),
        "unexpected error: {msg}"
    );
}

// ─── Boundary Tests ───

#[test]
fn test_partition_id_within_bounds() {
    let config = simple_config(16);
    let router = LocalPartitionRouter::new(&config);
    for i in 0..5000 {
        let record_id = format!("boundary-test-{i}");
        let partition = router.route(&record_id).expect("route succeeds");
        assert!(
            partition < 16,
            "partition {partition} out of bounds for partition_count=16"
        );
    }
}

#[test]
fn test_power_of_two_masking() {
    let partition_count: u32 = 16;
    let mask = partition_count - 1;
    for val in [0u32, 1, 15, 16, 17, 255, 256, 1000, u32::MAX] {
        assert_eq!(val & mask, val % partition_count);
    }
}

#[test]
fn test_partition_count_1() {
    let config = simple_config(1);
    let router = LocalPartitionRouter::new(&config);
    for i in 0..100 {
        let record_id = format!("key-{i}");
        let partition = router.route(&record_id).expect("route succeeds");
        assert_eq!(partition, 0);
    }
}

#[test]
fn test_partition_count_256() {
    let config = simple_config(256);
    let router = LocalPartitionRouter::new(&config);
    for i in 0..5000 {
        let record_id = format!("key-256-{i}");
        let partition = router.route(&record_id).expect("route succeeds");
        assert!(
            partition < 256,
            "partition {partition} out of bounds for partition_count=256"
        );
    }
}

// ─── Trait Object Test ───

#[test]
fn test_router_as_trait_object() {
    let config = simple_config(16);
    let router = LocalPartitionRouter::new(&config);
    let dyn_router: &dyn PartitionRouter = &router;
    let partition = dyn_router
        .route("trait-object-key")
        .expect("route succeeds");
    assert!(partition < 16);
}
