use flushdb_server::{NamespaceConfig, PartitionKeyStrategy, WriteConsistency};
use flushdb_types::FlushError;

// --- Construction Tests ---

#[test]
fn test_new_simple_defaults() {
    let ns = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    assert_eq!(ns.name, "test-ns");
    assert_eq!(ns.partition_count, 4);
    assert_eq!(ns.partition_key_strategy, PartitionKeyStrategy::Simple);
}

#[test]
fn test_new_with_strategy() {
    let strategy = PartitionKeyStrategy::Composite {
        delimiter: ":".to_string(),
        field_indices: vec![0, 2],
    };
    let ns = NamespaceConfig::with_strategy("my-ns".to_string(), strategy.clone(), 8)
        .expect("should succeed");
    assert_eq!(ns.partition_key_strategy, strategy);
    assert_eq!(ns.partition_count, 8);
}

#[test]
fn test_default_values() {
    let ns = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    assert_eq!(ns.memtable_size_threshold, 67_108_864);
    assert!((ns.bloom_filter_fp_rate - 0.01).abs() < f64::EPSILON);
    assert_eq!(ns.default_page_size_bytes, 2_097_152);
    assert_eq!(ns.max_page_size_bytes, 8_388_608);
    assert_eq!(ns.target_latency_slo_ms, 10);
    assert_eq!(ns.max_latency_slo_ms, 500);
    assert_eq!(ns.write_consistency, WriteConsistency::Quorum);
    assert_eq!(ns.replication_factor, 3);
    assert_eq!(ns.compaction_strategy, "LEVELED");
}

#[test]
fn test_s3_path_prefix_default() {
    let ns = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    assert_eq!(ns.s3_path_prefix(), "flushdb/test-ns/");
}

// --- Validation Tests ---

#[test]
fn test_rejects_empty_name() {
    let result = NamespaceConfig::new(String::new(), 4);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert_eq!(message, "name: must not be empty");
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

#[test]
fn test_rejects_slash_in_name() {
    let result = NamespaceConfig::new("bad/name".to_string(), 4);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert_eq!(message, "name: must not contain / or null bytes");
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

#[test]
fn test_rejects_null_in_name() {
    let result = NamespaceConfig::new("bad\0name".to_string(), 4);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert_eq!(message, "name: must not contain / or null bytes");
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

#[test]
fn test_rejects_zero_partition_count() {
    let result = NamespaceConfig::new("test-ns".to_string(), 0);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert_eq!(message, "partition_count: must be > 0");
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

#[test]
fn test_rejects_non_power_of_two() {
    for count in [3, 5, 6, 7, 9, 10, 12, 15, 17, 100] {
        let result = NamespaceConfig::new("test-ns".to_string(), count);
        assert!(result.is_err(), "partition_count={count} should be rejected");
        match result.unwrap_err() {
            FlushError::InvalidArgument { message } => {
                assert_eq!(message, "partition_count: must be a power of 2");
            }
            e => panic!("unexpected error for partition_count={count}: {e:?}"),
        }
    }
}

#[test]
fn test_accepts_power_of_two() {
    for count in [1, 2, 4, 8, 16, 32, 64, 128, 256] {
        let result = NamespaceConfig::new("test-ns".to_string(), count);
        assert!(
            result.is_ok(),
            "partition_count={count} should be accepted, got: {:?}",
            result.err()
        );
    }
}

#[test]
fn test_rejects_invalid_bloom_fp_rate() {
    let mut ns = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");

    ns.bloom_filter_fp_rate = 0.0;
    assert!(matches!(
        ns.validate(),
        Err(FlushError::InvalidArgument { message }) if message == "bloom_filter_fp_rate: must be in (0.0, 1.0)"
    ));

    ns.bloom_filter_fp_rate = -0.5;
    assert!(matches!(
        ns.validate(),
        Err(FlushError::InvalidArgument { message }) if message == "bloom_filter_fp_rate: must be in (0.0, 1.0)"
    ));

    ns.bloom_filter_fp_rate = 1.0;
    assert!(matches!(
        ns.validate(),
        Err(FlushError::InvalidArgument { message }) if message == "bloom_filter_fp_rate: must be in (0.0, 1.0)"
    ));

    ns.bloom_filter_fp_rate = 1.5;
    assert!(matches!(
        ns.validate(),
        Err(FlushError::InvalidArgument { message }) if message == "bloom_filter_fp_rate: must be in (0.0, 1.0)"
    ));
}

#[test]
fn test_rejects_page_size_inversion() {
    let mut ns = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    ns.default_page_size_bytes = 10_000_000;
    ns.max_page_size_bytes = 5_000_000;
    assert!(matches!(
        ns.validate(),
        Err(FlushError::InvalidArgument { message }) if message == "default_page_size_bytes: must be <= max_page_size_bytes"
    ));
}

#[test]
fn test_rejects_slo_inversion() {
    let mut ns = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    ns.target_latency_slo_ms = 1000;
    ns.max_latency_slo_ms = 100;
    assert!(matches!(
        ns.validate(),
        Err(FlushError::InvalidArgument { message }) if message == "target_latency_slo_ms: must be <= max_latency_slo_ms"
    ));
}

#[test]
fn test_rejects_composite_empty_fields() {
    let strategy = PartitionKeyStrategy::Composite {
        delimiter: ":".to_string(),
        field_indices: vec![],
    };
    let result = NamespaceConfig::with_strategy("test-ns".to_string(), strategy, 4);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert_eq!(message, "field_indices: must have at least one field index");
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

#[test]
fn test_rejects_prefix_zero_length() {
    let strategy = PartitionKeyStrategy::Prefix { length: 0 };
    let result = NamespaceConfig::with_strategy("test-ns".to_string(), strategy, 4);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert_eq!(message, "prefix_length: must be > 0");
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

// --- Serialization Tests ---

#[test]
fn test_serde_round_trip() {
    let ns = NamespaceConfig::with_strategy(
        "round-trip-ns".to_string(),
        PartitionKeyStrategy::Prefix { length: 4 },
        16,
    )
    .expect("should succeed");

    let json = serde_json::to_string(&ns).expect("serialize");
    let deserialized: NamespaceConfig = serde_json::from_str(&json).expect("deserialize");

    assert_eq!(ns.name, deserialized.name);
    assert_eq!(ns.partition_key_strategy, deserialized.partition_key_strategy);
    assert_eq!(ns.partition_count, deserialized.partition_count);
    assert_eq!(ns.s3_path_prefix, deserialized.s3_path_prefix);
    assert_eq!(ns.memtable_size_threshold, deserialized.memtable_size_threshold);
    assert!((ns.bloom_filter_fp_rate - deserialized.bloom_filter_fp_rate).abs() < f64::EPSILON);
    assert_eq!(ns.default_page_size_bytes, deserialized.default_page_size_bytes);
    assert_eq!(ns.max_page_size_bytes, deserialized.max_page_size_bytes);
    assert_eq!(ns.target_latency_slo_ms, deserialized.target_latency_slo_ms);
    assert_eq!(ns.max_latency_slo_ms, deserialized.max_latency_slo_ms);
    assert_eq!(ns.write_consistency, deserialized.write_consistency);
    assert_eq!(ns.replication_factor, deserialized.replication_factor);
}

#[test]
fn test_serde_missing_optional_fields() {
    let json = r#"{"name":"minimal","partition_key_strategy":"Simple","partition_count":4}"#;
    let ns: NamespaceConfig = serde_json::from_str(json).expect("deserialize");

    assert_eq!(ns.name, "minimal");
    assert_eq!(ns.partition_count, 4);
    assert_eq!(ns.partition_key_strategy, PartitionKeyStrategy::Simple);
    assert_eq!(ns.memtable_size_threshold, 67_108_864);
    assert!((ns.bloom_filter_fp_rate - 0.01).abs() < f64::EPSILON);
    assert_eq!(ns.default_page_size_bytes, 2_097_152);
    assert_eq!(ns.max_page_size_bytes, 8_388_608);
    assert_eq!(ns.target_latency_slo_ms, 10);
    assert_eq!(ns.max_latency_slo_ms, 500);
    assert_eq!(ns.write_consistency, WriteConsistency::Quorum);
    assert_eq!(ns.replication_factor, 3);
    assert_eq!(ns.compaction_strategy, "LEVELED");
}

#[test]
fn test_serde_forward_compatibility() {
    let json = r#"{
        "name": "forward-compat",
        "partition_key_strategy": "Simple",
        "partition_count": 4,
        "some_future_field": "some_value",
        "another_new_thing": 42
    }"#;
    let ns: NamespaceConfig = serde_json::from_str(json).expect("should ignore unknown fields");
    assert_eq!(ns.name, "forward-compat");
    assert_eq!(ns.partition_count, 4);
}

// --- Immutability Tests ---

#[test]
fn test_can_update_mutable_fields() {
    let original = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    let mut updated = original.clone();
    updated.memtable_size_threshold = 128 * 1024 * 1024;
    updated.bloom_filter_fp_rate = 0.001;
    updated.default_page_size_bytes = 4_194_304;
    updated.max_page_size_bytes = 16_777_216;
    updated.target_latency_slo_ms = 5;
    updated.max_latency_slo_ms = 200;

    assert!(original.can_update_from(&updated).is_ok());
}

#[test]
fn test_rejects_name_change() {
    let original = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    let mut updated = original.clone();
    updated.name = "different-ns".to_string();

    let result = original.can_update_from(&updated);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("name") && message.contains("immutable"),
                "message should mention name and immutable, got: {message}"
            );
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

#[test]
fn test_rejects_strategy_change() {
    let original = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    let mut updated = original.clone();
    updated.partition_key_strategy = PartitionKeyStrategy::Prefix { length: 3 };

    let result = original.can_update_from(&updated);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("partition_key_strategy") && message.contains("immutable"),
                "message should mention partition_key_strategy and immutable, got: {message}"
            );
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

#[test]
fn test_rejects_partition_count_change() {
    let original = NamespaceConfig::new("test-ns".to_string(), 4).expect("should succeed");
    let mut updated = original.clone();
    updated.partition_count = 8;

    let result = original.can_update_from(&updated);
    assert!(result.is_err());
    match result.unwrap_err() {
        FlushError::InvalidArgument { message } => {
            assert!(
                message.contains("partition_count") && message.contains("immutable"),
                "message should mention partition_count and immutable, got: {message}"
            );
        }
        e => panic!("unexpected error: {e:?}"),
    }
}

// --- Engine Config Conversion Tests ---

#[test]
fn test_engine_config_defaults() {
    let ns = NamespaceConfig::new("engine-test".to_string(), 4).expect("should succeed");
    let dir = std::path::PathBuf::from("/tmp/flushdb-test");
    let engine_cfg = ns.engine_config(dir.clone());

    assert_eq!(engine_cfg.namespace, "engine-test");
    assert_eq!(engine_cfg.local_dir, dir);
    assert_eq!(engine_cfg.memtable_config.size_threshold, 67_108_864);
    assert_eq!(engine_cfg.manifest_config.base_path, "flushdb/engine-test/");

    // fp=0.01 -> ~10 bits per key
    let expected_bits = (-(0.01_f64).ln() / (2.0_f64.ln().powi(2))).ceil() as u32;
    assert_eq!(engine_cfg.flush_config.sst_config.bloom_bits_per_key, expected_bits);
}

#[test]
fn test_engine_config_custom_values() {
    let mut ns = NamespaceConfig::new("custom-engine".to_string(), 8).expect("should succeed");
    ns.memtable_size_threshold = 128 * 1024 * 1024;
    ns.bloom_filter_fp_rate = 0.001;

    let dir = std::path::PathBuf::from("/tmp/flushdb-custom");
    let engine_cfg = ns.engine_config(dir);

    assert_eq!(engine_cfg.memtable_config.size_threshold, 128 * 1024 * 1024);

    let expected_bits = (-(0.001_f64).ln() / (2.0_f64.ln().powi(2))).ceil() as u32;
    assert_eq!(engine_cfg.flush_config.sst_config.bloom_bits_per_key, expected_bits);
}
