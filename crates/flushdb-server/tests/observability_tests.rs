use std::time::Duration;

use flushdb_engine::{CacheStats, Level};
use flushdb_server::observability;

#[test]
fn test_init_tracing_default() {
    let result = observability::init_tracing("info");
    assert!(result.is_ok());
}

#[test]
fn test_init_tracing_custom_level() {
    let result = observability::init_tracing("debug");
    assert!(result.is_ok());
}

#[test]
fn test_record_write_latency() {
    observability::record_write_latency("test_ns", "put", Duration::from_millis(42));
}

#[test]
fn test_record_read_latency() {
    observability::record_read_latency("test_ns", "get", Duration::from_millis(15));
}

#[test]
fn test_record_flush() {
    observability::record_flush("test_ns", Duration::from_secs(1), 1024);
}

#[test]
fn test_record_compaction() {
    observability::record_compaction("test_ns", "L1", Duration::from_millis(500), 8192);
}

#[test]
fn test_update_cache_stats() {
    let stats = CacheStats {
        hits: 100,
        misses: 20,
        insertions: 50,
        evictions: 5,
        weighted_size_bytes: 4096,
        entry_count: 45,
    };
    observability::update_cache_stats("test_ns", &stats);
}

#[test]
fn test_update_cache_stats_zero_total() {
    let stats = CacheStats {
        hits: 0,
        misses: 0,
        insertions: 0,
        evictions: 0,
        weighted_size_bytes: 0,
        entry_count: 0,
    };
    observability::update_cache_stats("test_ns", &stats);
}

#[test]
fn test_update_level_stats() {
    let levels = vec![
        (Level::L0, 1024),
        (Level::L1, 4096),
        (Level::L2, 16384),
        (Level::L3, 65536),
    ];
    observability::update_level_stats("test_ns", &levels, 3);
}

#[test]
fn test_metrics_with_namespace_label() {
    observability::record_write_latency("ns_alpha", "put", Duration::from_millis(10));
    observability::record_write_latency("ns_beta", "put", Duration::from_millis(20));
    observability::record_read_latency("ns_alpha", "get", Duration::from_millis(5));
    observability::record_read_latency("ns_beta", "get", Duration::from_millis(8));

    observability::record_write_items("ns_alpha", 10);
    observability::record_write_items("ns_beta", 20);
    observability::record_read_items("ns_alpha", 5);
    observability::record_read_items("ns_beta", 15);
}

#[test]
fn test_record_write_items() {
    observability::record_write_items("test_ns", 42);
}

#[test]
fn test_record_write_bytes() {
    observability::record_write_bytes("test_ns", 2048);
}

#[test]
fn test_record_read_items() {
    observability::record_read_items("test_ns", 10);
}

#[test]
fn test_record_read_bytes() {
    observability::record_read_bytes("test_ns", 4096);
}

#[test]
fn test_record_idempotent_dedup() {
    observability::record_idempotent_dedup("test_ns");
}

#[test]
fn test_record_slo_early_return() {
    observability::record_slo_early_return("test_ns");
}

#[test]
fn test_record_page_token_resume() {
    observability::record_page_token_resume("test_ns");
}

#[test]
fn test_set_active_connections() {
    observability::set_active_connections(5);
}

#[test]
fn test_set_namespace_count() {
    observability::set_namespace_count(3);
}

#[test]
fn test_set_partition_count() {
    observability::set_partition_count(12);
}
