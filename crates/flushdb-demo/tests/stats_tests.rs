use std::time::Duration;

use flushdb_demo::stats::BenchStats;

#[test]
fn test_empty_stats_snapshot() {
    let stats = BenchStats::new();
    let snap = stats.snapshot();
    assert_eq!(snap.total_ops, 0);
    assert_eq!(snap.p50_us, 0);
    assert_eq!(snap.p95_us, 0);
    assert_eq!(snap.p99_us, 0);
    assert_eq!(snap.max_us, 0);
}

#[test]
fn test_record_single_op() {
    let mut stats = BenchStats::new();
    stats.record(Duration::from_micros(500));
    let snap = stats.snapshot();
    assert_eq!(snap.total_ops, 1);
    assert!(snap.p50_us >= 499 && snap.p50_us <= 501);
}

#[test]
fn test_record_multiple_ops() {
    let mut stats = BenchStats::new();
    for i in 1..=100 {
        stats.record(Duration::from_micros(i * 100));
    }
    let snap = stats.snapshot();
    assert_eq!(snap.total_ops, 100);
    assert!(snap.p50_us > 0);
    assert!(snap.p95_us >= snap.p50_us);
    assert!(snap.p99_us >= snap.p95_us);
    assert!(snap.max_us >= snap.p99_us);
}

#[test]
fn test_merge_stats() {
    let mut a = BenchStats::new();
    let mut b = BenchStats::new();
    for _ in 0..50 {
        a.record(Duration::from_micros(100));
    }
    for _ in 0..50 {
        b.record(Duration::from_micros(200));
    }
    a.merge(&b);
    let snap = a.snapshot();
    assert_eq!(snap.total_ops, 100);
}

#[test]
fn test_clamp_very_large_duration() {
    let mut stats = BenchStats::new();
    stats.record(Duration::from_secs(120));
    let snap = stats.snapshot();
    assert_eq!(snap.total_ops, 1);
    // HDR histogram may round the value slightly
    assert!(snap.max_us >= 59_000_000 && snap.max_us <= 61_000_000);
}

#[test]
fn test_ops_per_sec_positive() {
    let mut stats = BenchStats::new();
    for _ in 0..10 {
        stats.record(Duration::from_micros(100));
    }
    let snap = stats.snapshot();
    assert!(snap.ops_per_sec > 0.0);
}
