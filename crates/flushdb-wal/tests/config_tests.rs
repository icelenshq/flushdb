use std::path::Path;
use std::time::Duration;

use flushdb_wal::{
    parse_segment_number, segment_filename, segment_path, FsyncMode, WalConfig,
};

#[test]
fn test_default_config_values() {
    let config = WalConfig::default();
    assert_eq!(config.segment_size_target, 33_554_432);
    assert_eq!(config.max_wal_size, 268_435_456);
    assert_eq!(config.max_total_wal_bytes, 536_870_912);
    assert_eq!(config.segment_max_age, Duration::from_secs(300));
    assert_eq!(config.fsync_mode, FsyncMode::Sync);
    assert_eq!(config.group_commit_interval, Duration::from_micros(200));
    assert_eq!(config.group_commit_max_bytes, 262_144);
    assert_eq!(config.batch_sync_interval, Duration::from_millis(10));
}

#[test]
fn test_segment_filename_zero_padded() {
    assert_eq!(segment_filename(1), "segment-000000000001.wal");
}

#[test]
fn test_segment_filename_large_number() {
    assert_eq!(
        segment_filename(999_999_999_999),
        "segment-999999999999.wal"
    );
}

#[test]
fn test_parse_segment_number_valid() {
    assert_eq!(
        parse_segment_number("segment-000000000042.wal"),
        Some(42)
    );
}

#[test]
fn test_parse_segment_number_invalid_prefix() {
    assert_eq!(parse_segment_number("log-000000000001.wal"), None);
}

#[test]
fn test_parse_segment_number_invalid_extension() {
    assert_eq!(parse_segment_number("segment-000000000001.log"), None);
}

#[test]
fn test_parse_segment_number_non_numeric() {
    assert_eq!(parse_segment_number("segment-abcdefghijkl.wal"), None);
}

#[test]
fn test_parse_segment_number_wrong_width() {
    // Too short: "001" is 3 digits, not 12
    assert_eq!(parse_segment_number("segment-001.wal"), None);
    // Too long: 13 digits
    assert_eq!(parse_segment_number("segment-0000000000001.wal"), None);
}

#[test]
fn test_segment_filename_roundtrip() {
    for n in [0, 1, 42, 100, 999_999_999_999] {
        let filename = segment_filename(n);
        assert_eq!(parse_segment_number(&filename), Some(n), "roundtrip failed for {n}");
    }
}

#[test]
fn test_segment_path_joins_correctly() {
    let dir = Path::new("/tmp/wal");
    let path = segment_path(dir, 5);
    assert_eq!(path, dir.join("segment-000000000005.wal"));
}
