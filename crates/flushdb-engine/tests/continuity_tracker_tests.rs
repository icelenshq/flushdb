use flushdb_engine::cache::{CacheConfig, ContinuityTracker};
use flushdb_engine::ManifestId;

fn tracker() -> ContinuityTracker {
    let config = CacheConfig {
        enable_continuity_tracking: true,
        ..CacheConfig::default()
    };
    ContinuityTracker::new(&config)
}

fn disabled_tracker() -> ContinuityTracker {
    let config = CacheConfig {
        enable_continuity_tracking: false,
        ..CacheConfig::default()
    };
    ContinuityTracker::new(&config)
}

#[test]
fn test_mark_and_check_present() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", ManifestId::new(1));
    assert!(t.is_known_absent(b"rec1", b"m", ManifestId::new(1)));
}

#[test]
fn test_check_outside_range() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"m", ManifestId::new(1));
    assert!(!t.is_known_absent(b"rec1", b"z", ManifestId::new(1)));
}

#[test]
fn test_check_at_boundaries() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"m", ManifestId::new(1));
    // start is inclusive
    assert!(t.is_known_absent(b"rec1", b"a", ManifestId::new(1)));
    // end is exclusive
    assert!(!t.is_known_absent(b"rec1", b"m", ManifestId::new(1)));
}

#[test]
fn test_empty_tracker() {
    let t = tracker();
    assert!(!t.is_known_absent(b"rec1", b"m", ManifestId::new(1)));
}

#[test]
fn test_disabled_tracker() {
    let mut t = disabled_tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", ManifestId::new(1));
    assert!(!t.is_known_absent(b"rec1", b"m", ManifestId::new(1)));
    assert_eq!(t.tracked_record_count(), 0);
}

#[test]
fn test_same_manifest_version_hit() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", ManifestId::new(5));
    assert!(t.is_known_absent(b"rec1", b"m", ManifestId::new(5)));
}

#[test]
fn test_different_manifest_version_miss() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", ManifestId::new(5));
    assert!(!t.is_known_absent(b"rec1", b"m", ManifestId::new(6)));
}

#[test]
fn test_invalidate_before_manifest() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"d", ManifestId::new(3));
    t.mark_range_complete(b"rec2", b"a", b"d", ManifestId::new(5));
    t.mark_range_complete(b"rec3", b"a", b"d", ManifestId::new(7));

    t.invalidate_before_manifest(ManifestId::new(6));

    assert!(!t.is_known_absent(b"rec1", b"b", ManifestId::new(3)));
    assert!(!t.is_known_absent(b"rec2", b"b", ManifestId::new(5)));
    assert!(t.is_known_absent(b"rec3", b"b", ManifestId::new(7)));
    assert_eq!(t.tracked_record_count(), 1);
}

#[test]
fn test_adjacent_intervals_merge() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"m", ManifestId::new(1));
    t.mark_range_complete(b"rec1", b"m", b"z", ManifestId::new(1));
    assert_eq!(t.total_interval_count(), 1);
    assert!(t.is_known_absent(b"rec1", b"a", ManifestId::new(1)));
    assert!(t.is_known_absent(b"rec1", b"m", ManifestId::new(1)));
    assert!(t.is_known_absent(b"rec1", b"s", ManifestId::new(1)));
}

#[test]
fn test_overlapping_intervals_merge() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"n", ManifestId::new(1));
    t.mark_range_complete(b"rec1", b"f", b"z", ManifestId::new(1));
    assert_eq!(t.total_interval_count(), 1);
    assert!(t.is_known_absent(b"rec1", b"a", ManifestId::new(1)));
    assert!(t.is_known_absent(b"rec1", b"s", ManifestId::new(1)));
}

#[test]
fn test_non_overlapping_intervals_separate() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"d", ManifestId::new(1));
    t.mark_range_complete(b"rec1", b"x", b"z", ManifestId::new(1));
    assert_eq!(t.total_interval_count(), 2);
    assert!(t.is_known_absent(b"rec1", b"b", ManifestId::new(1)));
    assert!(t.is_known_absent(b"rec1", b"y", ManifestId::new(1)));
    assert!(!t.is_known_absent(b"rec1", b"m", ManifestId::new(1)));
}

#[test]
fn test_invalidate_for_record() {
    let mut t = tracker();
    t.mark_range_complete(b"rec-a", b"a", b"z", ManifestId::new(1));
    t.mark_range_complete(b"rec-b", b"a", b"z", ManifestId::new(1));

    t.invalidate_for_record(b"rec-a");

    assert!(!t.is_known_absent(b"rec-a", b"m", ManifestId::new(1)));
    assert!(t.is_known_absent(b"rec-b", b"m", ManifestId::new(1)));
    assert_eq!(t.tracked_record_count(), 1);
}

#[test]
fn test_invalidate_all() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"z", ManifestId::new(1));
    t.mark_range_complete(b"rec2", b"a", b"z", ManifestId::new(2));
    t.mark_range_complete(b"rec3", b"a", b"z", ManifestId::new(3));

    t.invalidate_all();

    assert_eq!(t.tracked_record_count(), 0);
    assert_eq!(t.total_interval_count(), 0);
}

#[test]
fn test_empty_start_key() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"", b"m", ManifestId::new(1));
    assert!(t.is_known_absent(b"rec1", b"a", ManifestId::new(1)));
    assert!(!t.is_known_absent(b"rec1", b"z", ManifestId::new(1)));
}

#[test]
fn test_empty_end_key() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"m", b"", ManifestId::new(1));
    assert!(t.is_known_absent(b"rec1", b"z", ManifestId::new(1)));
    assert!(!t.is_known_absent(b"rec1", b"a", ManifestId::new(1)));
}

#[test]
fn test_full_record_range() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"", b"", ManifestId::new(1));
    assert!(t.is_known_absent(b"rec1", b"anything", ManifestId::new(1)));
    assert!(t.is_known_absent(b"rec1", b"\x00", ManifestId::new(1)));
    assert!(t.is_known_absent(b"rec1", b"\xff\xff\xff", ManifestId::new(1)));
}

#[test]
fn test_single_byte_range() {
    let mut t = tracker();
    t.mark_range_complete(b"rec1", b"a", b"b", ManifestId::new(1));
    assert!(t.is_known_absent(b"rec1", b"a", ManifestId::new(1)));
    assert!(!t.is_known_absent(b"rec1", b"b", ManifestId::new(1)));
}
