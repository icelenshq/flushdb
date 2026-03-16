use std::time::Duration;

use flushdb_wal::DirtySegmentTracker;

// === Basic Tracking Tests ===

#[test]
fn test_record_write_creates_segment_entry() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    assert!(!tracker.is_segment_clean(1));
}

#[test]
fn test_record_write_tracks_highest_sequence() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.record_write(1, 100, 3); // lower seq, should not overwrite
    tracker.record_write(1, 100, 10); // higher seq

    let gens = tracker.dirty_generation_ids(1);
    assert_eq!(gens, vec![100]);
}

#[test]
fn test_record_write_multiple_generations() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.record_write(1, 200, 8);

    let mut gens = tracker.dirty_generation_ids(1);
    gens.sort();
    assert_eq!(gens, vec![100, 200]);
}

#[test]
fn test_record_write_multiple_segments() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.record_write(2, 100, 10);

    let dirty = tracker.all_dirty_segments();
    assert_eq!(dirty, vec![1, 2]);
}

// === Flush and Cleanup Tests ===

#[test]
fn test_mark_flushed_removes_from_all_segments() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.record_write(2, 100, 10);
    tracker.record_write(3, 100, 15);

    tracker.mark_generation_flushed(100);

    assert!(tracker.is_segment_clean(1));
    assert!(tracker.is_segment_clean(2));
    assert!(tracker.is_segment_clean(3));
}

#[test]
fn test_mark_flushed_returns_clean_segments() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);

    let clean = tracker.mark_generation_flushed(100);
    assert_eq!(clean, vec![1]);
}

#[test]
fn test_mark_flushed_partial_clean() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.record_write(1, 200, 8);

    let clean = tracker.mark_generation_flushed(100);
    // Segment 1 still has generation 200, so it's not clean
    assert!(clean.is_empty());
    assert!(!tracker.is_segment_clean(1));

    let clean = tracker.mark_generation_flushed(200);
    assert_eq!(clean, vec![1]);
}

#[test]
fn test_mark_flushed_unknown_generation() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);

    let clean = tracker.mark_generation_flushed(999);
    assert!(clean.is_empty());
}

#[test]
fn test_deletable_segments_empty_initially() {
    let tracker = DirtySegmentTracker::new();
    assert!(tracker.deletable_segments().is_empty());
}

#[test]
fn test_deletable_segments_after_full_flush() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.mark_generation_flushed(100);

    let deletable = tracker.deletable_segments();
    assert_eq!(deletable, vec![1]);
}

// === Query Tests ===

#[test]
fn test_is_segment_clean_unknown_segment() {
    let tracker = DirtySegmentTracker::new();
    assert!(tracker.is_segment_clean(999));
}

#[test]
fn test_is_segment_clean_dirty_segment() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    assert!(!tracker.is_segment_clean(1));
}

#[test]
fn test_dirty_generation_ids_returns_correct_ids() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.record_write(1, 200, 8);
    tracker.record_write(1, 150, 6);

    let mut gens = tracker.dirty_generation_ids(1);
    gens.sort();
    assert_eq!(gens, vec![100, 150, 200]);
}

#[test]
fn test_all_dirty_segments_lists_all() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(3, 100, 5);
    tracker.record_write(1, 200, 8);
    tracker.record_write(5, 100, 10);

    let dirty = tracker.all_dirty_segments();
    assert_eq!(dirty, vec![1, 3, 5]);
}

// === Flush Trigger Tests ===

#[test]
fn test_segments_older_than_threshold() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    // Just created, so shouldn't be older than 1 hour
    let old = tracker.segments_older_than(Duration::from_secs(3600));
    assert!(old.is_empty());

    // But should be older than 0
    let old = tracker.segments_older_than(Duration::ZERO);
    assert_eq!(old, vec![1]);
}

#[test]
fn test_oldest_pinned_segment() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    std::thread::sleep(Duration::from_millis(10));
    tracker.record_write(2, 200, 10);

    let oldest = tracker.oldest_pinned_segment();
    assert_eq!(oldest, Some(1));
}

#[test]
fn test_needs_age_flush_true() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    // With zero duration, everything is "old"
    assert!(tracker.needs_age_flush(Duration::ZERO));
}

#[test]
fn test_needs_age_flush_false() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    // With very long duration, nothing is old
    assert!(!tracker.needs_age_flush(Duration::from_secs(3600)));
}

#[test]
fn test_needs_size_flush() {
    let tracker = DirtySegmentTracker::new();
    assert!(tracker.needs_size_flush(1000, 500));
    assert!(!tracker.needs_size_flush(500, 1000));
}

// === Edge Cases ===

#[test]
fn test_generations_for_segment_returns_sorted_ids() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 300, 1);
    tracker.record_write(1, 100, 2);
    tracker.record_write(1, 200, 3);

    let gens = tracker.generations_for_segment(1);
    assert_eq!(gens, vec![100, 200, 300]);
}

#[test]
fn test_generations_for_segment_empty() {
    let tracker = DirtySegmentTracker::new();
    assert!(tracker.generations_for_segment(999).is_empty());
}

#[test]
fn test_deletable_segments_empty_after_remove() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.mark_generation_flushed(100);
    assert_eq!(tracker.deletable_segments(), vec![1]);

    tracker.remove_segment(1);
    assert!(
        tracker.deletable_segments().is_empty(),
        "removed segment should not appear in deletable list"
    );
}

#[test]
fn test_remove_segment_cleans_all_tracking() {
    let mut tracker = DirtySegmentTracker::new();
    tracker.record_write(1, 100, 5);
    tracker.record_write(1, 200, 8);

    tracker.remove_segment(1);
    assert!(tracker.is_segment_clean(1));
    assert!(tracker.dirty_generation_ids(1).is_empty());
    assert!(tracker.all_dirty_segments().is_empty());
}

#[test]
fn test_multiple_flush_cycles() {
    let mut tracker = DirtySegmentTracker::new();

    // Cycle 1
    tracker.record_write(1, 100, 5);
    tracker.mark_generation_flushed(100);
    assert!(tracker.is_segment_clean(1));

    // Cycle 2 - new generation writes to same segment
    tracker.record_write(1, 200, 10);
    assert!(!tracker.is_segment_clean(1));
    tracker.mark_generation_flushed(200);
    assert!(tracker.is_segment_clean(1));
}

#[test]
fn test_concurrent_generations_complex() {
    let mut tracker = DirtySegmentTracker::new();

    // 5 generations across 3 segments
    tracker.record_write(1, 1, 10);
    tracker.record_write(1, 2, 20);
    tracker.record_write(2, 2, 30);
    tracker.record_write(2, 3, 40);
    tracker.record_write(3, 3, 50);
    tracker.record_write(3, 4, 60);
    tracker.record_write(3, 5, 70);

    // Flush gen 1 → segment 1 still dirty (has gen 2)
    let clean = tracker.mark_generation_flushed(1);
    assert!(clean.is_empty());

    // Flush gen 2 → segment 1 becomes clean, segment 2 still dirty
    let clean = tracker.mark_generation_flushed(2);
    assert_eq!(clean, vec![1]);

    // Flush gen 3 → segment 2 becomes clean
    let clean = tracker.mark_generation_flushed(3);
    assert_eq!(clean, vec![2]);

    // Flush gen 4 → segment 3 still dirty (has gen 5)
    let clean = tracker.mark_generation_flushed(4);
    assert!(clean.is_empty());

    // Flush gen 5 → segment 3 becomes clean
    let clean = tracker.mark_generation_flushed(5);
    assert_eq!(clean, vec![3]);
}
