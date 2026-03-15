use bytes::Bytes;
use flushdb_engine::{RangeTombstone, RangeTombstoneIndex};

fn tombstone(record_id: &[u8], start: &[u8], end: &[u8], seq: u64) -> RangeTombstone {
    RangeTombstone {
        record_id: Bytes::copy_from_slice(record_id),
        start_key: Bytes::copy_from_slice(start),
        end_key: Bytes::copy_from_slice(end),
        sequence_number: seq,
    }
}

// ── Basic Coverage Tests ────────────────────────────────────────────

#[test]
fn test_covers_key_in_range() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec1", b"a", b"d", 10));

    assert!(idx.covers(b"rec1", b"b", 5));
}

#[test]
fn test_does_not_cover_key_outside_range() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec1", b"a", b"d", 10));

    assert!(!idx.covers(b"rec1", b"e", 5));
}

#[test]
fn test_does_not_cover_key_at_end_exclusive() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec1", b"a", b"d", 10));

    assert!(!idx.covers(b"rec1", b"d", 5));
}

#[test]
fn test_covers_key_at_start_inclusive() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec1", b"a", b"d", 10));

    assert!(idx.covers(b"rec1", b"a", 5));
}

#[test]
fn test_does_not_cover_higher_sequence() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec1", b"a", b"d", 5));

    assert!(!idx.covers(b"rec1", b"b", 10));
}

#[test]
fn test_covers_lower_sequence() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec1", b"a", b"d", 10));

    assert!(idx.covers(b"rec1", b"b", 5));
}

#[test]
fn test_does_not_cover_equal_sequence() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec1", b"a", b"d", 5));

    assert!(!idx.covers(b"rec1", b"b", 5));
}

// ── Unbounded Range Tests ───────────────────────────────────────────

#[test]
fn test_empty_end_key_covers_all_after_start() {
    let mut idx = RangeTombstoneIndex::new();
    // Empty end_key means unbounded upper
    idx.add(tombstone(b"rec1", b"m", b"", 10));

    assert!(idx.covers(b"rec1", b"m", 5));
    assert!(idx.covers(b"rec1", b"z", 5));
    assert!(idx.covers(b"rec1", b"zzzzz", 5));
    // Before start_key should not be covered
    assert!(!idx.covers(b"rec1", b"a", 5));
}

#[test]
fn test_empty_start_key_covers_from_beginning() {
    let mut idx = RangeTombstoneIndex::new();
    // Empty start_key means from beginning of record
    idx.add(tombstone(b"rec1", b"", b"m", 10));

    assert!(idx.covers(b"rec1", b"a", 5));
    assert!(idx.covers(b"rec1", b"l", 5));
    // Empty string key itself is covered (empty start <= empty key, and empty key < "m")
    assert!(idx.covers(b"rec1", b"", 5));
    // At or after end_key should not be covered
    assert!(!idx.covers(b"rec1", b"m", 5));
    assert!(!idx.covers(b"rec1", b"z", 5));
}

#[test]
fn test_both_empty_covers_entire_record() {
    let mut idx = RangeTombstoneIndex::new();
    // Both empty means entire record
    idx.add(tombstone(b"rec1", b"", b"", 10));

    assert!(idx.covers(b"rec1", b"", 5));
    assert!(idx.covers(b"rec1", b"a", 5));
    assert!(idx.covers(b"rec1", b"zzzzz", 5));
    assert!(idx.covers(b"rec1", b"\xff\xff\xff", 5));
}

// ── Multi-Record Tests ──────────────────────────────────────────────

#[test]
fn test_tombstone_scoped_to_record() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"record-A", b"a", b"z", 10));

    assert!(idx.covers(b"record-A", b"m", 5));
    assert!(!idx.covers(b"record-B", b"m", 5));
}

#[test]
fn test_multiple_records_independent() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec-alpha", b"a", b"d", 10));
    idx.add(tombstone(b"rec-beta", b"x", b"z", 20));

    // rec-alpha tombstone covers "b" in rec-alpha
    assert!(idx.covers(b"rec-alpha", b"b", 5));
    // rec-alpha tombstone does NOT cover "b" in rec-beta (different record)
    assert!(!idx.covers(b"rec-beta", b"b", 15));
    // rec-beta tombstone covers "y" in rec-beta
    assert!(idx.covers(b"rec-beta", b"y", 15));
    // rec-beta tombstone does NOT cover "y" in rec-alpha
    assert!(!idx.covers(b"rec-alpha", b"y", 5));
}

#[test]
fn test_tombstones_for_record_returns_correct_slice() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec-A", b"a", b"c", 10));
    idx.add(tombstone(b"rec-A", b"m", b"p", 12));
    idx.add(tombstone(b"rec-B", b"x", b"z", 15));

    let slice_a = idx.tombstones_for_record(b"rec-A");
    assert_eq!(slice_a.len(), 2);
    assert_eq!(slice_a[0].start_key.as_ref(), b"a");
    assert_eq!(slice_a[1].start_key.as_ref(), b"m");

    let slice_b = idx.tombstones_for_record(b"rec-B");
    assert_eq!(slice_b.len(), 1);
    assert_eq!(slice_b[0].start_key.as_ref(), b"x");
}

#[test]
fn test_tombstones_for_record_empty() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"rec-A", b"a", b"z", 10));

    let slice = idx.tombstones_for_record(b"nonexistent");
    assert!(slice.is_empty());
}

// ── Overlapping Tombstone Tests ─────────────────────────────────────

#[test]
fn test_overlapping_tombstones_any_covers() {
    let mut idx = RangeTombstoneIndex::new();
    // Two overlapping tombstones: [a, f) seq 10 and [c, h) seq 12
    idx.add(tombstone(b"rec1", b"a", b"f", 10));
    idx.add(tombstone(b"rec1", b"c", b"h", 12));

    // "d" is in both ranges — covered by either
    assert!(idx.covers(b"rec1", b"d", 5));
    // "b" is only in [a, f)
    assert!(idx.covers(b"rec1", b"b", 5));
    // "g" is only in [c, h)
    assert!(idx.covers(b"rec1", b"g", 5));
}

#[test]
fn test_overlapping_different_sequences() {
    let mut idx = RangeTombstoneIndex::new();
    // Low-seq tombstone [a, f) seq 3
    idx.add(tombstone(b"rec1", b"a", b"f", 3));
    // High-seq tombstone [c, h) seq 20
    idx.add(tombstone(b"rec1", b"c", b"h", 20));

    // "d" at entry_seq 10: covered by [c, h) seq 20, but NOT by [a, f) seq 3
    assert!(idx.covers(b"rec1", b"d", 10));

    // "b" at entry_seq 10: only in [a, f) seq 3, which has lower seq — not covered
    assert!(!idx.covers(b"rec1", b"b", 10));

    // "b" at entry_seq 1: covered by [a, f) seq 3
    assert!(idx.covers(b"rec1", b"b", 1));
}

#[test]
fn test_adjacent_tombstones() {
    let mut idx = RangeTombstoneIndex::new();
    // [a, c) and [c, f) — adjacent, non-overlapping
    idx.add(tombstone(b"rec1", b"a", b"c", 10));
    idx.add(tombstone(b"rec1", b"c", b"f", 10));

    // "b" covered by first
    assert!(idx.covers(b"rec1", b"b", 5));
    // "c" is NOT covered by first (end exclusive), but IS covered by second (start inclusive)
    assert!(idx.covers(b"rec1", b"c", 5));
    // "d" covered by second
    assert!(idx.covers(b"rec1", b"d", 5));
    // "f" not covered by either
    assert!(!idx.covers(b"rec1", b"f", 5));
}

// ── Ordering Tests ──────────────────────────────────────────────────

#[test]
fn test_tombstones_maintained_sorted() {
    let mut idx = RangeTombstoneIndex::new();
    // Add in reverse order
    idx.add(tombstone(b"rec-Z", b"x", b"z", 1));
    idx.add(tombstone(b"rec-A", b"m", b"p", 2));
    idx.add(tombstone(b"rec-A", b"a", b"c", 3));
    idx.add(tombstone(b"rec-M", b"d", b"f", 4));

    let all: Vec<_> = idx.iter().collect();
    assert_eq!(all.len(), 4);
    // Should be sorted by (record_id, start_key)
    assert_eq!(all[0].record_id.as_ref(), b"rec-A");
    assert_eq!(all[0].start_key.as_ref(), b"a");
    assert_eq!(all[1].record_id.as_ref(), b"rec-A");
    assert_eq!(all[1].start_key.as_ref(), b"m");
    assert_eq!(all[2].record_id.as_ref(), b"rec-M");
    assert_eq!(all[2].start_key.as_ref(), b"d");
    assert_eq!(all[3].record_id.as_ref(), b"rec-Z");
    assert_eq!(all[3].start_key.as_ref(), b"x");
}

#[test]
fn test_iter_returns_sorted_order() {
    let mut idx = RangeTombstoneIndex::new();
    idx.add(tombstone(b"beta", b"z", b"", 1));
    idx.add(tombstone(b"alpha", b"b", b"d", 2));
    idx.add(tombstone(b"alpha", b"a", b"b", 3));
    idx.add(tombstone(b"beta", b"a", b"m", 4));

    let keys: Vec<(&[u8], &[u8])> = idx
        .iter()
        .map(|ts| (ts.record_id.as_ref(), ts.start_key.as_ref()))
        .collect();

    assert_eq!(
        keys,
        vec![
            (&b"alpha"[..], &b"a"[..]),
            (&b"alpha"[..], &b"b"[..]),
            (&b"beta"[..], &b"a"[..]),
            (&b"beta"[..], &b"z"[..]),
        ]
    );
}

// ── Edge Cases ──────────────────────────────────────────────────────

#[test]
fn test_empty_index_covers_nothing() {
    let idx = RangeTombstoneIndex::new();

    assert!(!idx.covers(b"rec1", b"a", 0));
    assert!(!idx.covers(b"rec1", b"", 0));
    assert!(!idx.covers(b"anything", b"anything", 0));
}

#[test]
fn test_single_key_range() {
    let mut idx = RangeTombstoneIndex::new();
    // Range [x, x\x01) — a tight range that covers "x" but not keys starting after "x\0"
    idx.add(tombstone(b"rec1", b"x", b"x\x01", 10));

    assert!(idx.covers(b"rec1", b"x", 5));
    assert!(idx.covers(b"rec1", b"x\0", 5)); // "x\0" is >= "x" and < "x\x01"
    assert!(!idx.covers(b"rec1", b"x\x01", 5)); // end is exclusive
    assert!(!idx.covers(b"rec1", b"w", 5));
    assert!(!idx.covers(b"rec1", b"y", 5));
}

#[test]
fn test_range_tombstone_index_default() {
    let idx = RangeTombstoneIndex::default();
    assert!(idx.is_empty());
    assert_eq!(idx.len(), 0);
    assert!(!idx.covers(b"rec1", b"a", 0));
}

#[test]
fn test_len_and_is_empty() {
    let mut idx = RangeTombstoneIndex::new();
    assert!(idx.is_empty());
    assert_eq!(idx.len(), 0);

    idx.add(tombstone(b"rec1", b"a", b"z", 10));
    assert!(!idx.is_empty());
    assert_eq!(idx.len(), 1);

    idx.add(tombstone(b"rec2", b"a", b"z", 20));
    assert_eq!(idx.len(), 2);
}
