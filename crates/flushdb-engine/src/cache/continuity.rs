use std::collections::HashMap;

use bytes::Bytes;

use crate::manifest::ManifestId;

use super::CacheConfig;

#[derive(Clone, Debug)]
pub struct ContinuityInterval {
    pub start_key: Bytes,
    pub end_key: Bytes,
    pub manifest_id: ManifestId,
}

pub struct ContinuityTracker {
    intervals: HashMap<Bytes, Vec<ContinuityInterval>>,
    enabled: bool,
    max_records: usize,
    max_intervals_per_record: usize,
}

impl ContinuityTracker {
    pub fn new(config: &CacheConfig) -> Self {
        Self {
            intervals: HashMap::new(),
            enabled: config.enable_continuity_tracking,
            max_records: 10_000,
            max_intervals_per_record: 100,
        }
    }

    pub fn mark_range_complete(
        &mut self,
        record_id: &[u8],
        start_key: &[u8],
        end_key: &[u8],
        manifest_id: ManifestId,
    ) {
        if !self.enabled {
            return;
        }

        let record_key = Bytes::copy_from_slice(record_id);
        let new_interval = ContinuityInterval {
            start_key: Bytes::copy_from_slice(start_key),
            end_key: Bytes::copy_from_slice(end_key),
            manifest_id,
        };

        let intervals = self.intervals.entry(record_key).or_default();
        intervals.push(new_interval);
        merge_intervals(intervals);
        self.enforce_max_intervals_per_record(record_id);
        self.enforce_max_records();
    }

    pub fn is_known_absent(
        &self,
        record_id: &[u8],
        item_key: &[u8],
        current_manifest_id: ManifestId,
    ) -> bool {
        if !self.enabled {
            return false;
        }

        let Some(intervals) = self.intervals.get(record_id) else {
            return false;
        };

        intervals.iter().any(|interval| {
            interval.manifest_id == current_manifest_id && key_in_range(item_key, interval)
        })
    }

    pub fn invalidate_for_record(&mut self, record_id: &[u8]) {
        self.intervals.remove(record_id);
    }

    pub fn invalidate_before_manifest(&mut self, manifest_id: ManifestId) {
        for intervals in self.intervals.values_mut() {
            intervals.retain(|interval| interval.manifest_id >= manifest_id);
        }
        self.intervals.retain(|_, intervals| !intervals.is_empty());
    }

    pub fn invalidate_all(&mut self) {
        self.intervals.clear();
    }

    pub fn tracked_record_count(&self) -> usize {
        self.intervals.len()
    }

    pub fn total_interval_count(&self) -> usize {
        self.intervals.values().map(|v| v.len()).sum()
    }

    fn enforce_max_intervals_per_record(&mut self, record_id: &[u8]) {
        let Some(intervals) = self.intervals.get_mut(record_id) else {
            return;
        };

        if intervals.len() <= self.max_intervals_per_record {
            return;
        }

        intervals.sort_by_key(|i| i.manifest_id);
        let excess = intervals.len() - self.max_intervals_per_record;
        intervals.drain(..excess);
    }

    fn enforce_max_records(&mut self) {
        if self.intervals.len() <= self.max_records {
            return;
        }

        while self.intervals.len() > self.max_records {
            let oldest_record = self
                .intervals
                .iter()
                .map(|(key, intervals)| {
                    let oldest_manifest = intervals
                        .iter()
                        .map(|i| i.manifest_id)
                        .min()
                        .unwrap_or(ManifestId::ZERO);
                    (key.clone(), oldest_manifest)
                })
                .min_by_key(|(_, manifest_id)| *manifest_id)
                .map(|(key, _)| key);

            if let Some(key) = oldest_record {
                self.intervals.remove(&key);
            } else {
                break;
            }
        }
    }
}

fn key_in_range(key: &[u8], interval: &ContinuityInterval) -> bool {
    let gte_start = interval.start_key.is_empty() || key >= interval.start_key.as_ref();
    let lt_end = interval.end_key.is_empty() || key < interval.end_key.as_ref();
    gte_start && lt_end
}

fn overlaps_or_adjacent(a: &ContinuityInterval, b: &ContinuityInterval) -> bool {
    let a_end_gte_b_start = a.end_key.is_empty()
        || b.start_key.is_empty()
        || a.end_key.as_ref() >= b.start_key.as_ref();
    let b_end_gte_a_start = b.end_key.is_empty()
        || a.start_key.is_empty()
        || b.end_key.as_ref() >= a.start_key.as_ref();
    a_end_gte_b_start && b_end_gte_a_start
}

fn merged_bounds(a: &ContinuityInterval, b: &ContinuityInterval) -> (Bytes, Bytes) {
    let start = if a.start_key.is_empty() || b.start_key.is_empty() {
        Bytes::new()
    } else if a.start_key.as_ref() <= b.start_key.as_ref() {
        a.start_key.clone()
    } else {
        b.start_key.clone()
    };

    let end = if a.end_key.is_empty() || b.end_key.is_empty() {
        Bytes::new()
    } else if a.end_key.as_ref() >= b.end_key.as_ref() {
        a.end_key.clone()
    } else {
        b.end_key.clone()
    };

    (start, end)
}

fn merge_intervals(intervals: &mut Vec<ContinuityInterval>) {
    if intervals.len() <= 1 {
        return;
    }

    intervals.sort_by(|a, b| {
        if a.start_key.is_empty() && !b.start_key.is_empty() {
            std::cmp::Ordering::Less
        } else if !a.start_key.is_empty() && b.start_key.is_empty() {
            std::cmp::Ordering::Greater
        } else {
            a.start_key.as_ref().cmp(b.start_key.as_ref())
        }
    });

    let mut merged: Vec<ContinuityInterval> = Vec::new();

    for interval in intervals.drain(..) {
        if let Some(last) = merged.last_mut() {
            if overlaps_or_adjacent(last, &interval) {
                let (start, end) = merged_bounds(last, &interval);
                let manifest_id = std::cmp::max(last.manifest_id, interval.manifest_id);
                last.start_key = start;
                last.end_key = end;
                last.manifest_id = manifest_id;
                continue;
            }
        }
        merged.push(interval);
    }

    *intervals = merged;
}
