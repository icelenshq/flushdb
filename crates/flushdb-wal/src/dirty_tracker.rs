use std::collections::HashMap;
use std::time::{Duration, Instant};

pub struct DirtySegmentTracker {
    dirty_maps: HashMap<u64, HashMap<u64, u64>>,
    segment_created_at: HashMap<u64, Instant>,
}

impl Default for DirtySegmentTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl DirtySegmentTracker {
    pub fn new() -> Self {
        Self {
            dirty_maps: HashMap::new(),
            segment_created_at: HashMap::new(),
        }
    }

    pub fn record_write(
        &mut self,
        segment_number: u64,
        generation_id: u64,
        sequence_number: u64,
    ) {
        self.segment_created_at
            .entry(segment_number)
            .or_insert_with(Instant::now);

        let gen_map = self.dirty_maps.entry(segment_number).or_default();
        let current = gen_map.entry(generation_id).or_insert(0);
        if sequence_number > *current {
            *current = sequence_number;
        }
    }

    pub fn mark_generation_flushed(&mut self, generation_id: u64) -> Vec<u64> {
        let mut newly_clean = Vec::new();

        let segment_nums: Vec<u64> = self.dirty_maps.keys().copied().collect();
        for seg_num in segment_nums {
            if let Some(gen_map) = self.dirty_maps.get_mut(&seg_num) {
                let was_present = gen_map.remove(&generation_id).is_some();
                if was_present && gen_map.is_empty() {
                    newly_clean.push(seg_num);
                }
            }
        }

        newly_clean.sort();
        newly_clean
    }

    pub fn is_segment_clean(&self, segment_number: u64) -> bool {
        match self.dirty_maps.get(&segment_number) {
            None => true,
            Some(gen_map) => gen_map.is_empty(),
        }
    }

    pub fn deletable_segments(&self) -> Vec<u64> {
        let mut result: Vec<u64> = self
            .dirty_maps
            .iter()
            .filter(|(_, gen_map)| gen_map.is_empty())
            .map(|(&seg_num, _)| seg_num)
            .collect();
        result.sort();
        result
    }

    pub fn dirty_generation_ids(&self, segment_number: u64) -> Vec<u64> {
        match self.dirty_maps.get(&segment_number) {
            None => Vec::new(),
            Some(gen_map) => {
                let mut ids: Vec<u64> = gen_map.keys().copied().collect();
                ids.sort();
                ids
            }
        }
    }

    pub fn all_dirty_segments(&self) -> Vec<u64> {
        let mut result: Vec<u64> = self
            .dirty_maps
            .iter()
            .filter(|(_, gen_map)| !gen_map.is_empty())
            .map(|(&seg_num, _)| seg_num)
            .collect();
        result.sort();
        result
    }

    pub fn segments_older_than(&self, max_age: Duration) -> Vec<u64> {
        let now = Instant::now();
        let mut result: Vec<u64> = self
            .segment_created_at
            .iter()
            .filter(|(seg_num, created_at)| {
                now.duration_since(**created_at) > max_age
                    && self
                        .dirty_maps
                        .get(seg_num)
                        .is_some_and(|m| !m.is_empty())
            })
            .map(|(&seg_num, _)| seg_num)
            .collect();
        result.sort();
        result
    }

    pub fn oldest_pinned_segment(&self) -> Option<u64> {
        self.segment_created_at
            .iter()
            .filter(|(seg_num, _)| {
                self.dirty_maps
                    .get(seg_num)
                    .is_some_and(|m| !m.is_empty())
            })
            .min_by_key(|(_, created_at)| *created_at)
            .map(|(&seg_num, _)| seg_num)
    }

    pub fn generations_for_segment(&self, segment_number: u64) -> Vec<u64> {
        self.dirty_generation_ids(segment_number)
    }

    pub fn remove_segment(&mut self, segment_number: u64) {
        self.dirty_maps.remove(&segment_number);
        self.segment_created_at.remove(&segment_number);
    }

    pub fn needs_age_flush(&self, max_age: Duration) -> bool {
        let now = Instant::now();
        self.segment_created_at.iter().any(|(seg_num, created_at)| {
            now.duration_since(*created_at) > max_age
                && self
                    .dirty_maps
                    .get(seg_num)
                    .is_some_and(|m| !m.is_empty())
        })
    }

    pub fn needs_size_flush(&self, total_wal_size: u64, threshold: u64) -> bool {
        total_wal_size > threshold
    }
}
