use bytes::Bytes;
use std::cmp::Ordering;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeTombstone {
    pub record_id: Bytes,
    pub start_key: Bytes,
    pub end_key: Bytes,
    pub sequence_number: u64,
}

fn compare_tombstones(a: &RangeTombstone, b: &RangeTombstone) -> Ordering {
    match a.record_id.as_ref().cmp(b.record_id.as_ref()) {
        Ordering::Equal => a.start_key.as_ref().cmp(b.start_key.as_ref()),
        other => other,
    }
}

pub struct RangeTombstoneIndex {
    tombstones: Vec<RangeTombstone>,
}

impl RangeTombstoneIndex {
    pub fn new() -> Self {
        Self {
            tombstones: Vec::new(),
        }
    }

    pub fn add(&mut self, tombstone: RangeTombstone) {
        let pos = self
            .tombstones
            .binary_search_by(|existing| compare_tombstones(existing, &tombstone))
            .unwrap_or_else(|i| i);
        self.tombstones.insert(pos, tombstone);
    }

    pub fn covers(&self, record_id: &[u8], item_key: &[u8], entry_sequence: u64) -> bool {
        for ts in self.tombstones_for_record(record_id) {
            if ts.start_key.as_ref() <= item_key {
                let in_range = ts.end_key.is_empty() || item_key < ts.end_key.as_ref();
                if in_range && ts.sequence_number > entry_sequence {
                    return true;
                }
            }
            if ts.start_key.as_ref() > item_key {
                break;
            }
        }
        false
    }

    pub fn tombstones_for_record(&self, record_id: &[u8]) -> &[RangeTombstone] {
        // Binary search to find any tombstone with this record_id.
        // Since tombstones are sorted by (record_id, start_key), all tombstones
        // for a given record_id form a contiguous run.
        let search_result = self
            .tombstones
            .binary_search_by(|ts| ts.record_id.as_ref().cmp(record_id));

        let anchor = match search_result {
            Ok(idx) => idx,
            Err(_) => return &[],
        };

        // Walk backward to find the first tombstone for this record_id
        let start = {
            let mut i = anchor;
            while i > 0 && self.tombstones[i - 1].record_id.as_ref() == record_id {
                i -= 1;
            }
            i
        };

        // Walk forward to find the last tombstone for this record_id
        let end = {
            let mut i = anchor + 1;
            while i < self.tombstones.len() && self.tombstones[i].record_id.as_ref() == record_id {
                i += 1;
            }
            i
        };

        &self.tombstones[start..end]
    }

    pub fn len(&self) -> usize {
        self.tombstones.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tombstones.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &RangeTombstone> {
        self.tombstones.iter()
    }
}

impl Default for RangeTombstoneIndex {
    fn default() -> Self {
        Self::new()
    }
}
