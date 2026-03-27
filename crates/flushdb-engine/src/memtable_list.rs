use std::cmp::Ordering;
use std::time::Duration;

use flushdb_types::{
    CompositeKey, EntryType, FlushError, FlushResult, IdempotencyToken, MemtableEntry,
};

use crate::memtable::{Memtable, MemtableConfig};
use crate::range_tombstone::RangeTombstone;

pub struct MemtableList {
    active: Memtable,
    frozen: Vec<Memtable>,
    frozen_generation_ids: Vec<u64>,
    config: MemtableConfig,
}

impl MemtableList {
    pub fn new(config: MemtableConfig, starting_sequence: u64) -> Self {
        Self {
            active: Memtable::new(config.clone(), starting_sequence),
            frozen: Vec::new(),
            frozen_generation_ids: Vec::new(),
            config,
        }
    }

    pub fn check_dedup(&self, token: &IdempotencyToken) -> FlushResult<()> {
        if token.is_none() {
            return Ok(());
        }
        if self.active.check_dedup(token) {
            return Err(FlushError::DuplicateToken {
                token: format!("{:?}", token),
            });
        }
        for frozen_mt in &self.frozen {
            if frozen_mt.check_dedup(token) {
                return Err(FlushError::DuplicateToken {
                    token: format!("{:?}", token),
                });
            }
        }
        Ok(())
    }

    pub fn insert(&mut self, entry: MemtableEntry) -> FlushResult<u64> {
        if !entry.idempotency_key.is_none() {
            if self.active.check_dedup(&entry.idempotency_key) {
                return Err(FlushError::DuplicateToken {
                    token: format!("{:?}", entry.idempotency_key),
                });
            }
            for frozen_mt in &self.frozen {
                if frozen_mt.check_dedup(&entry.idempotency_key) {
                    return Err(FlushError::DuplicateToken {
                        token: format!("{:?}", entry.idempotency_key),
                    });
                }
            }
        }
        self.active.insert(entry)
    }

    pub(crate) fn insert_prechecked(&mut self, entry: MemtableEntry) -> FlushResult<u64> {
        self.active.insert_with_assigned_sequence(entry)
    }

    pub fn get(&self, key: &CompositeKey) -> Option<MemtableEntry> {
        if let Some(entry) = self.active.get(key) {
            return Some(entry);
        }
        for frozen_mt in &self.frozen {
            if let Some(entry) = frozen_mt.get(key) {
                return Some(entry);
            }
        }
        None
    }

    pub fn scan(&self, start: &CompositeKey, end: &CompositeKey) -> Vec<MemtableEntry> {
        let mut all: Vec<MemtableEntry> = Vec::new();
        all.extend(self.active.scan_with_tombstones(start, end));
        for frozen_mt in &self.frozen {
            all.extend(frozen_mt.scan_with_tombstones(start, end));
        }
        merge_and_filter(all)
    }

    pub fn scan_record(&self, record_id: &[u8]) -> Vec<MemtableEntry> {
        let mut all: Vec<MemtableEntry> = Vec::new();
        all.extend(self.active.scan_record_with_tombstones(record_id));
        for frozen_mt in &self.frozen {
            all.extend(frozen_mt.scan_record_with_tombstones(record_id));
        }
        merge_and_filter(all)
    }

    pub fn freeze_active(&mut self) -> FlushResult<()> {
        self.freeze_active_with_generation(0)
    }

    pub(crate) fn freeze_active_with_generation(&mut self, generation_id: u64) -> FlushResult<()> {
        if self.frozen.len() >= self.config.max_frozen_count {
            return Err(FlushError::ResourceExhausted {
                resource: "memtable".into(),
                message: format!(
                    "too many frozen memtables: {}/{}",
                    self.frozen.len(),
                    self.config.max_frozen_count
                ),
            });
        }
        let next_seq = self.active.next_sequence_number();
        self.active.freeze();
        let old_active = std::mem::replace(
            &mut self.active,
            Memtable::new(self.config.clone(), next_seq),
        );
        self.frozen.insert(0, old_active);
        self.frozen_generation_ids.insert(0, generation_id);
        Ok(())
    }

    pub fn should_freeze(&self, max_age: Duration) -> bool {
        self.active.should_freeze_by_size() || self.active.should_freeze_by_age(max_age)
    }

    pub fn pop_oldest_frozen(&mut self) -> Option<Memtable> {
        self.pop_oldest_frozen_with_generation()
            .map(|(_, memtable)| memtable)
    }

    pub(crate) fn pop_oldest_frozen_with_generation(&mut self) -> Option<(u64, Memtable)> {
        if self.frozen.is_empty() {
            debug_assert!(self.frozen_generation_ids.is_empty());
            None
        } else {
            let oldest_generation = self
                .frozen_generation_ids
                .pop()
                .expect("frozen generations must track frozen memtables");
            Some((oldest_generation, self.frozen.remove(self.frozen.len() - 1)))
        }
    }

    pub fn frozen_count(&self) -> usize {
        self.frozen.len()
    }

    pub fn has_frozen(&self) -> bool {
        !self.frozen.is_empty()
    }

    pub fn is_backpressured(&self) -> bool {
        self.frozen.len() >= self.config.max_frozen_count
    }

    pub fn is_memory_backpressured(&self) -> bool {
        self.total_memory_usage() >= self.config.memtable_memory_limit
    }

    pub fn active_memory_usage(&self) -> usize {
        self.active.approximate_memory_usage()
    }

    pub fn total_memory_usage(&self) -> usize {
        let mut total = self.active.approximate_memory_usage();
        for frozen_mt in &self.frozen {
            total += frozen_mt.approximate_memory_usage();
        }
        total
    }

    pub fn active_entry_count(&self) -> usize {
        self.active.entry_count()
    }

    pub fn total_entry_count(&self) -> usize {
        let mut total = self.active.entry_count();
        for frozen_mt in &self.frozen {
            total += frozen_mt.entry_count();
        }
        total
    }

    pub fn next_sequence_number(&self) -> u64 {
        self.active.next_sequence_number()
    }

    pub fn active(&self) -> &Memtable {
        &self.active
    }

    pub fn frozen(&self) -> &[Memtable] {
        &self.frozen
    }

    pub fn range_tombstone_covers(
        &self,
        record_id: &[u8],
        item_key: &[u8],
        entry_sequence: u64,
    ) -> bool {
        if self
            .active
            .range_tombstones()
            .covers(record_id, item_key, entry_sequence)
        {
            return true;
        }
        for frozen_mt in &self.frozen {
            if frozen_mt
                .range_tombstones()
                .covers(record_id, item_key, entry_sequence)
            {
                return true;
            }
        }
        false
    }

    pub fn all_range_tombstones(&self) -> Vec<RangeTombstone> {
        let mut tombstones = Vec::new();
        for ts in self.active.range_tombstones().iter() {
            tombstones.push(ts.clone());
        }
        for frozen_mt in &self.frozen {
            for ts in frozen_mt.range_tombstones().iter() {
                tombstones.push(ts.clone());
            }
        }
        tombstones
    }

    pub fn scan_all_with_tombstones(
        &self,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Vec<MemtableEntry> {
        let mut all: Vec<MemtableEntry> = Vec::new();
        all.extend(self.active.scan_with_tombstones(start, end));
        for frozen_mt in &self.frozen {
            all.extend(frozen_mt.scan_with_tombstones(start, end));
        }
        // Sort by (key ASC, seq DESC) for merge iterator
        all.sort_by(|a, b| match a.composite_key.cmp(&b.composite_key) {
            Ordering::Equal => b.sequence_number.cmp(&a.sequence_number),
            other => other,
        });
        // Dedup: keep only the highest-seq entry per key
        all.dedup_by(|b, a| a.composite_key == b.composite_key);
        all
    }
}

fn merge_and_filter(mut entries: Vec<MemtableEntry>) -> Vec<MemtableEntry> {
    // Sort by (key ASC, seq DESC) so highest seq for each key comes first
    entries.sort_by(|a, b| match a.composite_key.cmp(&b.composite_key) {
        Ordering::Equal => b.sequence_number.cmp(&a.sequence_number),
        other => other,
    });
    // Dedup: keep only the highest-seq entry per key
    entries.dedup_by(|b, a| a.composite_key == b.composite_key);
    // Filter out tombstones after cross-memtable dedup so DELETEs shadow older PUTs
    entries.retain(|e| !matches!(e.entry_type, EntryType::Delete | EntryType::RangeDelete));
    entries
}
