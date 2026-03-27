use std::collections::HashSet;
use std::time::{Duration, Instant};

use bytes::Bytes;
use flushdb_types::{
    CompositeKey, EntryType, FlushError, FlushResult, IdempotencyToken, MemtableEntry,
};

use crate::range_tombstone::{RangeTombstone, RangeTombstoneIndex};
use crate::skiplist::{SkipList, SkipNode};

#[derive(Debug, Clone)]
pub struct MemtableConfig {
    pub size_threshold: usize,
    pub max_frozen_count: usize,
    pub memtable_memory_limit: usize,
}

impl Default for MemtableConfig {
    fn default() -> Self {
        let size_threshold = 67_108_864;
        let max_frozen_count = 3;
        Self {
            size_threshold,
            max_frozen_count,
            memtable_memory_limit: (max_frozen_count + 1) * size_threshold,
        }
    }
}

pub struct DedupSet {
    tokens: HashSet<IdempotencyToken>,
}

impl DedupSet {
    pub fn new() -> Self {
        Self {
            tokens: HashSet::new(),
        }
    }

    pub fn contains(&self, token: &IdempotencyToken) -> bool {
        if token.is_none() {
            return false;
        }
        self.tokens.contains(token)
    }

    pub fn insert(&mut self, token: IdempotencyToken) {
        if token.is_none() {
            return;
        }
        self.tokens.insert(token);
    }

    pub fn len(&self) -> usize {
        self.tokens.len()
    }

    pub fn is_empty(&self) -> bool {
        self.tokens.is_empty()
    }
}

impl Default for DedupSet {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Memtable {
    skiplist: SkipList,
    range_tombstones: RangeTombstoneIndex,
    dedup_set: DedupSet,
    next_sequence_number: u64,
    config: MemtableConfig,
    created_at: Instant,
    frozen: bool,
}

fn filter_scan_results(entries: Vec<MemtableEntry>) -> Vec<MemtableEntry> {
    entries
        .into_iter()
        .filter(|e| !matches!(e.entry_type, EntryType::Delete | EntryType::RangeDelete))
        .collect()
}

fn node_to_entry(node: &SkipNode) -> MemtableEntry {
    MemtableEntry::with_sequence(
        node.key.clone(),
        node.value.clone(),
        node.metadata.clone(),
        node.idempotency_key,
        node.sequence_number,
        node.entry_type,
    )
}

impl Memtable {
    pub fn new(config: MemtableConfig, starting_sequence: u64) -> Self {
        Self {
            skiplist: SkipList::new(),
            range_tombstones: RangeTombstoneIndex::new(),
            dedup_set: DedupSet::new(),
            next_sequence_number: starting_sequence,
            config,
            created_at: Instant::now(),
            frozen: false,
        }
    }

    pub fn insert(&mut self, entry: MemtableEntry) -> FlushResult<u64> {
        if self.frozen {
            return Err(FlushError::ResourceExhausted {
                resource: "memtable".into(),
                message: "memtable is frozen".into(),
            });
        }

        if self.dedup_set.contains(&entry.idempotency_key) {
            return Err(FlushError::DuplicateToken {
                token: format!("{:?}", entry.idempotency_key),
            });
        }

        self.insert_prechecked(entry)
    }

    pub(crate) fn insert_prechecked(&mut self, mut entry: MemtableEntry) -> FlushResult<u64> {
        if self.frozen {
            return Err(FlushError::ResourceExhausted {
                resource: "memtable".into(),
                message: "memtable is frozen".into(),
            });
        }

        let seq = self.next_sequence_number;
        entry.sequence_number = seq;
        self.next_sequence_number += 1;

        self.insert_inner(entry);

        Ok(seq)
    }

    /// Insert an entry whose sequence number was already assigned by the engine.
    /// Used by the batch write path where the engine is the sole authority on
    /// sequence numbers (they must match what was written to the WAL).
    pub(crate) fn insert_with_assigned_sequence(
        &mut self,
        entry: MemtableEntry,
    ) -> FlushResult<u64> {
        if self.frozen {
            return Err(FlushError::ResourceExhausted {
                resource: "memtable".into(),
                message: "memtable is frozen".into(),
            });
        }

        let seq = entry.sequence_number;
        if seq >= self.next_sequence_number {
            self.next_sequence_number = seq + 1;
        }

        self.insert_inner(entry);

        Ok(seq)
    }

    fn insert_inner(&mut self, entry: MemtableEntry) {
        if entry.entry_type == EntryType::RangeDelete {
            let item_key = entry.composite_key.item_key();
            let start_key = if item_key.is_empty() {
                Bytes::new()
            } else {
                Bytes::copy_from_slice(&item_key[1..])
            };
            self.range_tombstones.add(RangeTombstone {
                record_id: Bytes::copy_from_slice(entry.record_id()),
                start_key,
                end_key: entry.value.clone(),
                sequence_number: entry.sequence_number,
            });
        }

        self.dedup_set.insert(entry.idempotency_key);
        self.skiplist.insert(entry);
    }

    pub fn get(&self, key: &CompositeKey) -> Option<MemtableEntry> {
        let node = self.skiplist.get(key)?;

        if self.range_tombstones.covers(
            node.key.record_id(),
            node.key.item_key(),
            node.sequence_number,
        ) {
            return None;
        }

        Some(node_to_entry(node))
    }

    pub fn scan(&self, start: &CompositeKey, end: &CompositeKey) -> Vec<MemtableEntry> {
        filter_scan_results(self.scan_with_tombstones(start, end))
    }

    pub fn scan_record(&self, record_id: &[u8]) -> Vec<MemtableEntry> {
        filter_scan_results(self.scan_record_with_tombstones(record_id))
    }

    /// Returns deduplicated entries including tombstones — used by MemtableList
    /// for correct cross-memtable merge before tombstone filtering.
    pub(crate) fn scan_with_tombstones(
        &self,
        start: &CompositeKey,
        end: &CompositeKey,
    ) -> Vec<MemtableEntry> {
        self.dedup_and_check_range_tombstones(self.skiplist.range(start, end))
    }

    /// Returns deduplicated entries including tombstones — used by MemtableList.
    pub(crate) fn scan_record_with_tombstones(&self, record_id: &[u8]) -> Vec<MemtableEntry> {
        self.dedup_and_check_range_tombstones(self.skiplist.scan_record(record_id))
    }

    fn dedup_and_check_range_tombstones<'a>(
        &self,
        iter: impl Iterator<Item = &'a SkipNode>,
    ) -> Vec<MemtableEntry> {
        let mut results = Vec::new();
        let mut last_key: Option<CompositeKey> = None;

        for node in iter {
            if last_key.as_ref() == Some(&node.key) {
                continue;
            }
            last_key = Some(node.key.clone());

            if self.range_tombstones.covers(
                node.key.record_id(),
                node.key.item_key(),
                node.sequence_number,
            ) {
                continue;
            }
            results.push(node_to_entry(node));
        }
        results
    }

    pub fn iter(&self) -> impl Iterator<Item = &SkipNode> + '_ {
        self.skiplist.iter()
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.skiplist.approximate_memory_usage()
    }

    pub fn should_freeze_by_size(&self) -> bool {
        self.approximate_memory_usage() >= self.config.size_threshold
    }

    pub fn should_freeze_by_age(&self, max_age: Duration) -> bool {
        self.created_at.elapsed() >= max_age
    }

    pub fn entry_count(&self) -> usize {
        self.skiplist.len()
    }

    pub fn next_sequence_number(&self) -> u64 {
        self.next_sequence_number
    }

    pub fn is_frozen(&self) -> bool {
        self.frozen
    }

    pub fn freeze(&mut self) {
        self.frozen = true;
    }

    pub fn is_empty(&self) -> bool {
        self.skiplist.is_empty()
    }

    pub fn range_tombstone_count(&self) -> usize {
        self.range_tombstones.len()
    }

    pub fn check_dedup(&self, token: &IdempotencyToken) -> bool {
        self.dedup_set.contains(token)
    }

    pub fn into_skiplist(self) -> SkipList {
        debug_assert!(self.frozen, "into_skiplist called on active memtable");
        self.skiplist
    }

    pub fn range_tombstones(&self) -> &RangeTombstoneIndex {
        &self.range_tombstones
    }
}
