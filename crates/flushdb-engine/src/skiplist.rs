use std::cmp::Ordering;
use std::collections::VecDeque;

use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, IdempotencyToken, MemtableEntry};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::arena::Arena;

pub const MAX_HEIGHT: usize = 12;
const BRANCHING_FACTOR: u32 = 4;

pub struct SkipNode {
    pub key: CompositeKey,
    pub sequence_number: u64,
    pub value: Bytes,
    pub metadata: Bytes,
    pub entry_type: EntryType,
    pub idempotency_key: IdempotencyToken,
    height: usize,
    next: [Option<usize>; MAX_HEIGHT],
}

impl SkipNode {
    pub fn height(&self) -> usize {
        self.height
    }

    fn new(entry: MemtableEntry, height: usize) -> Self {
        Self {
            key: entry.composite_key,
            sequence_number: entry.sequence_number,
            value: entry.value,
            metadata: entry.metadata,
            entry_type: entry.entry_type,
            idempotency_key: entry.idempotency_key,
            height,
            next: [None; MAX_HEIGHT],
        }
    }

    fn sentinel() -> Self {
        let sentinel_key =
            CompositeKey::new(b"__sentinel__", b"").expect("sentinel key is always valid");
        Self {
            key: sentinel_key,
            sequence_number: 0,
            value: Bytes::new(),
            metadata: Bytes::new(),
            entry_type: EntryType::Put,
            idempotency_key: IdempotencyToken::none(),
            height: MAX_HEIGHT,
            next: [None; MAX_HEIGHT],
        }
    }
}

fn compare_entries(a_key: &CompositeKey, a_seq: u64, b_key: &CompositeKey, b_seq: u64) -> Ordering {
    match a_key.cmp(b_key) {
        Ordering::Equal => b_seq.cmp(&a_seq),
        other => other,
    }
}

fn random_height(rng: &mut SmallRng) -> usize {
    let mut height = 1;
    while height < MAX_HEIGHT && rng.random_range(0..BRANCHING_FACTOR) == 0 {
        height += 1;
    }
    height
}

fn approximate_entry_size(entry: &MemtableEntry) -> usize {
    std::mem::size_of::<SkipNode>()
        + entry.composite_key.as_bytes().len()
        + entry.value.len()
        + entry.metadata.len()
}

pub struct SkipList {
    nodes: Vec<SkipNode>,
    head: usize,
    height: usize,
    len: usize,
    arena: Arena,
    rng: SmallRng,
}

impl SkipList {
    pub fn new() -> Self {
        Self {
            nodes: vec![SkipNode::sentinel()],
            head: 0,
            height: 1,
            len: 0,
            arena: Arena::new(),
            rng: SmallRng::from_os_rng(),
        }
    }

    pub fn with_arena(arena: Arena) -> Self {
        Self {
            nodes: vec![SkipNode::sentinel()],
            head: 0,
            height: 1,
            len: 0,
            arena,
            rng: SmallRng::from_os_rng(),
        }
    }

    #[cfg(test)]
    pub fn with_seed(seed: u64) -> Self {
        Self {
            nodes: vec![SkipNode::sentinel()],
            head: 0,
            height: 1,
            len: 0,
            arena: Arena::new(),
            rng: SmallRng::seed_from_u64(seed),
        }
    }

    pub fn insert(&mut self, entry: MemtableEntry) {
        let entry_size = approximate_entry_size(&entry);
        let height = random_height(&mut self.rng);

        if height > self.height {
            self.height = height;
        }

        let mut update = [self.head; MAX_HEIGHT];
        let mut current = self.head;

        for level in (0..self.height).rev() {
            while let Some(next_idx) = self.nodes[current].next[level] {
                let cmp = compare_entries(
                    &self.nodes[next_idx].key,
                    self.nodes[next_idx].sequence_number,
                    &entry.composite_key,
                    entry.sequence_number,
                );
                if cmp == Ordering::Less {
                    current = next_idx;
                } else {
                    break;
                }
            }
            update[level] = current;
        }

        if let Some(next_idx) = self.nodes[update[0]].next[0] {
            if self.nodes[next_idx].key == entry.composite_key
                && self.nodes[next_idx].sequence_number == entry.sequence_number
            {
                self.nodes[next_idx].value = entry.value;
                self.nodes[next_idx].metadata = entry.metadata;
                self.nodes[next_idx].entry_type = entry.entry_type;
                self.nodes[next_idx].idempotency_key = entry.idempotency_key;
                return;
            }
        }

        let new_idx = self.nodes.len();
        self.nodes.push(SkipNode::new(entry, height));

        for (level, &prev_idx) in update.iter().enumerate().take(height) {
            self.nodes[new_idx].next[level] = self.nodes[prev_idx].next[level];
            self.nodes[prev_idx].next[level] = Some(new_idx);
        }

        self.len += 1;
        let _ = self.arena.allocate(entry_size);
    }

    pub fn get(&self, key: &CompositeKey) -> Option<&SkipNode> {
        let mut current = self.head;

        for level in (0..self.height).rev() {
            while let Some(next_idx) = self.nodes[current].next[level] {
                match self.nodes[next_idx].key.cmp(key) {
                    Ordering::Less => current = next_idx,
                    Ordering::Equal | Ordering::Greater => break,
                }
            }
        }

        if let Some(next_idx) = self.nodes[current].next[0] {
            if self.nodes[next_idx].key == *key {
                return Some(&self.nodes[next_idx]);
            }
        }
        None
    }

    pub fn contains_key(&self, key: &CompositeKey) -> bool {
        self.get(key).is_some()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn approximate_memory_usage(&self) -> usize {
        self.arena.total_allocated()
    }

    fn seek_to(&self, key: &CompositeKey) -> Option<usize> {
        let mut current = self.head;

        for level in (0..self.height).rev() {
            while let Some(next_idx) = self.nodes[current].next[level] {
                if self.nodes[next_idx].key < *key {
                    current = next_idx;
                } else {
                    break;
                }
            }
        }

        self.nodes[current].next[0]
    }

    pub fn iter(&self) -> SkipListIterator<'_> {
        SkipListIterator {
            skiplist: self,
            current: self.nodes[self.head].next[0],
            stop: StopCondition::None,
        }
    }

    pub fn range<'a>(
        &'a self,
        start: &CompositeKey,
        end: &'a CompositeKey,
    ) -> SkipListIterator<'a> {
        let current = self.seek_to(start);
        SkipListIterator {
            skiplist: self,
            current,
            stop: StopCondition::BeforeKey(end),
        }
    }

    pub fn range_from(&self, start: &CompositeKey) -> SkipListIterator<'_> {
        let current = self.seek_to(start);
        SkipListIterator {
            skiplist: self,
            current,
            stop: StopCondition::None,
        }
    }

    pub fn scan_record<'a>(&'a self, record_id: &'a [u8]) -> SkipListIterator<'a> {
        let min_key = match CompositeKey::min_key_for_record(record_id) {
            Ok(k) => k,
            Err(_) => {
                return SkipListIterator {
                    skiplist: self,
                    current: None,
                    stop: StopCondition::None,
                }
            }
        };
        let current = self.seek_to(&min_key);
        SkipListIterator {
            skiplist: self,
            current,
            stop: StopCondition::RecordBoundary(record_id),
        }
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl IntoIterator for SkipList {
    type Item = SkipNode;
    type IntoIter = SkipListIntoIterator;

    fn into_iter(self) -> SkipListIntoIterator {
        let mut idx_order = Vec::with_capacity(self.len);
        let mut current = self.nodes[self.head].next[0];
        while let Some(idx) = current {
            idx_order.push(idx);
            current = self.nodes[idx].next[0];
        }

        let mut nodes_map: Vec<Option<SkipNode>> = self.nodes.into_iter().map(Some).collect();
        let mut sorted_nodes = VecDeque::with_capacity(idx_order.len());
        for idx in idx_order {
            if let Some(node) = nodes_map[idx].take() {
                sorted_nodes.push_back(node);
            }
        }

        SkipListIntoIterator { sorted_nodes }
    }
}

enum StopCondition<'a> {
    None,
    BeforeKey(&'a CompositeKey),
    RecordBoundary(&'a [u8]),
}

pub struct SkipListIterator<'a> {
    skiplist: &'a SkipList,
    current: Option<usize>,
    stop: StopCondition<'a>,
}

impl<'a> Iterator for SkipListIterator<'a> {
    type Item = &'a SkipNode;

    fn next(&mut self) -> Option<Self::Item> {
        let idx = self.current?;
        let node = &self.skiplist.nodes[idx];

        match &self.stop {
            StopCondition::None => {}
            StopCondition::BeforeKey(end_key) => {
                if node.key >= **end_key {
                    self.current = None;
                    return None;
                }
            }
            StopCondition::RecordBoundary(record_id) => {
                if node.key.record_id() != *record_id {
                    self.current = None;
                    return None;
                }
            }
        }

        self.current = node.next[0];
        Some(node)
    }
}

pub struct SkipListIntoIterator {
    sorted_nodes: VecDeque<SkipNode>,
}

impl Iterator for SkipListIntoIterator {
    type Item = SkipNode;

    fn next(&mut self) -> Option<Self::Item> {
        self.sorted_nodes.pop_front()
    }
}
