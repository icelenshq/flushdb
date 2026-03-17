use std::cmp::Ordering;
use std::collections::BinaryHeap;

use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, EntryValue, MemtableEntry};

use crate::skiplist::SkipNode;
use crate::sstable::BlockEntry;

// --- MergeEntry ---

#[derive(Clone, Debug)]
pub struct MergeEntry {
    pub composite_key: CompositeKey,
    pub value: Bytes,
    pub metadata: Bytes,
    pub entry_type: EntryType,
    pub sequence_number: u64,
}

impl MergeEntry {
    pub fn from_memtable_entry(entry: &MemtableEntry) -> Self {
        Self {
            composite_key: entry.composite_key.clone(),
            value: entry.value.clone(),
            metadata: entry.metadata.clone(),
            entry_type: entry.entry_type,
            sequence_number: entry.sequence_number,
        }
    }

    pub fn from_skip_node(node: &SkipNode) -> Self {
        Self {
            composite_key: node.key.clone(),
            value: node.value.clone(),
            metadata: node.metadata.clone(),
            entry_type: node.entry_type,
            sequence_number: node.sequence_number,
        }
    }

    pub fn from_block_entry(entry: BlockEntry) -> Self {
        let value = match entry.value {
            EntryValue::Inline(b) => b,
            EntryValue::BlobRef { .. } => Bytes::new(),
        };
        Self {
            composite_key: entry.composite_key,
            value,
            metadata: entry.metadata,
            entry_type: entry.entry_type,
            sequence_number: entry.sequence_number,
        }
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.entry_type, EntryType::Delete | EntryType::RangeDelete)
    }

    pub fn is_put(&self) -> bool {
        matches!(self.entry_type, EntryType::Put)
    }
}

// --- MergeSource ---

pub trait MergeSource: Send {
    fn peek(&self) -> Option<&MergeEntry>;
    fn advance(&mut self);
    fn source_id(&self) -> usize;
}

// --- VecSource ---

pub struct VecSource {
    entries: Vec<MergeEntry>,
    pos: usize,
    source_id: usize,
}

impl VecSource {
    pub fn new(entries: Vec<MergeEntry>, source_id: usize) -> Self {
        Self {
            entries,
            pos: 0,
            source_id,
        }
    }
}

impl MergeSource for VecSource {
    fn peek(&self) -> Option<&MergeEntry> {
        self.entries.get(self.pos)
    }

    fn advance(&mut self) {
        if self.pos < self.entries.len() {
            self.pos += 1;
        }
    }

    fn source_id(&self) -> usize {
        self.source_id
    }
}

// --- HeapItem ---

struct HeapItem {
    key: CompositeKey,
    seq: u64,
    source_idx: usize,
    source_id: usize,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq && self.source_id == other.source_id
    }
}

impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// BinaryHeap is a max-heap. We reverse key ordering for min-heap by key,
// but keep sequence_number in natural order so the newest entry (highest seq)
// is popped first when keys are equal.
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        match other.key.cmp(&self.key) {
            Ordering::Equal => match self.seq.cmp(&other.seq) {
                Ordering::Equal => other.source_id.cmp(&self.source_id),
                ord => ord,
            },
            ord => ord,
        }
    }
}

// --- MergeIterator ---

pub struct MergeIterator {
    heap: BinaryHeap<HeapItem>,
    sources: Vec<Box<dyn MergeSource>>,
}

impl MergeIterator {
    pub fn new(sources: Vec<Box<dyn MergeSource>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (idx, source) in sources.iter().enumerate() {
            if let Some(entry) = source.peek() {
                heap.push(HeapItem {
                    key: entry.composite_key.clone(),
                    seq: entry.sequence_number,
                    source_idx: idx,
                    source_id: source.source_id(),
                });
            }
        }

        Self { heap, sources }
    }

    pub fn next_entry(&mut self) -> Option<MergeEntry> {
        let item = self.heap.pop()?;
        let source = &mut self.sources[item.source_idx];
        let entry = source.peek().expect("heap item from exhausted source").clone();
        source.advance();

        // Re-push if source has more entries
        if let Some(next) = source.peek() {
            self.heap.push(HeapItem {
                key: next.composite_key.clone(),
                seq: next.sequence_number,
                source_idx: item.source_idx,
                source_id: item.source_id,
            });
        }

        Some(entry)
    }

    pub fn next_deduped(&mut self) -> Option<MergeEntry> {
        let entry = self.next_entry()?;

        // Skip all subsequent entries with the same composite_key
        while let Some(top) = self.heap.peek() {
            if top.key == entry.composite_key {
                self.next_entry();
            } else {
                break;
            }
        }

        Some(entry)
    }

    pub fn is_exhausted(&self) -> bool {
        self.heap.is_empty()
    }
}
