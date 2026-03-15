use bytes::Bytes;

use crate::composite_key::CompositeKey;
use crate::entry_type::EntryType;
use crate::idempotency_token::IdempotencyToken;

/// Internal entry stored in the memtable.
///
/// All fields are public — this is an internal data structure, not a public API boundary.
/// Intentionally does NOT implement `Ord`; ordering in the memtable is by `CompositeKey`
/// and `sequence_number`, managed externally.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemtableEntry {
    pub composite_key: CompositeKey,
    pub value: Bytes,
    pub metadata: Bytes,
    pub idempotency_key: IdempotencyToken,
    pub sequence_number: u64,
    pub entry_type: EntryType,
}

impl MemtableEntry {
    pub fn new(
        composite_key: CompositeKey,
        value: Bytes,
        metadata: Bytes,
        idempotency_key: IdempotencyToken,
        entry_type: EntryType,
    ) -> Self {
        Self {
            composite_key,
            value,
            metadata,
            idempotency_key,
            sequence_number: 0,
            entry_type,
        }
    }

    pub fn with_sequence(
        composite_key: CompositeKey,
        value: Bytes,
        metadata: Bytes,
        idempotency_key: IdempotencyToken,
        sequence_number: u64,
        entry_type: EntryType,
    ) -> Self {
        Self {
            composite_key,
            value,
            metadata,
            idempotency_key,
            sequence_number,
            entry_type,
        }
    }

    pub fn record_id(&self) -> &[u8] {
        self.composite_key.record_id()
    }

    pub fn item_key(&self) -> &[u8] {
        self.composite_key.item_key()
    }

    pub fn is_tombstone(&self) -> bool {
        matches!(self.entry_type, EntryType::Delete | EntryType::RangeDelete)
    }

    pub fn is_put(&self) -> bool {
        matches!(self.entry_type, EntryType::Put)
    }
}
