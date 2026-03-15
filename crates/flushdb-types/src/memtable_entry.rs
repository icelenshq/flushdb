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
    /// Creates a new entry with `sequence_number` defaulting to 0.
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

    /// Creates a new entry with an explicit sequence number.
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

    /// Returns the record_id portion of the composite key.
    pub fn record_id(&self) -> &[u8] {
        self.composite_key.record_id()
    }

    /// Returns the item_key portion of the composite key.
    pub fn item_key(&self) -> &[u8] {
        self.composite_key.item_key()
    }

    /// Returns `true` if this entry represents a deletion (Delete or RangeDelete).
    pub fn is_tombstone(&self) -> bool {
        matches!(self.entry_type, EntryType::Delete | EntryType::RangeDelete)
    }

    /// Returns `true` if this entry represents a Put operation.
    pub fn is_put(&self) -> bool {
        matches!(self.entry_type, EntryType::Put)
    }
}
