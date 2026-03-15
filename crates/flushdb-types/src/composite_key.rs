use std::hash::{Hash, Hasher};

use bytes::Bytes;

use crate::error::{FlushError, FlushResult};

pub const MAX_RECORD_ID_LEN: usize = 256;
pub const MAX_ITEM_KEY_LEN: usize = 4096;
pub const MAX_COMPOSITE_KEY_LEN: usize = 4353; // 256 + 1 + 4096
pub const SEPARATOR: u8 = 0x00;
pub const RANGE_TOMBSTONE_PREFIX: u8 = 0xFF;

/// A composite key encoding `[record_id][0x00][item_key]` as contiguous bytes.
///
/// The record_id must not contain null bytes, ensuring the first `0x00` in
/// the encoded form is always the separator. Ordering is raw byte comparison.
#[derive(Clone)]
pub struct CompositeKey(Bytes);

impl CompositeKey {
    pub fn new(record_id: &[u8], item_key: &[u8]) -> FlushResult<Self> {
        validate_record_id(record_id)?;
        validate_item_key(item_key)?;
        Ok(Self(encode(record_id, item_key)))
    }

    /// Shortcut for an empty item_key: `[record_id][0x00]`.
    pub fn from_record_only(record_id: &[u8]) -> FlushResult<Self> {
        Self::new(record_id, b"")
    }

    /// Creates a range-tombstone key: `[record_id][0x00][0xFF][start_key]`.
    pub fn range_tombstone_key(record_id: &[u8], start_key: &[u8]) -> FlushResult<Self> {
        validate_record_id(record_id)?;
        let item_len = 1 + start_key.len();
        if item_len > MAX_ITEM_KEY_LEN {
            return Err(FlushError::KeyTooLong {
                field: "item_key",
                actual: item_len,
                max: MAX_ITEM_KEY_LEN,
            });
        }
        let mut buf = Vec::with_capacity(record_id.len() + 1 + item_len);
        buf.extend_from_slice(record_id);
        buf.push(SEPARATOR);
        buf.push(RANGE_TOMBSTONE_PREFIX);
        buf.extend_from_slice(start_key);
        Ok(Self(Bytes::from(buf)))
    }

    /// Smallest possible key for a record: `[record_id][0x00]`.
    pub fn min_key_for_record(record_id: &[u8]) -> FlushResult<Self> {
        Self::from_record_only(record_id)
    }

    /// Largest boundary key for a record: `[record_id][0x00][0xFF]`.
    /// Sorts after all normal data keys but at/before range-tombstone keys.
    pub fn max_key_for_record(record_id: &[u8]) -> FlushResult<Self> {
        validate_record_id(record_id)?;
        let mut buf = Vec::with_capacity(record_id.len() + 2);
        buf.extend_from_slice(record_id);
        buf.push(SEPARATOR);
        buf.push(RANGE_TOMBSTONE_PREFIX);
        Ok(Self(Bytes::from(buf)))
    }

    /// Parse a composite key from raw bytes.
    /// Validates that the bytes contain at least one `0x00` separator.
    /// Does NOT re-validate length limits.
    pub fn from_bytes(bytes: Bytes) -> FlushResult<Self> {
        if !bytes.contains(&SEPARATOR) {
            return Err(FlushError::InvalidKey {
                reason: "composite key must contain at least one 0x00 separator".to_string(),
            });
        }
        Ok(Self(bytes))
    }

    pub fn record_id(&self) -> &[u8] {
        let sep = self
            .0
            .iter()
            .position(|&b| b == SEPARATOR)
            .expect("CompositeKey invariant: always contains a separator");
        &self.0[..sep]
    }

    pub fn item_key(&self) -> &[u8] {
        let sep = self
            .0
            .iter()
            .position(|&b| b == SEPARATOR)
            .expect("CompositeKey invariant: always contains a separator");
        &self.0[sep + 1..]
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    pub fn is_range_tombstone(&self) -> bool {
        let ik = self.item_key();
        ik.first() == Some(&RANGE_TOMBSTONE_PREFIX)
    }

    pub fn is_empty_item_key(&self) -> bool {
        self.item_key().is_empty()
    }
}

impl std::fmt::Debug for CompositeKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeKey")
            .field("record_id", &String::from_utf8_lossy(self.record_id()))
            .field("item_key_len", &self.item_key().len())
            .field("is_range_tombstone", &self.is_range_tombstone())
            .finish()
    }
}

impl PartialEq for CompositeKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Eq for CompositeKey {}

impl PartialOrd for CompositeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for CompositeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_ref().cmp(other.0.as_ref())
    }
}

impl Hash for CompositeKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

fn validate_record_id(record_id: &[u8]) -> FlushResult<()> {
    if record_id.is_empty() {
        return Err(FlushError::InvalidKey {
            reason: "record_id must not be empty".to_string(),
        });
    }
    if record_id.contains(&SEPARATOR) {
        return Err(FlushError::InvalidKey {
            reason: "record_id must not contain null bytes".to_string(),
        });
    }
    if record_id.len() > MAX_RECORD_ID_LEN {
        return Err(FlushError::KeyTooLong {
            field: "record_id",
            actual: record_id.len(),
            max: MAX_RECORD_ID_LEN,
        });
    }
    Ok(())
}

fn validate_item_key(item_key: &[u8]) -> FlushResult<()> {
    if item_key.len() > MAX_ITEM_KEY_LEN {
        return Err(FlushError::KeyTooLong {
            field: "item_key",
            actual: item_key.len(),
            max: MAX_ITEM_KEY_LEN,
        });
    }
    Ok(())
}

fn encode(record_id: &[u8], item_key: &[u8]) -> Bytes {
    let mut buf = Vec::with_capacity(record_id.len() + 1 + item_key.len());
    buf.extend_from_slice(record_id);
    buf.push(SEPARATOR);
    buf.extend_from_slice(item_key);
    Bytes::from(buf)
}
