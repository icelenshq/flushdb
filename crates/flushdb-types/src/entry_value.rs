use bytes::Bytes;

/// Represents the value portion of an SSTable entry.
///
/// Values are either stored inline within the SSTable block or referenced
/// as an external blob for large values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryValue {
    Inline(Bytes),
    BlobRef {
        blob_id: Bytes,
        offset: u64,
        size: u32,
    },
}

impl EntryValue {
    pub const INLINE_TAG: u8 = 0x00;
    pub const BLOB_REF_TAG: u8 = 0x01;

    pub fn is_inline(&self) -> bool {
        matches!(self, EntryValue::Inline(_))
    }

    pub fn is_blob_ref(&self) -> bool {
        matches!(self, EntryValue::BlobRef { .. })
    }

    pub fn inline_value(&self) -> Option<&Bytes> {
        match self {
            EntryValue::Inline(v) => Some(v),
            EntryValue::BlobRef { .. } => None,
        }
    }

    /// Consumes self and returns the inline bytes, or `None` if this is a BlobRef.
    pub fn as_inline(self) -> Option<Bytes> {
        match self {
            EntryValue::Inline(v) => Some(v),
            EntryValue::BlobRef { .. } => None,
        }
    }

    /// Returns the byte size of an inline value, or 0 for BlobRef.
    pub fn inline_size(&self) -> usize {
        match self {
            EntryValue::Inline(v) => v.len(),
            EntryValue::BlobRef { .. } => 0,
        }
    }
}
