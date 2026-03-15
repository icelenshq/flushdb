use bytes::Bytes;

/// A single key-value pair within a record.
///
/// Items are the second level of the two-level map: each record contains
/// a sorted set of items keyed by arbitrary bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Item {
    /// Sort key within the record.
    pub key: Bytes,
    /// Payload.
    pub value: Bytes,
    /// Optional metadata (content type, schema version, etc.).
    /// Empty `Bytes` means no metadata.
    pub metadata: Bytes,
    /// Chunk index for large values. 0 for non-chunked items.
    pub chunk: u32,
}

impl Item {
    /// Create an item with the given key and value, empty metadata, and chunk 0.
    pub fn new(key: Bytes, value: Bytes) -> Self {
        Self {
            key,
            value,
            metadata: Bytes::new(),
            chunk: 0,
        }
    }

    /// Create an item with key, value, and metadata; chunk defaults to 0.
    pub fn with_metadata(key: Bytes, value: Bytes, metadata: Bytes) -> Self {
        Self {
            key,
            value,
            metadata,
            chunk: 0,
        }
    }

    /// Create an item with all fields specified.
    pub fn with_all(key: Bytes, value: Bytes, metadata: Bytes, chunk: u32) -> Self {
        Self {
            key,
            value,
            metadata,
            chunk,
        }
    }
}
