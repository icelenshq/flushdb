use bytes::Bytes;
use flushdb_types::{CompositeKey, FlushError, FlushResult};

use super::varint::{decode_varint, encode_varint};

#[derive(Debug, Clone)]
pub struct IndexEntry {
    pub first_key: CompositeKey,
    pub block_offset: u64,
    pub block_size: u32,
    pub uncompressed_size: u32,
}

pub struct IndexBlockBuilder {
    entries: Vec<IndexEntry>,
}

impl Default for IndexBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IndexBlockBuilder {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn add(
        &mut self,
        first_key: CompositeKey,
        block_offset: u64,
        block_size: u32,
        uncompressed_size: u32,
    ) {
        self.entries.push(IndexEntry {
            first_key,
            block_offset,
            block_size,
            uncompressed_size,
        });
    }

    pub fn build(self) -> IndexBlock {
        IndexBlock {
            entries: self.entries,
        }
    }

    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }
}

pub struct IndexBlock {
    entries: Vec<IndexEntry>,
}

impl IndexBlock {
    pub fn find_block(&self, key: &CompositeKey) -> Option<&IndexEntry> {
        let idx = self.find_block_index(key)?;
        Some(&self.entries[idx])
    }

    pub fn find_block_index(&self, key: &CompositeKey) -> Option<usize> {
        let partition_point = self
            .entries
            .partition_point(|e| e.first_key.as_bytes() <= key.as_bytes());
        if partition_point == 0 {
            None
        } else {
            Some(partition_point - 1)
        }
    }

    pub fn get(&self, index: usize) -> Option<&IndexEntry> {
        self.entries.get(index)
    }

    pub fn block_count(&self) -> usize {
        self.entries.len()
    }

    pub fn key_range(&self) -> Option<(&CompositeKey, &CompositeKey)> {
        if self.entries.is_empty() {
            return None;
        }
        Some((
            &self.entries.first().unwrap().first_key,
            &self.entries.last().unwrap().first_key,
        ))
    }

    pub fn overlaps(&self, start: &CompositeKey, end: &CompositeKey) -> bool {
        let Some((index_min, index_max)) = self.key_range() else {
            return false;
        };
        if index_max.as_bytes() < start.as_bytes() {
            return false;
        }
        if index_min.as_bytes() > end.as_bytes() {
            return false;
        }
        true
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = Vec::new();
        let count = self.entries.len() as u32;
        buf.extend_from_slice(&count.to_le_bytes());

        for entry in &self.entries {
            let key_bytes = entry.first_key.as_bytes();
            encode_varint(key_bytes.len() as u64, &mut buf);
            buf.extend_from_slice(key_bytes);
            buf.extend_from_slice(&entry.block_offset.to_le_bytes());
            buf.extend_from_slice(&entry.block_size.to_le_bytes());
            buf.extend_from_slice(&entry.uncompressed_size.to_le_bytes());
        }

        Bytes::from(buf)
    }

    pub fn deserialize(data: &[u8]) -> FlushResult<Self> {
        if data.len() < 4 {
            return Err(FlushError::CorruptedData {
                message: "index block too short".into(),
            });
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let mut pos = 4;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            if pos >= data.len() {
                return Err(FlushError::CorruptedData {
                    message: "index block entry truncated".into(),
                });
            }

            let (key_len, n) = decode_varint(&data[pos..])?;
            pos += n;
            let kl = key_len as usize;

            if pos + kl + 16 > data.len() {
                return Err(FlushError::CorruptedData {
                    message: "index block entry truncated".into(),
                });
            }

            let first_key = CompositeKey::from_bytes(Bytes::copy_from_slice(&data[pos..pos + kl]))?;
            pos += kl;

            let block_offset = u64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
            pos += 8;
            let block_size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;
            let uncompressed_size = u32::from_le_bytes(data[pos..pos + 4].try_into().unwrap());
            pos += 4;

            entries.push(IndexEntry {
                first_key,
                block_offset,
                block_size,
                uncompressed_size,
            });
        }

        Ok(Self { entries })
    }

    pub fn iter(&self) -> impl Iterator<Item = &IndexEntry> {
        self.entries.iter()
    }
}
