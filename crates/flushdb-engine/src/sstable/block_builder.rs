use std::collections::HashSet;
use std::io;

use bytes::Bytes;
use flushdb_types::{
    CompositeKey, EntryType, EntryValue, FlushError, FlushResult, IdempotencyToken,
};

use super::types::CompressionType;
use super::varint::encode_varint;

#[derive(Debug)]
pub struct FinishedBlock {
    pub data: Bytes,
    pub first_key: CompositeKey,
    pub last_key: CompositeKey,
    pub entry_count: u32,
    pub uncompressed_size: u32,
    pub record_ids: HashSet<Bytes>,
    pub idempotency_tokens: Vec<IdempotencyToken>,
}

pub struct BlockBuilder {
    buffer: Vec<u8>,
    entry_count: u32,
    block_size_target: usize,
    first_key: Option<CompositeKey>,
    last_key: Option<CompositeKey>,
    record_ids_seen: HashSet<Bytes>,
    idempotency_tokens: Vec<IdempotencyToken>,
    last_record_id: Option<Bytes>,
}

impl BlockBuilder {
    pub fn new(block_size_target: usize) -> Self {
        Self {
            buffer: Vec::new(),
            entry_count: 0,
            block_size_target,
            first_key: None,
            last_key: None,
            record_ids_seen: HashSet::new(),
            idempotency_tokens: Vec::new(),
            last_record_id: None,
        }
    }

    pub fn add_entry(
        &mut self,
        key: &CompositeKey,
        value: &[u8],
        metadata: &[u8],
        entry_type: EntryType,
        sequence_number: u64,
        idempotency_token: IdempotencyToken,
    ) {
        let record_id = key.record_id();
        let item_key = key.item_key();

        // Record ID dedup: first entry always writes full record_id
        let dedup = self.entry_count > 0
            && self
                .last_record_id
                .as_ref()
                .is_some_and(|last| last.as_ref() == record_id);

        if dedup {
            encode_varint(0, &mut self.buffer);
        } else {
            encode_varint(record_id.len() as u64, &mut self.buffer);
            self.buffer.extend_from_slice(record_id);
        }

        encode_varint(item_key.len() as u64, &mut self.buffer);
        self.buffer.extend_from_slice(item_key);

        // Value encoding: tag + data_len + data
        self.buffer.push(EntryValue::INLINE_TAG);
        encode_varint(value.len() as u64, &mut self.buffer);
        self.buffer.extend_from_slice(value);

        encode_varint(metadata.len() as u64, &mut self.buffer);
        self.buffer.extend_from_slice(metadata);

        self.buffer.push(entry_type.as_u8());
        encode_varint(sequence_number, &mut self.buffer);

        // Track metadata
        self.record_ids_seen
            .insert(Bytes::copy_from_slice(record_id));
        if !idempotency_token.is_none() {
            self.idempotency_tokens.push(idempotency_token);
        }

        if self.first_key.is_none() {
            self.first_key = Some(key.clone());
        }
        self.last_key = Some(key.clone());
        self.last_record_id = Some(Bytes::copy_from_slice(record_id));
        self.entry_count += 1;
    }

    pub fn is_full(&self) -> bool {
        self.buffer.len() >= self.block_size_target
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn finish(mut self, compression: CompressionType) -> FlushResult<FinishedBlock> {
        if self.entry_count == 0 {
            return Err(FlushError::InvalidArgument {
                message: "cannot finish empty block".into(),
            });
        }

        let crc = crc32fast::hash(&self.buffer);
        self.buffer.extend_from_slice(&crc.to_le_bytes());
        let uncompressed_size = self.buffer.len() as u32;

        let compressed = match compression {
            CompressionType::None => self.buffer,
            CompressionType::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                encoder
                    .compress_vec(&self.buffer)
                    .map_err(|e| FlushError::Io(io::Error::other(e.to_string())))?
            }
            CompressionType::Zstd => zstd::bulk::compress(&self.buffer, 3)
                .map_err(|e| FlushError::Io(io::Error::other(e.to_string())))?,
        };

        Ok(FinishedBlock {
            data: Bytes::from(compressed),
            first_key: self.first_key.unwrap(),
            last_key: self.last_key.unwrap(),
            entry_count: self.entry_count,
            uncompressed_size,
            record_ids: self.record_ids_seen,
            idempotency_tokens: self.idempotency_tokens,
        })
    }

    pub fn estimated_size(&self) -> usize {
        self.buffer.len()
    }

    pub fn entry_count(&self) -> u32 {
        self.entry_count
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
        self.entry_count = 0;
        self.first_key = None;
        self.last_key = None;
        self.record_ids_seen.clear();
        self.idempotency_tokens.clear();
        self.last_record_id = None;
    }
}
