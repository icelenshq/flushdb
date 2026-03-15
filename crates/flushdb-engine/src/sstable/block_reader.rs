use bytes::Bytes;
use flushdb_types::{
    CompositeKey, EntryType, EntryValue, FlushError, FlushResult,
};

use super::types::CompressionType;
use super::varint::decode_varint;

#[derive(Debug, Clone)]
pub struct BlockEntry {
    pub composite_key: CompositeKey,
    pub value: EntryValue,
    pub metadata: Bytes,
    pub entry_type: EntryType,
    pub sequence_number: u64,
}

pub fn decode_block(data: &[u8], compression: CompressionType) -> FlushResult<Vec<BlockEntry>> {
    let iter = BlockEntryIterator::new(data, compression)?;
    iter.collect()
}

pub struct BlockEntryIterator {
    data: Bytes,
    offset: usize,
    last_record_id: Option<Bytes>,
}

impl BlockEntryIterator {
    pub fn new(raw_block: &[u8], compression: CompressionType) -> FlushResult<Self> {
        let decompressed = decompress(raw_block, compression)?;

        if decompressed.len() < 4 {
            return Err(FlushError::CorruptedData {
                message: "block too short for CRC".into(),
            });
        }

        let payload_len = decompressed.len() - 4;
        let stored_crc =
            u32::from_le_bytes(decompressed[payload_len..].try_into().unwrap());
        let computed_crc = crc32fast::hash(&decompressed[..payload_len]);

        if stored_crc != computed_crc {
            return Err(FlushError::CrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        Ok(Self {
            data: Bytes::copy_from_slice(&decompressed[..payload_len]),
            offset: 0,
            last_record_id: None,
        })
    }
}

impl Iterator for BlockEntryIterator {
    type Item = FlushResult<BlockEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.data.len() {
            return None;
        }
        Some(self.decode_next())
    }
}

impl BlockEntryIterator {
    fn decode_next(&mut self) -> FlushResult<BlockEntry> {
        let buf = &self.data[self.offset..];

        // record_id
        let (record_id_len, n) = decode_varint(buf)?;
        let mut pos = n;

        let record_id = if record_id_len == 0 {
            match &self.last_record_id {
                Some(rid) => rid.clone(),
                None => {
                    return Err(FlushError::CorruptedData {
                        message: "first entry in block has record_id_len=0".into(),
                    })
                }
            }
        } else {
            let rid_len = record_id_len as usize;
            if pos + rid_len > buf.len() {
                return Err(FlushError::CorruptedData {
                    message: "record_id truncated".into(),
                });
            }
            let rid = Bytes::copy_from_slice(&buf[pos..pos + rid_len]);
            pos += rid_len;
            self.last_record_id = Some(rid.clone());
            rid
        };

        // item_key
        let (item_key_len, n) = decode_varint(&buf[pos..])?;
        pos += n;
        let ik_len = item_key_len as usize;
        if pos + ik_len > buf.len() {
            return Err(FlushError::CorruptedData {
                message: "item_key truncated".into(),
            });
        }
        let item_key = &buf[pos..pos + ik_len];
        pos += ik_len;

        // Construct composite key
        let mut key_bytes = Vec::with_capacity(record_id.len() + 1 + item_key.len());
        key_bytes.extend_from_slice(&record_id);
        key_bytes.push(0x00); // separator
        key_bytes.extend_from_slice(item_key);
        let composite_key = CompositeKey::from_bytes(Bytes::from(key_bytes))?;

        // value: tag + data_len + data
        if pos >= buf.len() {
            return Err(FlushError::CorruptedData {
                message: "value tag missing".into(),
            });
        }
        let value_tag = buf[pos];
        pos += 1;

        let (value_data_len, n) = decode_varint(&buf[pos..])?;
        pos += n;
        let vd_len = value_data_len as usize;
        if pos + vd_len > buf.len() {
            return Err(FlushError::CorruptedData {
                message: "value data truncated".into(),
            });
        }
        let value_data = &buf[pos..pos + vd_len];
        pos += vd_len;

        let value = match value_tag {
            EntryValue::INLINE_TAG => EntryValue::Inline(Bytes::copy_from_slice(value_data)),
            EntryValue::BLOB_REF_TAG => {
                if value_data.len() < 2 {
                    return Err(FlushError::CorruptedData {
                        message: "blob_ref data too short".into(),
                    });
                }
                let blob_id_len = u16::from_le_bytes(value_data[0..2].try_into().unwrap()) as usize;
                if value_data.len() < 2 + blob_id_len + 8 + 4 {
                    return Err(FlushError::CorruptedData {
                        message: "blob_ref fields truncated".into(),
                    });
                }
                let blob_id = Bytes::copy_from_slice(&value_data[2..2 + blob_id_len]);
                let offset_start = 2 + blob_id_len;
                let offset =
                    u64::from_le_bytes(value_data[offset_start..offset_start + 8].try_into().unwrap());
                let size = u32::from_le_bytes(
                    value_data[offset_start + 8..offset_start + 12].try_into().unwrap(),
                );
                EntryValue::BlobRef {
                    blob_id,
                    offset,
                    size,
                }
            }
            _ => {
                return Err(FlushError::CorruptedData {
                    message: format!("unknown entry value tag: {value_tag}"),
                })
            }
        };

        // metadata
        let (metadata_len, n) = decode_varint(&buf[pos..])?;
        pos += n;
        let md_len = metadata_len as usize;
        if pos + md_len > buf.len() {
            return Err(FlushError::CorruptedData {
                message: "metadata truncated".into(),
            });
        }
        let metadata = Bytes::copy_from_slice(&buf[pos..pos + md_len]);
        pos += md_len;

        // entry_type
        if pos >= buf.len() {
            return Err(FlushError::CorruptedData {
                message: "entry_type missing".into(),
            });
        }
        let entry_type = EntryType::from_u8(buf[pos])?;
        pos += 1;

        // sequence_number
        let (sequence_number, n) = decode_varint(&buf[pos..])?;
        pos += n;

        self.offset += pos;

        Ok(BlockEntry {
            composite_key,
            value,
            metadata,
            entry_type,
            sequence_number,
        })
    }
}

fn decompress(data: &[u8], compression: CompressionType) -> FlushResult<Vec<u8>> {
    match compression {
        CompressionType::None => Ok(data.to_vec()),
        CompressionType::Snappy => {
            let mut decoder = snap::raw::Decoder::new();
            decoder.decompress_vec(data).map_err(|e| {
                FlushError::CorruptedData {
                    message: format!("decompression failed: {e}"),
                }
            })
        }
        CompressionType::Zstd => {
            zstd::stream::decode_all(data).map_err(|e| {
                FlushError::CorruptedData {
                    message: format!("decompression failed: {e}"),
                }
            })
        }
    }
}
