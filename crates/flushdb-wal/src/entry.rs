use bytes::{BufMut, Bytes, BytesMut};
use flushdb_types::{
    CompositeKey, EntryType, FlushError, FlushResult, IdempotencyToken, MemtableEntry,
};

#[derive(Debug, Clone, PartialEq)]
pub struct WalEntry {
    pub sequence_number: u64,
    pub entry_type: EntryType,
    pub namespace: Bytes,
    pub record_id: Bytes,
    pub item_key: Bytes,
    pub item_value: Bytes,
    pub item_metadata: Bytes,
    pub idempotency_token: IdempotencyToken,
}

const FIXED_BODY_OVERHEAD: usize = 8 + 1 + 2 + 2 + 2 + 4 + 2 + 24; // 45 bytes

impl WalEntry {
    pub fn body_size(&self) -> usize {
        FIXED_BODY_OVERHEAD
            + self.namespace.len()
            + self.record_id.len()
            + self.item_key.len()
            + self.item_value.len()
            + self.item_metadata.len()
    }

    pub fn total_size(&self) -> usize {
        4 + self.body_size() + 4
    }

    pub fn encode(&self) -> Bytes {
        let body_size = self.body_size();
        let total = 4 + body_size + 4;
        let mut buf = BytesMut::with_capacity(total);

        // entry_length
        buf.put_u32_le(body_size as u32);

        // body start
        let body_start = buf.len();
        buf.put_u64_le(self.sequence_number);
        buf.put_u8(self.entry_type.as_u8());
        buf.put_u16_le(self.namespace.len() as u16);
        buf.put_slice(&self.namespace);
        buf.put_u16_le(self.record_id.len() as u16);
        buf.put_slice(&self.record_id);
        buf.put_u16_le(self.item_key.len() as u16);
        buf.put_slice(&self.item_key);
        buf.put_u32_le(self.item_value.len() as u32);
        buf.put_slice(&self.item_value);
        buf.put_u16_le(self.item_metadata.len() as u16);
        buf.put_slice(&self.item_metadata);
        buf.put_slice(self.idempotency_token.as_bytes());
        // body end

        let crc = crc32fast::hash(&buf[body_start..]);
        buf.put_u32_le(crc);

        buf.freeze()
    }

    pub fn read_entry_length(data: &[u8]) -> FlushResult<u32> {
        if data.len() < 4 {
            return Err(FlushError::CorruptedData {
                message: "WAL entry too short for length prefix".to_string(),
            });
        }
        Ok(u32::from_le_bytes([data[0], data[1], data[2], data[3]]))
    }

    pub fn validate_crc(body: &[u8], expected_crc: u32) -> FlushResult<()> {
        let actual = crc32fast::hash(body);
        if actual != expected_crc {
            return Err(FlushError::CrcMismatch {
                expected: expected_crc,
                actual,
            });
        }
        Ok(())
    }

    pub fn decode_body(body: &[u8]) -> FlushResult<Self> {
        if body.len() < FIXED_BODY_OVERHEAD {
            return Err(FlushError::CorruptedData {
                message: "WAL entry body too short".to_string(),
            });
        }

        let mut pos = 0;

        let sequence_number = u64::from_le_bytes(read_fixed::<8>(body, pos));
        pos += 8;

        let entry_type_byte = body[pos];
        let entry_type = EntryType::from_u8(entry_type_byte).map_err(|_| {
            FlushError::CorruptedData {
                message: format!("unknown WAL entry type: {entry_type_byte}"),
            }
        })?;
        pos += 1;

        let (namespace, new_pos) = read_var_u16(body, pos)?;
        pos = new_pos;

        let (record_id, new_pos) = read_var_u16(body, pos)?;
        pos = new_pos;

        let (item_key, new_pos) = read_var_u16(body, pos)?;
        pos = new_pos;

        let (item_value, new_pos) = read_var_u32(body, pos)?;
        pos = new_pos;

        let (item_metadata, new_pos) = read_var_u16(body, pos)?;
        pos = new_pos;

        if pos + 24 > body.len() {
            return Err(FlushError::CorruptedData {
                message: "WAL entry field length exceeds body".to_string(),
            });
        }
        let idempotency_token = IdempotencyToken::from_bytes(&body[pos..pos + 24])
            .map_err(|e| FlushError::CorruptedData {
                message: format!("invalid idempotency token: {e}"),
            })?;

        Ok(Self {
            sequence_number,
            entry_type,
            namespace: Bytes::copy_from_slice(namespace),
            record_id: Bytes::copy_from_slice(record_id),
            item_key: Bytes::copy_from_slice(item_key),
            item_value: Bytes::copy_from_slice(item_value),
            item_metadata: Bytes::copy_from_slice(item_metadata),
            idempotency_token,
        })
    }

    pub fn from_memtable_entry(entry: &MemtableEntry, namespace: &[u8]) -> Self {
        Self {
            sequence_number: entry.sequence_number,
            entry_type: entry.entry_type,
            namespace: Bytes::copy_from_slice(namespace),
            record_id: Bytes::copy_from_slice(entry.record_id()),
            item_key: Bytes::copy_from_slice(entry.item_key()),
            item_value: entry.value.clone(),
            item_metadata: entry.metadata.clone(),
            idempotency_token: entry.idempotency_key,
        }
    }

    pub fn to_memtable_entry(&self) -> FlushResult<MemtableEntry> {
        let composite_key = CompositeKey::new(&self.record_id, &self.item_key)?;
        Ok(MemtableEntry::with_sequence(
            composite_key,
            self.item_value.clone(),
            self.item_metadata.clone(),
            self.idempotency_token,
            self.sequence_number,
            self.entry_type,
        ))
    }
}

fn read_fixed<const N: usize>(data: &[u8], pos: usize) -> [u8; N] {
    let mut buf = [0u8; N];
    buf.copy_from_slice(&data[pos..pos + N]);
    buf
}

fn read_var_u16(body: &[u8], pos: usize) -> FlushResult<(&[u8], usize)> {
    if pos + 2 > body.len() {
        return Err(FlushError::CorruptedData {
            message: "WAL entry field length exceeds body".to_string(),
        });
    }
    let len = u16::from_le_bytes([body[pos], body[pos + 1]]) as usize;
    let start = pos + 2;
    let end = start + len;
    if end > body.len() {
        return Err(FlushError::CorruptedData {
            message: "WAL entry field length exceeds body".to_string(),
        });
    }
    Ok((&body[start..end], end))
}

fn read_var_u32(body: &[u8], pos: usize) -> FlushResult<(&[u8], usize)> {
    if pos + 4 > body.len() {
        return Err(FlushError::CorruptedData {
            message: "WAL entry field length exceeds body".to_string(),
        });
    }
    let len = u32::from_le_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]) as usize;
    let start = pos + 4;
    let end = start + len;
    if end > body.len() {
        return Err(FlushError::CorruptedData {
            message: "WAL entry field length exceeds body".to_string(),
        });
    }
    Ok((&body[start..end], end))
}
