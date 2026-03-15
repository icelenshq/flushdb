use std::time::{SystemTime, UNIX_EPOCH};

use flushdb_types::{FlushError, FlushResult};

use crate::config::{SEGMENT_HEADER_SIZE, WAL_MAGIC, WAL_VERSION};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentHeader {
    pub segment_number: u64,
    pub starting_sequence_number: u64,
    pub created_at_ms: u64,
}

impl SegmentHeader {
    pub fn new(segment_number: u64, starting_sequence_number: u64) -> Self {
        let created_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis() as u64;
        Self {
            segment_number,
            starting_sequence_number,
            created_at_ms,
        }
    }

    pub fn encode(&self) -> [u8; SEGMENT_HEADER_SIZE] {
        let mut buf = [0u8; SEGMENT_HEADER_SIZE];
        buf[0..4].copy_from_slice(&WAL_MAGIC);
        buf[4] = WAL_VERSION;
        buf[5] = 0; // flags
        buf[6..8].copy_from_slice(&[0u8; 2]); // reserved
        buf[8..16].copy_from_slice(&self.segment_number.to_le_bytes());
        buf[16..24].copy_from_slice(&self.starting_sequence_number.to_le_bytes());
        buf[24..32].copy_from_slice(&self.created_at_ms.to_le_bytes());
        buf
    }

    pub fn decode(data: &[u8]) -> FlushResult<Self> {
        if data.len() < SEGMENT_HEADER_SIZE {
            return Err(FlushError::CorruptedData {
                message: "segment header too short".to_string(),
            });
        }
        if data[0..4] != WAL_MAGIC {
            return Err(FlushError::CorruptedData {
                message: "invalid WAL segment magic".to_string(),
            });
        }
        let version = data[4];
        if version != WAL_VERSION {
            return Err(FlushError::CorruptedData {
                message: format!("unsupported WAL segment version: {version}"),
            });
        }
        // flags (offset 5) and reserved (offset 6..8) are tolerated regardless of value

        let segment_number = u64::from_le_bytes(read8(data, 8));
        let starting_sequence_number = u64::from_le_bytes(read8(data, 16));
        let created_at_ms = u64::from_le_bytes(read8(data, 24));

        Ok(Self {
            segment_number,
            starting_sequence_number,
            created_at_ms,
        })
    }
}

fn read8(data: &[u8], offset: usize) -> [u8; 8] {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&data[offset..offset + 8]);
    buf
}
