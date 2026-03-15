use std::io::Cursor;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};

use crate::error::{FlushError, FlushResult};

const ORDERED_KEY_SIZE: usize = 12;

/// Fixed-size 12-byte version key used for ordering entries.
///
/// Wire format (all big-endian):
///   Offset 0:  8 bytes -- timestamp_ms
///   Offset 8:  2 bytes -- node_id
///   Offset 10: 2 bytes -- sequence
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct OrderedKey {
    timestamp_ms: u64,
    node_id: u16,
    sequence: u16,
}

impl OrderedKey {
    pub fn new(timestamp_ms: u64, node_id: u16, sequence: u16) -> Self {
        Self {
            timestamp_ms,
            node_id,
            sequence,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> FlushResult<Self> {
        if bytes.len() != ORDERED_KEY_SIZE {
            return Err(FlushError::InvalidArgument {
                message: format!(
                    "OrderedKey requires exactly {ORDERED_KEY_SIZE} bytes, got {}",
                    bytes.len()
                ),
            });
        }

        let mut cursor = Cursor::new(bytes);
        let timestamp_ms = cursor.read_u64::<BigEndian>()?;
        let node_id = cursor.read_u16::<BigEndian>()?;
        let sequence = cursor.read_u16::<BigEndian>()?;

        Ok(Self {
            timestamp_ms,
            node_id,
            sequence,
        })
    }

    pub fn timestamp_ms(&self) -> u64 {
        self.timestamp_ms
    }

    pub fn node_id(&self) -> u16 {
        self.node_id
    }

    pub fn sequence(&self) -> u16 {
        self.sequence
    }

    pub fn to_bytes(&self) -> [u8; ORDERED_KEY_SIZE] {
        let mut buf = [0u8; ORDERED_KEY_SIZE];
        let mut cursor = Cursor::new(&mut buf[..]);
        cursor
            .write_u64::<BigEndian>(self.timestamp_ms)
            .expect("write to 12-byte buffer cannot fail");
        cursor
            .write_u16::<BigEndian>(self.node_id)
            .expect("write to 12-byte buffer cannot fail");
        cursor
            .write_u16::<BigEndian>(self.sequence)
            .expect("write to 12-byte buffer cannot fail");
        buf
    }
}
