use crate::FlushError;
use crate::FlushResult;

/// Discriminant for the type of mutation an entry represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum EntryType {
    Put = 0,
    Delete = 1,
    RangeDelete = 2,
}

impl EntryType {
    /// Decode a `u8` discriminant into an `EntryType`.
    ///
    /// Returns `FlushError::CorruptedData` for unknown discriminants.
    pub fn from_u8(value: u8) -> FlushResult<Self> {
        match value {
            0 => Ok(EntryType::Put),
            1 => Ok(EntryType::Delete),
            2 => Ok(EntryType::RangeDelete),
            other => Err(FlushError::CorruptedData {
                message: format!("unknown EntryType discriminant: {other}"),
            }),
        }
    }

    /// Encode this `EntryType` as its `u8` discriminant.
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}
