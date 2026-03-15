use std::fs;
use std::path::{Path, PathBuf};

use flushdb_types::{FlushError, FlushResult};

use crate::config::SEGMENT_HEADER_SIZE;
use crate::entry::WalEntry;
use crate::segment_header::SegmentHeader;

pub struct SegmentReader {
    data: Vec<u8>,
    header: SegmentHeader,
    position: usize,
    segment_path: PathBuf,
}

impl SegmentReader {
    pub fn open(path: &Path) -> FlushResult<Self> {
        let data = fs::read(path)?;
        if data.len() < SEGMENT_HEADER_SIZE {
            return Err(FlushError::CorruptedData {
                message: format!(
                    "segment file too short for header: {} bytes in {}",
                    data.len(),
                    path.display()
                ),
            });
        }
        let header = SegmentHeader::decode(&data)?;
        Ok(Self {
            data,
            header,
            position: SEGMENT_HEADER_SIZE,
            segment_path: path.to_path_buf(),
        })
    }

    pub fn header(&self) -> &SegmentHeader {
        &self.header
    }

    pub fn segment_number(&self) -> u64 {
        self.header.segment_number
    }

    pub fn next_entry(&mut self) -> FlushResult<Option<WalEntry>> {
        let remaining = &self.data[self.position..];

        // Step 1: not enough bytes for entry_length prefix
        if remaining.len() < 4 {
            return Ok(None);
        }

        // Step 2: read entry_length
        let entry_length = WalEntry::read_entry_length(remaining)? as usize;

        // Step 3: zero entry_length means padding / unused space
        if entry_length == 0 {
            return Ok(None);
        }

        // Step 4-5: check if body + CRC fit
        let needed = 4 + entry_length + 4; // length_prefix + body + crc
        if remaining.len() < needed {
            tracing::warn!(
                path = %self.segment_path.display(),
                position = self.position,
                "tail truncation: expected {} bytes but only {} remain",
                needed,
                remaining.len()
            );
            return Ok(None);
        }

        // Step 6: extract body and CRC
        let body = &remaining[4..4 + entry_length];
        let crc_offset = 4 + entry_length;
        let expected_crc = u32::from_le_bytes([
            remaining[crc_offset],
            remaining[crc_offset + 1],
            remaining[crc_offset + 2],
            remaining[crc_offset + 3],
        ]);

        // Step 7-9: validate CRC
        match WalEntry::validate_crc(body, expected_crc) {
            Ok(()) => {
                let entry = WalEntry::decode_body(body)?;
                self.position += needed;
                Ok(Some(entry))
            }
            Err(crc_err) => {
                // Check if this is tail corruption vs mid-segment corruption
                let after_pos = self.position + needed;
                if is_tail_corruption(&self.data, after_pos) {
                    tracing::warn!(
                        path = %self.segment_path.display(),
                        position = self.position,
                        "tail corruption detected, discarding remaining entries"
                    );
                    Ok(None)
                } else {
                    Err(crc_err)
                }
            }
        }
    }

    pub fn entries(self) -> SegmentEntryIterator {
        SegmentEntryIterator {
            reader: self,
            done: false,
        }
    }
}

fn is_tail_corruption(data: &[u8], from: usize) -> bool {
    if from >= data.len() {
        return true;
    }
    let remaining = &data[from..];
    // If all remaining bytes are zero, or remaining is too small for another entry
    remaining.len() < 8 || remaining.iter().all(|&b| b == 0)
}

pub struct SegmentEntryIterator {
    reader: SegmentReader,
    done: bool,
}

impl Iterator for SegmentEntryIterator {
    type Item = FlushResult<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        match self.reader.next_entry() {
            Ok(Some(entry)) => Some(Ok(entry)),
            Ok(None) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}
