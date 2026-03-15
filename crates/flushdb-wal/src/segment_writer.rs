use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use flushdb_types::{FlushError, FlushResult};

use crate::config::{segment_path, SEGMENT_HEADER_SIZE};
use crate::entry::WalEntry;
use crate::segment_header::SegmentHeader;

pub struct SegmentWriter {
    file: File,
    segment_number: u64,
    current_size: u64,
    entry_count: u64,
    path: PathBuf,
}

impl SegmentWriter {
    pub fn create(
        dir: &Path,
        segment_number: u64,
        starting_sequence: u64,
    ) -> FlushResult<Self> {
        fs::create_dir_all(dir)?;

        let path = segment_path(dir, segment_number);

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(FlushError::Io)?;

        let header = SegmentHeader::new(segment_number, starting_sequence);
        let header_bytes = header.encode();
        file.write_all(&header_bytes)?;

        Ok(Self {
            file,
            segment_number,
            current_size: SEGMENT_HEADER_SIZE as u64,
            entry_count: 0,
            path,
        })
    }

    pub fn append(&mut self, entry: &WalEntry) -> FlushResult<u64> {
        let offset = self.current_size;
        let encoded = entry.encode();
        self.file.write_all(&encoded)?;
        self.current_size += encoded.len() as u64;
        self.entry_count += 1;
        Ok(offset)
    }

    pub fn append_batch(&mut self, entries: &[WalEntry]) -> FlushResult<()> {
        let mut buf = Vec::new();
        for entry in entries {
            let encoded = entry.encode();
            buf.extend_from_slice(&encoded);
        }
        self.file.write_all(&buf)?;
        self.current_size += buf.len() as u64;
        self.entry_count += entries.len() as u64;
        Ok(())
    }

    pub fn sync(&mut self) -> FlushResult<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn current_size(&self) -> u64 {
        self.current_size
    }

    pub fn segment_number(&self) -> u64 {
        self.segment_number
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
