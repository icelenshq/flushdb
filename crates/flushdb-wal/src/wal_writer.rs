use std::fs;
use std::path::{Path, PathBuf};

use flushdb_types::{FlushError, FlushResult};

use crate::config::{parse_segment_number, segment_path, WalConfig};
use crate::entry::WalEntry;
use crate::segment_reader::SegmentReader;
use crate::segment_writer::SegmentWriter;

pub struct WalWriter {
    partition_dir: PathBuf,
    config: WalConfig,
    current_writer: SegmentWriter,
    next_sequence_number: u64,
    segment_numbers: Vec<u64>,
}

impl WalWriter {
    pub fn open(partition_dir: &Path, config: &WalConfig) -> FlushResult<Self> {
        fs::create_dir_all(partition_dir)?;

        let mut segment_numbers = discover_segments(partition_dir)?;

        if segment_numbers.is_empty() {
            let writer = SegmentWriter::create(partition_dir, 1, 1)?;
            segment_numbers.push(1);
            return Ok(Self {
                partition_dir: partition_dir.to_path_buf(),
                config: config.clone(),
                current_writer: writer,
                next_sequence_number: 1,
                segment_numbers,
            });
        }

        // Recover sequence number from existing segments (scan from last to first)
        let mut highest_seq: Option<u64> = None;
        for &seg_num in segment_numbers.iter().rev() {
            let path = segment_path(partition_dir, seg_num);
            let reader = SegmentReader::open(&path)?;
            for result in reader.entries() {
                match result {
                    Ok(entry) => {
                        let seq = entry.sequence_number;
                        highest_seq = Some(highest_seq.map_or(seq, |h: u64| h.max(seq)));
                    }
                    Err(_) => break, // stop on corruption
                }
            }
            if highest_seq.is_some() {
                break;
            }
        }

        let next_seq = highest_seq.map_or(1, |s| s + 1);
        let new_seg_num = segment_numbers.last().copied().unwrap_or(0) + 1;

        let writer = SegmentWriter::create(partition_dir, new_seg_num, next_seq)?;
        segment_numbers.push(new_seg_num);

        Ok(Self {
            partition_dir: partition_dir.to_path_buf(),
            config: config.clone(),
            current_writer: writer,
            next_sequence_number: next_seq,
            segment_numbers,
        })
    }

    pub fn append(&mut self, entry: &mut WalEntry) -> FlushResult<()> {
        entry.sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;
        self.current_writer.append(entry)?;

        if self.current_writer.current_size() >= self.config.segment_size_target {
            self.rotate()?;
        }
        Ok(())
    }

    pub fn append_batch(&mut self, entries: &mut [WalEntry]) -> FlushResult<()> {
        for entry in entries.iter_mut() {
            entry.sequence_number = self.next_sequence_number;
            self.next_sequence_number += 1;
        }
        self.current_writer.append_batch(entries)?;

        if self.current_writer.current_size() >= self.config.segment_size_target {
            self.rotate()?;
        }
        Ok(())
    }

    pub fn sync(&mut self) -> FlushResult<()> {
        self.current_writer.sync()
    }

    pub fn rotate(&mut self) -> FlushResult<()> {
        self.current_writer.sync()?;

        let new_seg_num = self.current_writer.segment_number() + 1;
        let new_writer = SegmentWriter::create(
            &self.partition_dir,
            new_seg_num,
            self.next_sequence_number,
        )?;

        self.current_writer = new_writer;
        self.segment_numbers.push(new_seg_num);
        Ok(())
    }

    pub fn current_segment_number(&self) -> u64 {
        self.current_writer.segment_number()
    }

    pub fn next_sequence_number(&self) -> u64 {
        self.next_sequence_number
    }

    pub fn active_segment_numbers(&self) -> &[u64] {
        &self.segment_numbers
    }

    pub fn total_size(&self) -> FlushResult<u64> {
        let mut total = 0u64;
        for &seg_num in &self.segment_numbers {
            let path = segment_path(&self.partition_dir, seg_num);
            match fs::metadata(&path) {
                Ok(meta) => total += meta.len(),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(FlushError::Io(e)),
            }
        }
        Ok(total)
    }

    pub fn remove_segment(&mut self, segment_number: u64) -> FlushResult<()> {
        if segment_number == self.current_writer.segment_number() {
            return Err(FlushError::InvalidArgument {
                message: "cannot remove the active segment".to_string(),
            });
        }
        let path = segment_path(&self.partition_dir, segment_number);
        fs::remove_file(&path)?;
        self.segment_numbers.retain(|&n| n != segment_number);
        Ok(())
    }
}

fn discover_segments(dir: &Path) -> FlushResult<Vec<u64>> {
    let mut numbers = Vec::new();
    let entries = match fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(numbers),
        Err(e) => return Err(FlushError::Io(e)),
    };
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(num) = parse_segment_number(&name.to_string_lossy()) {
            numbers.push(num);
        }
    }
    numbers.sort();
    Ok(numbers)
}
