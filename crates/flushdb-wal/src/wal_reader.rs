use std::fs;
use std::path::{Path, PathBuf};

use flushdb_types::{FlushError, FlushResult};

use crate::config::{parse_segment_number, segment_path};
use crate::entry::WalEntry;
use crate::segment_reader::SegmentReader;

pub struct WalReader {
    partition_dir: PathBuf,
    segment_numbers: Vec<u64>,
}

impl WalReader {
    pub fn open(partition_dir: &Path) -> FlushResult<Self> {
        if !partition_dir.exists() {
            return Err(FlushError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("WAL directory does not exist: {}", partition_dir.display()),
            )));
        }

        let mut segment_numbers = Vec::new();
        for entry in fs::read_dir(partition_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            if let Some(num) = parse_segment_number(&name.to_string_lossy()) {
                segment_numbers.push(num);
            }
        }
        segment_numbers.sort();

        Ok(Self {
            partition_dir: partition_dir.to_path_buf(),
            segment_numbers,
        })
    }

    pub fn replay_all(&self) -> FlushResult<Vec<WalEntry>> {
        let mut entries = Vec::new();
        for &seg_num in &self.segment_numbers {
            let path = segment_path(&self.partition_dir, seg_num);
            let reader = SegmentReader::open(&path)?;
            for result in reader.entries() {
                entries.push(result?);
            }
        }
        Ok(entries)
    }

    pub fn replay_from(&self, min_sequence: u64) -> FlushResult<Vec<WalEntry>> {
        let mut entries = Vec::new();
        let seg_count = self.segment_numbers.len();

        for (i, &seg_num) in self.segment_numbers.iter().enumerate() {
            // Segment skipping optimization: if the NEXT segment's starting_sequence
            // is <= min_sequence, this entire segment can be skipped
            if i + 1 < seg_count {
                let next_path = segment_path(&self.partition_dir, self.segment_numbers[i + 1]);
                if let Ok(next_reader) = SegmentReader::open(&next_path) {
                    if next_reader.header().starting_sequence_number <= min_sequence {
                        continue;
                    }
                }
            }

            let path = segment_path(&self.partition_dir, seg_num);
            let reader = SegmentReader::open(&path)?;
            for result in reader.entries() {
                let entry = result?;
                if entry.sequence_number >= min_sequence {
                    entries.push(entry);
                }
            }
        }
        Ok(entries)
    }

    pub fn iter(&self) -> WalEntryIterator {
        WalEntryIterator::new(self.partition_dir.clone(), self.segment_numbers.clone(), None)
    }

    pub fn iter_from(&self, min_sequence: u64) -> WalEntryIterator {
        WalEntryIterator::new(
            self.partition_dir.clone(),
            self.segment_numbers.clone(),
            Some(min_sequence),
        )
    }

    pub fn segment_count(&self) -> usize {
        self.segment_numbers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segment_numbers.is_empty()
    }
}

pub struct WalEntryIterator {
    partition_dir: PathBuf,
    segment_numbers: Vec<u64>,
    segment_index: usize,
    current_reader: Option<SegmentReader>,
    min_sequence: Option<u64>,
    done: bool,
}

impl WalEntryIterator {
    fn new(
        partition_dir: PathBuf,
        segment_numbers: Vec<u64>,
        min_sequence: Option<u64>,
    ) -> Self {
        Self {
            partition_dir,
            segment_numbers,
            segment_index: 0,
            current_reader: None,
            min_sequence,
            done: false,
        }
    }

    fn open_next_segment(&mut self) -> FlushResult<bool> {
        while self.segment_index < self.segment_numbers.len() {
            // Segment skipping optimization
            if let Some(min_seq) = self.min_sequence {
                if self.segment_index + 1 < self.segment_numbers.len() {
                    let next_seg = self.segment_numbers[self.segment_index + 1];
                    let next_path = segment_path(&self.partition_dir, next_seg);
                    if let Ok(next_reader) = SegmentReader::open(&next_path) {
                        if next_reader.header().starting_sequence_number <= min_seq {
                            self.segment_index += 1;
                            continue;
                        }
                    }
                }
            }

            let seg_num = self.segment_numbers[self.segment_index];
            let path = segment_path(&self.partition_dir, seg_num);
            self.current_reader = Some(SegmentReader::open(&path)?);
            self.segment_index += 1;
            return Ok(true);
        }
        Ok(false)
    }
}

impl Iterator for WalEntryIterator {
    type Item = FlushResult<WalEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            if self.current_reader.is_none() {
                match self.open_next_segment() {
                    Ok(true) => {}
                    Ok(false) => return None,
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                }
            }

            let reader = self.current_reader.as_mut().unwrap();
            match reader.next_entry() {
                Ok(Some(entry)) => {
                    if let Some(min_seq) = self.min_sequence {
                        if entry.sequence_number < min_seq {
                            continue;
                        }
                    }
                    return Some(Ok(entry));
                }
                Ok(None) => {
                    self.current_reader = None;
                    continue;
                }
                Err(e) => {
                    self.done = true;
                    return Some(Err(e));
                }
            }
        }
    }
}
