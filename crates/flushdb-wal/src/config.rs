use std::path::{Path, PathBuf};
use std::time::Duration;

pub const WAL_MAGIC: [u8; 4] = *b"FWAL";
pub const WAL_VERSION: u8 = 1;
pub const SEGMENT_HEADER_SIZE: usize = 32;
pub const SEGMENT_FILE_EXTENSION: &str = ".wal";
pub const SEGMENT_FILE_PREFIX: &str = "segment-";
pub const SEGMENT_NUMBER_WIDTH: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FsyncMode {
    #[default]
    Sync,
    BatchSync,
}

#[derive(Debug, Clone)]
pub struct WalConfig {
    pub segment_size_target: u64,
    pub max_wal_size: u64,
    pub max_total_wal_bytes: u64,
    pub segment_max_age: Duration,
    pub fsync_mode: FsyncMode,
    pub group_commit_interval: Duration,
    pub group_commit_max_bytes: usize,
    pub batch_sync_interval: Duration,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            segment_size_target: 33_554_432,      // 32 MB
            max_wal_size: 268_435_456,             // 256 MB
            max_total_wal_bytes: 536_870_912,      // 512 MB
            segment_max_age: Duration::from_secs(300),
            fsync_mode: FsyncMode::default(),
            group_commit_interval: Duration::from_micros(200),
            group_commit_max_bytes: 262_144,       // 256 KB
            batch_sync_interval: Duration::from_millis(10),
        }
    }
}

pub fn segment_filename(segment_number: u64) -> String {
    format!(
        "{}{:0>width$}{}",
        SEGMENT_FILE_PREFIX,
        segment_number,
        SEGMENT_FILE_EXTENSION,
        width = SEGMENT_NUMBER_WIDTH
    )
}

pub fn parse_segment_number(filename: &str) -> Option<u64> {
    let rest = filename.strip_prefix(SEGMENT_FILE_PREFIX)?;
    let number_str = rest.strip_suffix(SEGMENT_FILE_EXTENSION)?;
    if number_str.len() != SEGMENT_NUMBER_WIDTH {
        return None;
    }
    number_str.parse::<u64>().ok()
}

pub fn segment_path(dir: &Path, segment_number: u64) -> PathBuf {
    dir.join(segment_filename(segment_number))
}
