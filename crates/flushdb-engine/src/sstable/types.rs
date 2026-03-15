use flushdb_types::{FlushError, FlushResult};

pub const SSTABLE_MAGIC: u32 = 0x464C4442; // "FLDB"
pub const FORMAT_VERSION: u16 = 1;
pub const DEFAULT_BLOCK_SIZE: usize = 4096;
pub const FOOTER_SIZE: usize = 80;
pub const HEADER_SIZE: usize = 16;
pub const MIN_MAX_KEY_TRUNCATION_LEN: usize = 16;
pub const DEFAULT_BLOOM_BITS_PER_KEY: u32 = 10;
pub const DEFAULT_BLOOM_HASH_COUNT: u32 = 7;
pub const DEDUP_HASH_SIZE: usize = 16;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum CompressionType {
    None = 0,
    Snappy = 1,
    Zstd = 2,
}

impl CompressionType {
    pub fn from_u8(value: u8) -> FlushResult<Self> {
        match value {
            0 => Ok(Self::None),
            1 => Ok(Self::Snappy),
            2 => Ok(Self::Zstd),
            _ => Err(FlushError::CorruptedData {
                message: format!("unknown compression type: {value}"),
            }),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

#[derive(Clone, Debug)]
pub struct SstConfig {
    pub block_size_target: usize,
    pub compression: CompressionType,
    pub bloom_bits_per_key: u32,
}

impl Default for SstConfig {
    fn default() -> Self {
        Self {
            block_size_target: DEFAULT_BLOCK_SIZE,
            compression: CompressionType::Snappy,
            bloom_bits_per_key: DEFAULT_BLOOM_BITS_PER_KEY,
        }
    }
}

impl SstConfig {
    pub fn with_block_size(mut self, size: usize) -> Self {
        self.block_size_target = size;
        self
    }

    pub fn with_compression(mut self, compression: CompressionType) -> Self {
        self.compression = compression;
        self
    }

    pub fn with_bloom_bits_per_key(mut self, bits: u32) -> Self {
        self.bloom_bits_per_key = bits;
        self
    }
}
