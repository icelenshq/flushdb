use flushdb_types::{CompositeKey, FlushError, FlushResult};

use super::types::{
    CompressionType, FOOTER_SIZE, FORMAT_VERSION, HEADER_SIZE, MIN_MAX_KEY_TRUNCATION_LEN,
    SSTABLE_MAGIC,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstFooter {
    pub bloom_filter_offset: u64,
    pub bloom_filter_size: u32,
    pub index_block_offset: u64,
    pub index_block_size: u32,
    pub entry_count: u64,
    pub min_key: [u8; MIN_MAX_KEY_TRUNCATION_LEN],
    pub max_key: [u8; MIN_MAX_KEY_TRUNCATION_LEN],
    pub compression_type: CompressionType,
    pub format_version: u16,
    pub dedup_block_size: u32,
}

impl SstFooter {
    pub fn encode(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];

        buf[0..8].copy_from_slice(&self.bloom_filter_offset.to_le_bytes());
        buf[8..12].copy_from_slice(&self.bloom_filter_size.to_le_bytes());
        buf[12..20].copy_from_slice(&self.index_block_offset.to_le_bytes());
        buf[20..24].copy_from_slice(&self.index_block_size.to_le_bytes());
        buf[24..32].copy_from_slice(&self.entry_count.to_le_bytes());
        buf[32..48].copy_from_slice(&self.min_key);
        buf[48..64].copy_from_slice(&self.max_key);
        buf[64] = self.compression_type.as_u8();
        buf[65] = 0x00; // padding
        buf[66..68].copy_from_slice(&self.format_version.to_le_bytes());

        let crc = crc32fast::hash(&buf[0..68]);
        buf[68..72].copy_from_slice(&crc.to_le_bytes());
        buf[72..76].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        buf[76..80].copy_from_slice(&self.dedup_block_size.to_le_bytes());

        buf
    }

    pub fn decode(bytes: &[u8]) -> FlushResult<Self> {
        if bytes.len() != FOOTER_SIZE {
            return Err(FlushError::CorruptedData {
                message: "footer must be exactly 80 bytes".into(),
            });
        }

        let magic = u32::from_le_bytes(bytes[72..76].try_into().unwrap());
        if magic != SSTABLE_MAGIC {
            return Err(FlushError::CorruptedData {
                message: "invalid SSTable magic".into(),
            });
        }

        let stored_crc = u32::from_le_bytes(bytes[68..72].try_into().unwrap());
        let computed_crc = crc32fast::hash(&bytes[0..68]);
        if stored_crc != computed_crc {
            return Err(FlushError::CrcMismatch {
                expected: stored_crc,
                actual: computed_crc,
            });
        }

        let compression_type = CompressionType::from_u8(bytes[64])?;

        let mut min_key = [0u8; MIN_MAX_KEY_TRUNCATION_LEN];
        min_key.copy_from_slice(&bytes[32..48]);
        let mut max_key = [0u8; MIN_MAX_KEY_TRUNCATION_LEN];
        max_key.copy_from_slice(&bytes[48..64]);

        Ok(Self {
            bloom_filter_offset: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            bloom_filter_size: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            index_block_offset: u64::from_le_bytes(bytes[12..20].try_into().unwrap()),
            index_block_size: u32::from_le_bytes(bytes[20..24].try_into().unwrap()),
            entry_count: u64::from_le_bytes(bytes[24..32].try_into().unwrap()),
            min_key,
            max_key,
            compression_type,
            format_version: u16::from_le_bytes(bytes[66..68].try_into().unwrap()),
            dedup_block_size: u32::from_le_bytes(bytes[76..80].try_into().unwrap()),
        })
    }

    pub fn dedup_block_offset(&self) -> u64 {
        self.bloom_filter_offset - self.dedup_block_size as u64
    }

    pub fn truncate_key(key: &CompositeKey) -> [u8; MIN_MAX_KEY_TRUNCATION_LEN] {
        let key_bytes = key.as_bytes();
        let mut result = [0u8; MIN_MAX_KEY_TRUNCATION_LEN];
        let copy_len = key_bytes.len().min(MIN_MAX_KEY_TRUNCATION_LEN);
        result[..copy_len].copy_from_slice(&key_bytes[..copy_len]);
        result
    }

    pub fn may_contain_key(&self, key: &CompositeKey) -> bool {
        let truncated = Self::truncate_key(key);
        truncated >= self.min_key && truncated <= self.max_key
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstHeader {
    pub compression: CompressionType,
    pub entry_count: u64,
}

impl SstHeader {
    pub fn encode(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];

        buf[0..4].copy_from_slice(&SSTABLE_MAGIC.to_le_bytes());
        buf[4..6].copy_from_slice(&FORMAT_VERSION.to_le_bytes());
        buf[6] = self.compression.as_u8();
        buf[7] = 0x00; // padding
        buf[8..16].copy_from_slice(&self.entry_count.to_le_bytes());

        buf
    }

    pub fn decode(bytes: &[u8]) -> FlushResult<Self> {
        if bytes.len() != HEADER_SIZE {
            return Err(FlushError::CorruptedData {
                message: "header must be exactly 16 bytes".into(),
            });
        }

        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != SSTABLE_MAGIC {
            return Err(FlushError::CorruptedData {
                message: "invalid SSTable header magic".into(),
            });
        }

        let compression = CompressionType::from_u8(bytes[6])?;
        let entry_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

        Ok(Self {
            compression,
            entry_count,
        })
    }
}
