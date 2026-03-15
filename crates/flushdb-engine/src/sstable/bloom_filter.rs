use std::collections::HashSet;

use bytes::Bytes;
use flushdb_types::{FlushError, FlushResult};

use super::hash::murmurhash3_x64_128;

#[derive(Debug, Clone)]
pub enum FilterBlock {
    Bloom(BloomFilter),
}

impl FilterBlock {
    pub fn maybe_contains(&self, record_id: &[u8]) -> bool {
        match self {
            Self::Bloom(bf) => bf.maybe_contains(record_id),
        }
    }

    pub fn serialize(&self) -> Bytes {
        match self {
            Self::Bloom(bf) => {
                let inner = bf.serialize();
                let mut out = Vec::with_capacity(1 + inner.len());
                out.push(0x00); // Bloom tag
                out.extend_from_slice(&inner);
                Bytes::from(out)
            }
        }
    }

    pub fn deserialize(data: &[u8]) -> FlushResult<Self> {
        if data.is_empty() {
            return Err(FlushError::CorruptedData {
                message: "filter block is empty".into(),
            });
        }
        match data[0] {
            0x00 => {
                let bf = BloomFilter::deserialize(&data[1..])?;
                Ok(Self::Bloom(bf))
            }
            tag => Err(FlushError::CorruptedData {
                message: format!("unknown filter block type: {tag}"),
            }),
        }
    }
}

pub struct BloomFilterBuilder {
    record_ids: HashSet<Bytes>,
    bits_per_key: u32,
}

impl BloomFilterBuilder {
    pub fn new(bits_per_key: u32) -> Self {
        Self {
            record_ids: HashSet::new(),
            bits_per_key,
        }
    }

    pub fn add(&mut self, record_id: &[u8]) {
        self.record_ids.insert(Bytes::copy_from_slice(record_id));
    }

    pub fn add_all(&mut self, record_ids: &HashSet<Bytes>) {
        for rid in record_ids {
            self.record_ids.insert(rid.clone());
        }
    }

    pub fn build(self) -> BloomFilter {
        let n = self.record_ids.len() as u64;

        if n == 0 {
            return BloomFilter {
                bits: vec![0u8; 8],
                num_bits: 64,
                num_hash_functions: 1,
                num_keys: 0,
            };
        }

        let mut num_bits = n * self.bits_per_key as u64;
        num_bits = num_bits.max(64);
        // Round up to nearest multiple of 8
        num_bits = (num_bits + 7) & !7;

        let num_hash_functions = ((num_bits / n) as f64 * std::f64::consts::LN_2) as u32;
        let num_hash_functions = num_hash_functions.max(1);

        let byte_count = (num_bits / 8) as usize;
        let mut bits = vec![0u8; byte_count];

        for rid in &self.record_ids {
            let (h1, h2) = murmurhash3_x64_128(rid, 0);
            for k in 0..num_hash_functions {
                let bit_index = h1.wrapping_add((k as u64).wrapping_mul(h2)) % num_bits;
                let byte_idx = (bit_index / 8) as usize;
                let bit_idx = (bit_index % 8) as u8;
                bits[byte_idx] |= 1 << bit_idx;
            }
        }

        BloomFilter {
            bits,
            num_bits,
            num_hash_functions,
            num_keys: n,
        }
    }

    pub fn estimated_size_bytes(&self) -> usize {
        (self.record_ids.len() * self.bits_per_key as usize) / 8
    }
}

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: u64,
    num_hash_functions: u32,
    num_keys: u64,
}

impl BloomFilter {
    pub fn maybe_contains(&self, record_id: &[u8]) -> bool {
        if self.num_keys == 0 {
            return false;
        }

        let (h1, h2) = murmurhash3_x64_128(record_id, 0);
        for k in 0..self.num_hash_functions {
            let bit_index = h1.wrapping_add((k as u64).wrapping_mul(h2)) % self.num_bits;
            let byte_idx = (bit_index / 8) as usize;
            let bit_idx = (bit_index % 8) as u8;
            if self.bits[byte_idx] & (1 << bit_idx) == 0 {
                return false;
            }
        }
        true
    }

    pub fn serialize(&self) -> Bytes {
        let mut buf = Vec::with_capacity(20 + self.bits.len());
        buf.extend_from_slice(&self.num_bits.to_le_bytes());
        buf.extend_from_slice(&self.num_hash_functions.to_le_bytes());
        buf.extend_from_slice(&self.num_keys.to_le_bytes());
        buf.extend_from_slice(&self.bits);
        Bytes::from(buf)
    }

    pub fn deserialize(data: &[u8]) -> FlushResult<Self> {
        if data.len() < 20 {
            return Err(FlushError::CorruptedData {
                message: "bloom filter data too short for header".into(),
            });
        }

        let num_bits = u64::from_le_bytes(data[0..8].try_into().unwrap());
        let num_hash_functions = u32::from_le_bytes(data[8..12].try_into().unwrap());
        let num_keys = u64::from_le_bytes(data[12..20].try_into().unwrap());
        let bits = data[20..].to_vec();

        if (bits.len() as u64) * 8 < num_bits {
            return Err(FlushError::CorruptedData {
                message: "bloom filter bit array too short".into(),
            });
        }

        Ok(Self {
            bits,
            num_bits,
            num_hash_functions,
            num_keys,
        })
    }

    pub fn size_bytes(&self) -> usize {
        self.bits.len()
    }

    pub fn false_positive_rate(&self) -> f64 {
        let k = self.num_hash_functions as f64;
        let n = self.num_keys as f64;
        let m = self.num_bits as f64;
        (1.0 - (-k * n / m).exp()).powf(k)
    }
}
