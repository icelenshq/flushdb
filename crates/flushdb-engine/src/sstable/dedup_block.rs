use std::collections::HashSet;

use bytes::Bytes;
use flushdb_types::{FlushError, FlushResult, IdempotencyToken};

use super::hash::murmurhash3_x64_128;

pub struct DedupBlockBuilder {
    token_hashes: HashSet<[u8; 16]>,
}

impl Default for DedupBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl DedupBlockBuilder {
    pub fn new() -> Self {
        Self {
            token_hashes: HashSet::new(),
        }
    }

    pub fn add(&mut self, token: &IdempotencyToken) {
        if token.is_none() {
            return;
        }
        let hash = hash_token(token);
        self.token_hashes.insert(hash);
    }

    pub fn add_all(&mut self, tokens: &[IdempotencyToken]) {
        for token in tokens {
            self.add(token);
        }
    }

    pub fn build(self) -> DedupBlock {
        let mut hashes: Vec<[u8; 16]> = self.token_hashes.into_iter().collect();
        hashes.sort();
        DedupBlock { hashes }
    }

    pub fn is_empty(&self) -> bool {
        self.token_hashes.is_empty()
    }

    pub fn len(&self) -> usize {
        self.token_hashes.len()
    }
}

pub struct DedupBlock {
    hashes: Vec<[u8; 16]>,
}

impl DedupBlock {
    pub fn contains(&self, token: &IdempotencyToken) -> bool {
        if token.is_none() {
            return false;
        }
        let target = hash_token(token);
        self.hashes.binary_search(&target).is_ok()
    }

    pub fn len(&self) -> usize {
        self.hashes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.hashes.is_empty()
    }

    pub fn serialize(&self) -> Bytes {
        let count = self.hashes.len() as u32;
        let mut buf = Vec::with_capacity(4 + self.hashes.len() * 16);
        buf.extend_from_slice(&count.to_le_bytes());
        for hash in &self.hashes {
            buf.extend_from_slice(hash);
        }
        Bytes::from(buf)
    }

    pub fn deserialize(data: &[u8]) -> FlushResult<Self> {
        if data.len() < 4 {
            return Err(FlushError::CorruptedData {
                message: "dedup block too short".into(),
            });
        }

        let count = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let hash_data = &data[4..];

        if !hash_data.len().is_multiple_of(16) {
            return Err(FlushError::CorruptedData {
                message: "dedup block size not aligned to 16-byte hashes".into(),
            });
        }

        if hash_data.len() / 16 != count {
            return Err(FlushError::CorruptedData {
                message: "dedup block count mismatch".into(),
            });
        }

        let mut hashes = Vec::with_capacity(count);
        for i in 0..count {
            let offset = i * 16;
            let mut hash = [0u8; 16];
            hash.copy_from_slice(&hash_data[offset..offset + 16]);
            hashes.push(hash);
        }

        Ok(Self { hashes })
    }

    pub fn size_bytes(&self) -> usize {
        4 + self.hashes.len() * 16
    }
}

fn hash_token(token: &IdempotencyToken) -> [u8; 16] {
    let (h1, h2) = murmurhash3_x64_128(token.as_bytes(), 0x42);
    let mut hash = [0u8; 16];
    hash[0..8].copy_from_slice(&h1.to_le_bytes());
    hash[8..16].copy_from_slice(&h2.to_le_bytes());
    hash
}
