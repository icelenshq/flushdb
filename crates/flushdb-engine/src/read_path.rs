use bytes::Bytes;
use flushdb_types::{CompositeKey, EntryType, FlushError, FlushResult};

use crate::block_fetcher::BlockFetcher;
use crate::memtable_list::MemtableList;
use crate::merge_iterator::{MergeEntry, MergeIterator, VecSource};
use crate::range_tombstone::RangeTombstoneIndex;
use crate::sstable_handle::LevelState;

// --- GetResult ---

#[derive(Debug, Clone)]
pub struct GetResult {
    pub key: CompositeKey,
    pub value: Bytes,
    pub metadata: Bytes,
    pub sequence_number: u64,
}

impl GetResult {
    pub fn from_merge_entry(entry: &MergeEntry) -> Self {
        Self {
            key: entry.composite_key.clone(),
            value: entry.value.clone(),
            metadata: entry.metadata.clone(),
            sequence_number: entry.sequence_number,
        }
    }
}

// --- PageToken ---

#[derive(Debug, Clone)]
pub struct PageToken {
    pub last_composite_key: CompositeKey,
    pub last_sequence_number: u64,
}

impl PageToken {
    pub fn encode(&self) -> Bytes {
        let key_bytes = self.last_composite_key.as_bytes();
        let key_len = key_bytes.len() as u32;
        let mut buf = Vec::with_capacity(4 + key_bytes.len() + 8);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(key_bytes);
        buf.extend_from_slice(&self.last_sequence_number.to_le_bytes());
        Bytes::from(buf)
    }

    pub fn decode(data: &[u8]) -> FlushResult<Self> {
        if data.len() < 12 {
            return Err(FlushError::InvalidArgument {
                message: "page token too short".into(),
            });
        }
        let key_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        if data.len() < 4 + key_len + 8 {
            return Err(FlushError::InvalidArgument {
                message: "page token truncated".into(),
            });
        }
        let key = CompositeKey::from_bytes(Bytes::copy_from_slice(&data[4..4 + key_len]))?;
        let seq = u64::from_le_bytes(data[4 + key_len..4 + key_len + 8].try_into().unwrap());
        Ok(Self {
            last_composite_key: key,
            last_sequence_number: seq,
        })
    }

    pub fn to_base64(&self) -> String {
        let bytes = self.encode();
        base64_encode(&bytes)
    }

    pub fn from_base64(s: &str) -> FlushResult<Self> {
        let bytes = base64_decode(s)?;
        Self::decode(&bytes)
    }
}

// --- RangeReadOptions ---

#[derive(Debug, Clone)]
pub struct RangeReadOptions {
    pub page_size_bytes: usize,
    pub item_limit: Option<usize>,
    pub resume_from: Option<PageToken>,
}

impl Default for RangeReadOptions {
    fn default() -> Self {
        Self {
            page_size_bytes: 2 * 1024 * 1024,
            item_limit: None,
            resume_from: None,
        }
    }
}

// --- RangeReadResult ---

#[derive(Debug)]
pub struct RangeReadResult {
    pub entries: Vec<MergeEntry>,
    pub next_page_token: Option<PageToken>,
    pub total_bytes: usize,
}

// --- RangeTombstoneCollector ---

pub struct RangeTombstoneCollector {
    index: RangeTombstoneIndex,
}

impl RangeTombstoneCollector {
    pub fn new() -> Self {
        Self {
            index: RangeTombstoneIndex::new(),
        }
    }

    pub fn add_from_memtable_list(&mut self, memtable_list: &MemtableList) {
        for ts in memtable_list.all_range_tombstones() {
            self.index.add(ts);
        }
    }

    pub fn add(
        &mut self,
        record_id: &[u8],
        start_key: &[u8],
        end_key: &[u8],
        sequence: u64,
    ) {
        use crate::range_tombstone::RangeTombstone;
        self.index.add(RangeTombstone {
            record_id: Bytes::copy_from_slice(record_id),
            start_key: Bytes::copy_from_slice(start_key),
            end_key: Bytes::copy_from_slice(end_key),
            sequence_number: sequence,
        });
    }

    pub fn covers(&self, record_id: &[u8], item_key: &[u8], entry_sequence: u64) -> bool {
        self.index.covers(record_id, item_key, entry_sequence)
    }
}

impl Default for RangeTombstoneCollector {
    fn default() -> Self {
        Self::new()
    }
}

// --- ReadPath ---

pub struct ReadPath<'a> {
    fetcher: &'a dyn BlockFetcher,
}

impl<'a> ReadPath<'a> {
    pub fn new(fetcher: &'a dyn BlockFetcher) -> Self {
        Self { fetcher }
    }

    pub async fn point_read(
        &self,
        key: &CompositeKey,
        memtable_list: &MemtableList,
        levels: &[LevelState],
    ) -> FlushResult<Option<MergeEntry>> {
        // Step 1: Check memtables (newest first) — MemtableList already returns newest
        if let Some(entry) = memtable_list.get(key) {
            let merge_entry = MergeEntry::from_memtable_entry(&entry);
            if merge_entry.is_tombstone() {
                return Ok(None);
            }
            // Check range tombstones from memtable
            if memtable_list.range_tombstone_covers(key.record_id(), key.item_key(), entry.sequence_number) {
                return Ok(None);
            }
            return Ok(Some(merge_entry));
        }

        // Step 2: Check SSTables
        let mut best: Option<MergeEntry> = None;

        for level_state in levels {
            let candidates = level_state.find_candidates_for_key(key);
            for handle in candidates {
                if let Some(block_entry) = handle.get(key, self.fetcher).await? {
                    let merge_entry = MergeEntry::from_block_entry(block_entry);
                    match &best {
                        Some(b) if b.sequence_number >= merge_entry.sequence_number => {}
                        _ => best = Some(merge_entry),
                    }
                }
            }
        }

        // Step 3: Check result
        match best {
            Some(entry) if entry.is_tombstone() => Ok(None),
            Some(entry) => {
                // Check range tombstones
                if memtable_list.range_tombstone_covers(
                    key.record_id(),
                    key.item_key(),
                    entry.sequence_number,
                ) {
                    return Ok(None);
                }
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    pub async fn range_read(
        &self,
        record_id: &[u8],
        start_key: Option<&[u8]>,
        end_key: Option<&[u8]>,
        options: RangeReadOptions,
        memtable_list: &MemtableList,
        levels: &[LevelState],
    ) -> FlushResult<RangeReadResult> {
        // Step 1: Determine key range
        let start = match &options.resume_from {
            Some(token) => token.last_composite_key.clone(),
            None => match start_key {
                Some(sk) => CompositeKey::new(record_id, sk)?,
                None => CompositeKey::min_key_for_record(record_id)?,
            },
        };
        let end = match end_key {
            Some(ek) => CompositeKey::new(record_id, ek)?,
            None => CompositeKey::max_key_for_record(record_id)?,
        };

        // Step 2: Collect sources
        let mut sources: Vec<Box<dyn crate::merge_iterator::MergeSource>> = Vec::new();
        let mut source_id = 0;

        // Memtable sources
        let memtable_entries = memtable_list.scan_all_with_tombstones(&start, &end);
        if !memtable_entries.is_empty() {
            let merge_entries: Vec<MergeEntry> = memtable_entries
                .into_iter()
                .map(|e| MergeEntry::from_memtable_entry(&e))
                .collect();
            sources.push(Box::new(VecSource::new(merge_entries, source_id)));
            source_id += 1;
        }

        // SSTable sources
        for level_state in levels {
            let candidates = level_state.find_candidates_for_range(&start, &end);
            for handle in candidates {
                let entries = handle.scan(&start, Some(&end), self.fetcher).await?;
                if !entries.is_empty() {
                    let merge_entries: Vec<MergeEntry> = entries
                        .into_iter()
                        .map(MergeEntry::from_block_entry)
                        .collect();
                    sources.push(Box::new(VecSource::new(merge_entries, source_id)));
                    source_id += 1;
                }
            }
        }

        // Step 3: Merge and filter
        let mut iter = MergeIterator::new(sources);
        let mut collector = RangeTombstoneCollector::new();
        collector.add_from_memtable_list(memtable_list);

        let mut entries = Vec::new();
        let mut total_bytes: usize = 0;
        let resume_key = options.resume_from.as_ref().map(|t| &t.last_composite_key);

        while let Some(entry) = iter.next_deduped() {
            // Skip entries before resume point
            if let Some(rk) = resume_key {
                if entry.composite_key <= *rk {
                    // Track range tombstones even for skipped entries
                    if entry.entry_type == EntryType::RangeDelete {
                        if let Some(ik) = extract_range_tombstone_fields(&entry) {
                            collector.add(record_id, &ik.0, &ik.1, entry.sequence_number);
                        }
                    }
                    continue;
                }
            }

            // Skip if not in target record
            if entry.composite_key.record_id() != record_id {
                continue;
            }

            // Track range tombstones
            if entry.entry_type == EntryType::RangeDelete {
                if let Some(ik) = extract_range_tombstone_fields(&entry) {
                    collector.add(record_id, &ik.0, &ik.1, entry.sequence_number);
                }
                continue;
            }

            // Skip point tombstones
            if entry.is_tombstone() {
                continue;
            }

            // Check range tombstone coverage
            if collector.covers(
                entry.composite_key.record_id(),
                entry.composite_key.item_key(),
                entry.sequence_number,
            ) {
                continue;
            }

            let entry_bytes = entry.composite_key.as_bytes().len()
                + entry.value.len()
                + entry.metadata.len();
            total_bytes += entry_bytes;
            entries.push(entry);

            // Check limits
            if total_bytes >= options.page_size_bytes {
                break;
            }
            if let Some(limit) = options.item_limit {
                if entries.len() >= limit {
                    break;
                }
            }
        }

        // Step 4: Build page token
        let next_page_token = if !iter.is_exhausted() {
            entries.last().map(|last| PageToken {
                last_composite_key: last.composite_key.clone(),
                last_sequence_number: last.sequence_number,
            })
        } else {
            None
        };

        Ok(RangeReadResult {
            entries,
            next_page_token,
            total_bytes,
        })
    }

    pub async fn multi_get(
        &self,
        record_id: &[u8],
        keys: &[&[u8]],
        memtable_list: &MemtableList,
        levels: &[LevelState],
    ) -> FlushResult<Vec<Option<MergeEntry>>> {
        let mut results = Vec::with_capacity(keys.len());
        for key_bytes in keys {
            let key = CompositeKey::new(record_id, key_bytes)?;
            let entry = self.point_read(&key, memtable_list, levels).await?;
            results.push(entry);
        }
        Ok(results)
    }
}

fn extract_range_tombstone_fields(entry: &MergeEntry) -> Option<(Vec<u8>, Vec<u8>)> {
    let item_key = entry.composite_key.item_key();
    if item_key.is_empty() {
        return Some((Vec::new(), entry.value.to_vec()));
    }
    if item_key[0] == 0xFF {
        let start_key = item_key[1..].to_vec();
        let end_key = entry.value.to_vec();
        return Some((start_key, end_key));
    }
    None
}

// Simple base64 encode/decode for PageToken
fn base64_encode(bytes: &[u8]) -> String {
    const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();
    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = chunk.get(1).copied().unwrap_or(0) as u32;
        let b2 = chunk.get(2).copied().unwrap_or(0) as u32;
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}

fn base64_decode(s: &str) -> FlushResult<Vec<u8>> {
    fn char_to_val(c: u8) -> FlushResult<u32> {
        match c {
            b'A'..=b'Z' => Ok((c - b'A') as u32),
            b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
            b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
            b'+' => Ok(62),
            b'/' => Ok(63),
            b'=' => Ok(0),
            _ => Err(FlushError::InvalidArgument {
                message: format!("invalid base64 char: {c}"),
            }),
        }
    }

    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return Err(FlushError::InvalidArgument {
            message: "invalid base64 length".into(),
        });
    }
    let mut result = Vec::new();
    for chunk in bytes.chunks(4) {
        let v0 = char_to_val(chunk[0])?;
        let v1 = char_to_val(chunk[1])?;
        let v2 = char_to_val(chunk[2])?;
        let v3 = char_to_val(chunk[3])?;
        let triple = (v0 << 18) | (v1 << 12) | (v2 << 6) | v3;
        result.push(((triple >> 16) & 0xFF) as u8);
        if chunk[2] != b'=' {
            result.push(((triple >> 8) & 0xFF) as u8);
        }
        if chunk[3] != b'=' {
            result.push((triple & 0xFF) as u8);
        }
    }
    Ok(result)
}
