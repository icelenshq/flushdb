use std::collections::BTreeMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use flushdb_types::{CompositeKey, FlushError, FlushResult};

use crate::sstable::SstInfo;

// --- ManifestId ---

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestId(u64);

impl ManifestId {
    pub const ZERO: Self = ManifestId(0);

    pub fn new(id: u64) -> Self {
        Self(id)
    }

    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn to_path_string(&self) -> String {
        format!("{:020}", self.0)
    }

    pub fn from_path_string(s: &str) -> FlushResult<Self> {
        if s.len() != 20 {
            return Err(FlushError::InvalidArgument {
                message: format!("manifest ID must be 20 digits, got {} chars", s.len()),
            });
        }
        let id = s.parse::<u64>().map_err(|e| FlushError::InvalidArgument {
            message: format!("invalid manifest ID: {e}"),
        })?;
        Ok(Self(id))
    }
}

impl Serialize for ManifestId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_path_string())
    }
}

impl<'de> Deserialize<'de> for ManifestId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::from_path_string(&s).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_path_string())
    }
}

// --- Level ---

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Level {
    L0 = 0,
    L1 = 1,
    L2 = 2,
    L3 = 3,
}

impl Level {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::L0 => "L0",
            Self::L1 => "L1",
            Self::L2 => "L2",
            Self::L3 => "L3",
        }
    }

    pub fn parse(s: &str) -> FlushResult<Self> {
        match s {
            "L0" => Ok(Self::L0),
            "L1" => Ok(Self::L1),
            "L2" => Ok(Self::L2),
            "L3" => Ok(Self::L3),
            _ => Err(FlushError::InvalidArgument {
                message: format!("invalid level: {s}"),
            }),
        }
    }

    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    pub fn next(&self) -> Option<Self> {
        match self {
            Self::L0 => Some(Self::L1),
            Self::L1 => Some(Self::L2),
            Self::L2 => Some(Self::L3),
            Self::L3 => None,
        }
    }

    pub fn is_bottom(&self) -> bool {
        matches!(self, Self::L3)
    }

    pub fn max_size_bytes(&self) -> u64 {
        match self {
            Self::L0 => 0, // L0 is count-based, not size-based
            Self::L1 => 256 * 1024 * 1024,
            Self::L2 => 2560 * 1024 * 1024,
            Self::L3 => 25600 * 1024 * 1024,
        }
    }

    pub fn is_overlapping(&self) -> bool {
        matches!(self, Self::L0)
    }

    pub fn all() -> &'static [Level] {
        &[Self::L0, Self::L1, Self::L2, Self::L3]
    }
}

impl Serialize for Level {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for Level {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Self::parse(&s).map_err(serde::de::Error::custom)
    }
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

// --- SSTableMeta ---

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SSTableMeta {
    pub id: String,
    pub size_bytes: u64,
    pub entry_count: u64,
    #[serde(with = "base64_bytes")]
    pub min_key: Vec<u8>,
    #[serde(with = "base64_bytes")]
    pub max_key: Vec<u8>,
    pub bloom_filter_offset: u64,
    pub bloom_filter_size: u32,
    pub index_offset: u64,
    pub index_size: u32,
    pub created_at_ms: u64,
    pub sequence_range: (u64, u64),
    pub record_id_count: u64,
    pub run_id: Option<String>,
    pub fragment_index: Option<u32>,
    pub dedup_block_size: u32,
}

mod base64_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::Error;
        let encoded = base64_encode(bytes).map_err(S::Error::custom)?;
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        let s = String::deserialize(deserializer)?;
        base64_decode(&s).map_err(serde::de::Error::custom)
    }

    fn base64_encode(bytes: &[u8]) -> Result<String, String> {
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
        Ok(result)
    }

    fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
        fn char_to_val(c: u8) -> Result<u32, String> {
            match c {
                b'A'..=b'Z' => Ok((c - b'A') as u32),
                b'a'..=b'z' => Ok((c - b'a' + 26) as u32),
                b'0'..=b'9' => Ok((c - b'0' + 52) as u32),
                b'+' => Ok(62),
                b'/' => Ok(63),
                b'=' => Ok(0),
                _ => Err(format!("invalid base64 char: {c}")),
            }
        }

        let bytes = s.as_bytes();
        if !bytes.len().is_multiple_of(4) {
            return Err("invalid base64 length".into());
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
}

impl SSTableMeta {
    pub fn from_sst_info(
        info: &SstInfo,
        sequence_range: (u64, u64),
        record_id_count: u64,
        created_at_ms: u64,
    ) -> Self {
        let path = &info.path;
        let id = path
            .rsplit('/')
            .next()
            .unwrap_or(path)
            .trim_end_matches(".sst")
            .to_string();

        Self {
            id,
            size_bytes: info.file_size,
            entry_count: info.entry_count,
            min_key: info.min_key.as_bytes().to_vec(),
            max_key: info.max_key.as_bytes().to_vec(),
            bloom_filter_offset: info.bloom_filter_offset,
            bloom_filter_size: info.bloom_filter_size,
            index_offset: info.index_block_offset,
            index_size: info.index_block_size,
            created_at_ms,
            sequence_range,
            record_id_count,
            run_id: None,
            fragment_index: None,
            dedup_block_size: info.dedup_block_size,
        }
    }

    pub fn contains_key(&self, key: &CompositeKey) -> bool {
        let kb = key.as_bytes();
        kb >= self.min_key.as_slice() && kb <= self.max_key.as_slice()
    }

    pub fn overlaps(&self, other: &SSTableMeta) -> bool {
        self.overlaps_range(&other.min_key, &other.max_key)
    }

    pub fn overlaps_range(&self, start: &[u8], end: &[u8]) -> bool {
        !(self.max_key.as_slice() < start || self.min_key.as_slice() > end)
    }

    pub fn sst_path(&self, namespace: &str, level: Level) -> String {
        if let (Some(run_id), Some(frag_idx)) = (&self.run_id, self.fragment_index) {
            format!(
                "flushdb/{namespace}/sstables/{level}/run-{run_id}/frag-{frag_idx:04}.sst"
            )
        } else {
            format!("flushdb/{namespace}/sstables/{level}/{}.sst", self.id)
        }
    }
}

// --- BlobFileMeta ---

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobFileMeta {
    pub id: String,
    pub size_bytes: u64,
    pub live_bytes: u64,
    pub entry_count: u64,
    pub referenced_by_ssts: Vec<String>,
}

// --- Manifest ---

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub manifest_id: ManifestId,
    pub writer_epoch: u64,
    pub compactor_epoch: u64,
    pub namespace: String,
    pub created_at_ms: u64,
    pub last_flushed_sequence: u64,
    pub levels: BTreeMap<Level, Vec<SSTableMeta>>,
    pub blob_files: Vec<BlobFileMeta>,
    pub tombstone_compaction_watermarks: BTreeMap<Level, u64>,
    pub previous_manifest_id: ManifestId,
    pub is_snapshot: bool,
}

impl Manifest {
    pub fn new_empty(namespace: &str) -> Self {
        let mut levels = BTreeMap::new();
        for level in Level::all() {
            levels.insert(*level, Vec::new());
        }
        Self {
            format_version: 1,
            manifest_id: ManifestId::ZERO,
            writer_epoch: 0,
            compactor_epoch: 0,
            namespace: namespace.to_string(),
            created_at_ms: now_ms(),
            last_flushed_sequence: 0,
            levels,
            blob_files: Vec::new(),
            tombstone_compaction_watermarks: BTreeMap::new(),
            previous_manifest_id: ManifestId::ZERO,
            is_snapshot: false,
        }
    }

    pub fn sstables_at_level(&self, level: Level) -> &[SSTableMeta] {
        static EMPTY: Vec<SSTableMeta> = Vec::new();
        self.levels.get(&level).unwrap_or(&EMPTY)
    }

    pub fn l0_count(&self) -> usize {
        self.sstables_at_level(Level::L0).len()
    }

    pub fn level_size_bytes(&self, level: Level) -> u64 {
        self.sstables_at_level(level)
            .iter()
            .map(|m| m.size_bytes)
            .sum()
    }

    pub fn total_sstable_count(&self) -> usize {
        self.levels.values().map(|v| v.len()).sum()
    }

    pub fn all_sstable_ids(&self) -> Vec<&str> {
        self.levels
            .values()
            .flat_map(|v| v.iter().map(|m| m.id.as_str()))
            .collect()
    }

    pub fn find_overlapping(
        &self,
        level: Level,
        start: &[u8],
        end: &[u8],
    ) -> Vec<&SSTableMeta> {
        self.sstables_at_level(level)
            .iter()
            .filter(|m| m.overlaps_range(start, end))
            .collect()
    }

    pub fn find_sstable_for_key(
        &self,
        level: Level,
        key: &CompositeKey,
    ) -> Option<&SSTableMeta> {
        let ssts = self.sstables_at_level(level);
        if ssts.is_empty() {
            return None;
        }

        if level.is_overlapping() {
            return ssts.iter().find(|m| m.contains_key(key));
        }

        let kb = key.as_bytes();
        let idx = ssts.partition_point(|m| m.max_key.as_slice() < kb);
        if idx < ssts.len() && ssts[idx].contains_key(key) {
            Some(&ssts[idx])
        } else {
            None
        }
    }

    pub fn serialize(&self) -> FlushResult<Bytes> {
        let json = serde_json::to_vec_pretty(self).map_err(|e| FlushError::CorruptedData {
            message: format!("manifest serialization failed: {e}"),
        })?;
        Ok(Bytes::from(json))
    }

    pub fn deserialize(data: &[u8]) -> FlushResult<Self> {
        serde_json::from_slice(data).map_err(|e| FlushError::CorruptedData {
            message: format!("manifest deserialization failed: {e}"),
        })
    }
}

// --- ManifestUpdateTrigger ---

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ManifestUpdateTrigger {
    Flush,
    Compaction,
    GC,
}

// --- ManifestUpdate ---

#[derive(Clone, Debug)]
pub struct ManifestUpdate {
    pub trigger: ManifestUpdateTrigger,
    pub add_sstables: Vec<(Level, SSTableMeta)>,
    pub remove_sstables: Vec<(Level, String)>,
    pub new_last_flushed_sequence: Option<u64>,
    pub writer_epoch: u64,
    pub compactor_epoch: u64,
}

impl ManifestUpdate {
    pub fn apply(&self, current: &Manifest) -> FlushResult<Manifest> {
        let mut new_manifest = current.clone();
        new_manifest.previous_manifest_id = current.manifest_id;
        new_manifest.manifest_id = current.manifest_id.next();
        new_manifest.created_at_ms = now_ms();

        // Remove SSTables
        for (level, id) in &self.remove_sstables {
            let ssts = new_manifest.levels.entry(*level).or_default();
            let pos = ssts.iter().position(|m| m.id == *id);
            match pos {
                Some(idx) => {
                    ssts.remove(idx);
                }
                None => {
                    return Err(FlushError::InvalidArgument {
                        message: format!(
                            "cannot remove SSTable {id} from {level}: not found",
                        ),
                    });
                }
            }
        }

        // Add SSTables
        for (level, meta) in &self.add_sstables {
            let ssts = new_manifest.levels.entry(*level).or_default();
            ssts.push(meta.clone());

            // Keep L1+ sorted by min_key for binary search
            if !level.is_overlapping() {
                ssts.sort_by(|a, b| a.min_key.cmp(&b.min_key));
            }
        }

        // Update last_flushed_sequence
        if let Some(seq) = self.new_last_flushed_sequence {
            if seq < current.last_flushed_sequence {
                return Err(FlushError::InvalidArgument {
                    message: format!(
                        "cannot decrease last_flushed_sequence from {} to {}",
                        current.last_flushed_sequence, seq
                    ),
                });
            }
            new_manifest.last_flushed_sequence = seq;
        }

        // Update epochs
        new_manifest.writer_epoch = self.writer_epoch;
        new_manifest.compactor_epoch = self.compactor_epoch;

        Ok(new_manifest)
    }
}

// --- ManifestConfig ---

#[derive(Clone, Debug)]
pub struct ManifestConfig {
    pub snapshot_interval: u64,
    pub max_manifest_size: usize,
    pub pruning_batch_size: usize,
    pub base_path: String,
}

impl Default for ManifestConfig {
    fn default() -> Self {
        Self {
            snapshot_interval: 100,
            max_manifest_size: 16 * 1024 * 1024,
            pruning_batch_size: 50,
            base_path: "flushdb".to_string(),
        }
    }
}

// --- Path Helpers ---

pub fn manifest_path(base: &str, namespace: &str, id: &ManifestId) -> String {
    format!("{base}/{namespace}/manifests/{}", id.to_path_string())
}

pub fn manifest_prefix(base: &str, namespace: &str) -> String {
    format!("{base}/{namespace}/manifests/")
}

pub fn l0_sst_path(base: &str, namespace: &str, sst_id: &str) -> String {
    format!("{base}/{namespace}/sstables/L0/{sst_id}.sst")
}

pub fn run_fragment_path(
    base: &str,
    namespace: &str,
    level: Level,
    run_id: &str,
    fragment_index: u32,
) -> String {
    format!(
        "{base}/{namespace}/sstables/{level}/run-{run_id}/frag-{fragment_index:04}.sst"
    )
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
