use std::time::Duration;

use crate::manifest::types::{Level, Manifest, SSTableMeta};

#[derive(Clone, Debug)]
pub struct CompactionConfig {
    pub l0_compaction_trigger: usize,
    pub l0_slowdown_trigger: usize,
    pub l0_stop_trigger: usize,
    pub l1_max_bytes: u64,
    pub l2_max_bytes: u64,
    pub l3_max_bytes: u64,
    pub level_size_ratio: f64,
    pub tombstone_ttl: Duration,
    pub tombstone_gc_interval: Duration,
    pub target_fragment_size: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_compaction_trigger: 4,
            l0_slowdown_trigger: 8,
            l0_stop_trigger: 12,
            l1_max_bytes: 256 * 1024 * 1024,
            l2_max_bytes: 2_560 * 1024 * 1024,
            l3_max_bytes: 25_600 * 1024 * 1024,
            level_size_ratio: 10.0,
            tombstone_ttl: Duration::from_secs(7 * 24 * 3600),
            tombstone_gc_interval: Duration::from_secs(3600),
            target_fragment_size: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionType {
    L0ToL1,
    LevelToLevel,
    TombstoneGC,
}

#[derive(Clone, Debug)]
pub struct CompactionTask {
    pub task_type: CompactionType,
    pub source_level: Level,
    pub target_level: Level,
    pub input_sstables: Vec<SSTableMeta>,
    pub target_sstables: Vec<SSTableMeta>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteStallStatus {
    Normal,
    Slowdown { l0_count: usize, delay_ms: u64 },
    Stopped { l0_count: usize },
}

impl WriteStallStatus {
    pub fn is_normal(&self) -> bool {
        matches!(self, Self::Normal)
    }

    pub fn is_stopped(&self) -> bool {
        matches!(self, Self::Stopped { .. })
    }

    pub fn delay_ms(&self) -> u64 {
        match self {
            Self::Normal => 0,
            Self::Slowdown { delay_ms, .. } => *delay_ms,
            Self::Stopped { .. } => 0,
        }
    }

    pub fn l0_count(&self) -> usize {
        match self {
            Self::Normal => 0,
            Self::Slowdown { l0_count, .. } => *l0_count,
            Self::Stopped { l0_count } => *l0_count,
        }
    }
}

pub struct CompactionScheduler {
    config: CompactionConfig,
}

impl CompactionScheduler {
    pub fn new(config: CompactionConfig) -> Self {
        Self { config }
    }

    pub fn config(&self) -> &CompactionConfig {
        &self.config
    }

    pub fn check_triggers(&self, manifest: &Manifest) -> Vec<CompactionTask> {
        let mut tasks = Vec::new();

        // Priority 1: L0→L1
        if let Some(task) = self.check_l0_trigger(manifest) {
            tasks.push(task);
        }

        // Priority 2: L1→L2
        if let Some(task) = self.check_level_trigger(manifest, Level::L1) {
            tasks.push(task);
        }

        // Priority 3: L2→L3
        if let Some(task) = self.check_level_trigger(manifest, Level::L2) {
            tasks.push(task);
        }

        tasks
    }

    pub fn check_l0_trigger(&self, manifest: &Manifest) -> Option<CompactionTask> {
        let l0_count = manifest.l0_count();
        if l0_count <= self.config.l0_compaction_trigger {
            return None;
        }

        let l0_ssts: Vec<SSTableMeta> = manifest.sstables_at_level(Level::L0).to_vec();

        // Compute combined key range of all L0 SSTables
        let (min_key, max_key) = combined_key_range(&l0_ssts)?;

        // Find overlapping L1 SSTables
        let target_ssts = manifest
            .find_overlapping(Level::L1, &min_key, &max_key)
            .into_iter()
            .cloned()
            .collect();

        Some(CompactionTask {
            task_type: CompactionType::L0ToL1,
            source_level: Level::L0,
            target_level: Level::L1,
            input_sstables: l0_ssts,
            target_sstables: target_ssts,
        })
    }

    pub fn check_level_trigger(&self, manifest: &Manifest, level: Level) -> Option<CompactionTask> {
        let max_bytes = match level {
            Level::L1 => self.config.l1_max_bytes,
            Level::L2 => self.config.l2_max_bytes,
            Level::L3 => return None, // L3 is bottom level, no overflow target
            Level::L0 => return None, // L0 uses count trigger, not size
        };

        let current_size = manifest.level_size_bytes(level);
        if current_size <= max_bytes {
            return None;
        }

        let target_level = level.next()?;
        let ssts = manifest.sstables_at_level(level);

        // Pick the oldest SSTable (earliest created_at_ms)
        let input = ssts.iter().min_by_key(|m| m.created_at_ms)?;

        // Find overlapping SSTables at target level
        let target_ssts = manifest
            .find_overlapping(target_level, &input.min_key, &input.max_key)
            .into_iter()
            .cloned()
            .collect();

        Some(CompactionTask {
            task_type: CompactionType::LevelToLevel,
            source_level: level,
            target_level,
            input_sstables: vec![input.clone()],
            target_sstables: target_ssts,
        })
    }

    pub fn write_stall_status(&self, manifest: &Manifest) -> WriteStallStatus {
        let l0_count = manifest.l0_count();

        if l0_count >= self.config.l0_stop_trigger {
            return WriteStallStatus::Stopped { l0_count };
        }

        if l0_count > self.config.l0_slowdown_trigger {
            let delay_ms = (l0_count - self.config.l0_slowdown_trigger) as u64;
            return WriteStallStatus::Slowdown { l0_count, delay_ms };
        }

        WriteStallStatus::Normal
    }
}

fn combined_key_range(ssts: &[SSTableMeta]) -> Option<(Vec<u8>, Vec<u8>)> {
    if ssts.is_empty() {
        return None;
    }

    let mut min_key = ssts[0].min_key.clone();
    let mut max_key = ssts[0].max_key.clone();

    for sst in &ssts[1..] {
        if sst.min_key < min_key {
            min_key = sst.min_key.clone();
        }
        if sst.max_key > max_key {
            max_key = sst.max_key.clone();
        }
    }

    Some((min_key, max_key))
}
