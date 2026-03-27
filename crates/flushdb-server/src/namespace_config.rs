use std::path::PathBuf;
use std::time::Duration;

use flushdb_engine::{
    CacheConfig, CompactionConfig, EngineConfig, FlushConfig, FsyncMode, ManifestConfig,
    MemtableConfig, SstConfig, WalConfig,
};
use flushdb_types::{FlushError, FlushResult};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PartitionKeyStrategy {
    Simple,
    Composite {
        delimiter: String,
        field_indices: Vec<usize>,
    },
    Prefix {
        length: usize,
    },
    CustomHash {
        hash_name: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ConsistencyScope {
    #[default]
    Local,
    Global,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum ConsistencyTarget {
    #[default]
    ReadYourWrites,
    Eventual,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum WriteConsistency {
    One,
    #[default]
    Quorum,
    All,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageLayerType {
    S3,
    Cache,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLayerConfig {
    #[serde(default)]
    pub consistency_scope: ConsistencyScope,
    #[serde(default)]
    pub consistency_target: ConsistencyTarget,
    #[serde(default)]
    pub default_ttl_ms: Option<u64>,
}

impl StorageLayerConfig {
    pub fn default_ttl(&self) -> Option<std::time::Duration> {
        self.default_ttl_ms.map(std::time::Duration::from_millis)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageLayer {
    pub id: String,
    pub layer_type: StorageLayerType,
    pub config: StorageLayerConfig,
}

fn default_s3_path_prefix(name: &str) -> String {
    format!("flushdb/{name}")
}

fn default_memtable_size() -> u64 {
    67_108_864
}

fn default_compaction_strategy() -> String {
    "LEVELED".to_string()
}

fn default_bloom_fp() -> f64 {
    0.01
}

fn default_page_size() -> u32 {
    2_097_152
}

fn default_max_page_size() -> u32 {
    8_388_608
}

fn default_target_slo() -> u64 {
    10
}

fn default_max_slo() -> u64 {
    500
}

fn default_write_consistency() -> WriteConsistency {
    WriteConsistency::Quorum
}

fn default_replication_factor() -> u32 {
    3
}

fn default_wal_fsync_mode() -> String {
    "sync".to_string()
}

fn default_wal_group_commit_interval_us() -> u64 {
    200
}

fn default_wal_batch_sync_interval_ms() -> u64 {
    10
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NamespaceConfig {
    pub name: String,
    pub partition_key_strategy: PartitionKeyStrategy,
    pub partition_count: u32,
    #[serde(default = "serde_default_s3_path_prefix")]
    pub s3_path_prefix: String,
    #[serde(default)]
    pub persistence: Vec<StorageLayer>,
    #[serde(default = "default_memtable_size")]
    pub memtable_size_threshold: u64,
    #[serde(default = "default_compaction_strategy")]
    pub compaction_strategy: String,
    #[serde(default = "default_bloom_fp")]
    pub bloom_filter_fp_rate: f64,
    #[serde(default = "default_page_size")]
    pub default_page_size_bytes: u32,
    #[serde(default = "default_max_page_size")]
    pub max_page_size_bytes: u32,
    #[serde(default = "default_target_slo")]
    pub target_latency_slo_ms: u64,
    #[serde(default = "default_max_slo")]
    pub max_latency_slo_ms: u64,
    #[serde(default = "default_write_consistency")]
    pub write_consistency: WriteConsistency,
    #[serde(default = "default_replication_factor")]
    pub replication_factor: u32,
    #[serde(default = "default_wal_fsync_mode")]
    pub wal_fsync_mode: String,
    #[serde(default = "default_wal_group_commit_interval_us")]
    pub wal_group_commit_interval_us: u64,
    #[serde(default = "default_wal_batch_sync_interval_ms")]
    pub wal_batch_sync_interval_ms: u64,
}

fn serde_default_s3_path_prefix() -> String {
    "flushdb/default".to_string()
}

impl NamespaceConfig {
    pub fn new(name: String, partition_count: u32) -> FlushResult<Self> {
        Self::with_strategy(name, PartitionKeyStrategy::Simple, partition_count)
    }

    pub fn with_strategy(
        name: String,
        strategy: PartitionKeyStrategy,
        partition_count: u32,
    ) -> FlushResult<Self> {
        let s3_path_prefix = default_s3_path_prefix(&name);
        let config = Self {
            name,
            partition_key_strategy: strategy,
            partition_count,
            s3_path_prefix,
            persistence: Vec::new(),
            memtable_size_threshold: default_memtable_size(),
            compaction_strategy: default_compaction_strategy(),
            bloom_filter_fp_rate: default_bloom_fp(),
            default_page_size_bytes: default_page_size(),
            max_page_size_bytes: default_max_page_size(),
            target_latency_slo_ms: default_target_slo(),
            max_latency_slo_ms: default_max_slo(),
            write_consistency: default_write_consistency(),
            replication_factor: default_replication_factor(),
            wal_fsync_mode: default_wal_fsync_mode(),
            wal_group_commit_interval_us: default_wal_group_commit_interval_us(),
            wal_batch_sync_interval_ms: default_wal_batch_sync_interval_ms(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> FlushResult<()> {
        if self.name.is_empty() {
            return Err(FlushError::InvalidArgument {
                message: "name: must not be empty".to_string(),
            });
        }
        if self.name.contains('/') || self.name.contains('\0') {
            return Err(FlushError::InvalidArgument {
                message: "name: must not contain / or null bytes".to_string(),
            });
        }
        if self.partition_count == 0 {
            return Err(FlushError::InvalidArgument {
                message: "partition_count: must be > 0".to_string(),
            });
        }
        if !self.partition_count.is_power_of_two() {
            return Err(FlushError::InvalidArgument {
                message: "partition_count: must be a power of 2".to_string(),
            });
        }
        if self.bloom_filter_fp_rate <= 0.0 || self.bloom_filter_fp_rate >= 1.0 {
            return Err(FlushError::InvalidArgument {
                message: "bloom_filter_fp_rate: must be in (0.0, 1.0)".to_string(),
            });
        }
        if self.default_page_size_bytes > self.max_page_size_bytes {
            return Err(FlushError::InvalidArgument {
                message: "default_page_size_bytes: must be <= max_page_size_bytes".to_string(),
            });
        }
        if self.target_latency_slo_ms > self.max_latency_slo_ms {
            return Err(FlushError::InvalidArgument {
                message: "target_latency_slo_ms: must be <= max_latency_slo_ms".to_string(),
            });
        }
        parse_fsync_mode(&self.wal_fsync_mode)?;
        if self.wal_batch_sync_interval_ms == 0 {
            return Err(FlushError::InvalidArgument {
                message: "wal_batch_sync_interval_ms: must be > 0".to_string(),
            });
        }
        self.validate_strategy()?;
        Ok(())
    }

    fn validate_strategy(&self) -> FlushResult<()> {
        match &self.partition_key_strategy {
            PartitionKeyStrategy::Composite { field_indices, .. } => {
                if field_indices.is_empty() {
                    return Err(FlushError::InvalidArgument {
                        message: "field_indices: must have at least one field index".to_string(),
                    });
                }
            }
            PartitionKeyStrategy::Prefix { length } => {
                if *length == 0 {
                    return Err(FlushError::InvalidArgument {
                        message: "prefix_length: must be > 0".to_string(),
                    });
                }
            }
            PartitionKeyStrategy::Simple | PartitionKeyStrategy::CustomHash { .. } => {}
        }
        Ok(())
    }

    pub fn s3_path_prefix(&self) -> &str {
        &self.s3_path_prefix
    }

    pub fn engine_config(&self, local_dir: PathBuf) -> EngineConfig {
        let bloom_bits = fp_rate_to_bits(self.bloom_filter_fp_rate);
        let sst_config = SstConfig::default().with_bloom_bits_per_key(bloom_bits);
        let wal_fsync_mode = parse_fsync_mode(&self.wal_fsync_mode)
            .expect("validated namespace config must contain a supported WAL fsync mode");

        EngineConfig {
            memtable_config: MemtableConfig {
                size_threshold: self.memtable_size_threshold as usize,
                ..Default::default()
            },
            wal_config: WalConfig {
                fsync_mode: wal_fsync_mode,
                group_commit_interval: Duration::from_micros(self.wal_group_commit_interval_us),
                batch_sync_interval: Duration::from_millis(self.wal_batch_sync_interval_ms),
                ..WalConfig::default()
            },
            flush_config: FlushConfig {
                sst_config,
                ..Default::default()
            },
            compaction_config: CompactionConfig::default(),
            manifest_config: ManifestConfig {
                base_path: self.s3_path_prefix.clone(),
                ..Default::default()
            },
            cache_config: CacheConfig::default(),
            namespace: self.name.clone(),
            local_dir,
        }
    }

    pub fn can_update_from(&self, other: &NamespaceConfig) -> FlushResult<()> {
        if self.name != other.name {
            return Err(FlushError::InvalidArgument {
                message: format!(
                    "name is immutable: cannot change from '{}' to '{}'",
                    self.name, other.name
                ),
            });
        }
        if self.partition_key_strategy != other.partition_key_strategy {
            return Err(FlushError::InvalidArgument {
                message: "partition_key_strategy is immutable: cannot change after creation"
                    .to_string(),
            });
        }
        if self.partition_count != other.partition_count {
            return Err(FlushError::InvalidArgument {
                message: format!(
                    "partition_count is immutable: cannot change from {} to {}",
                    self.partition_count, other.partition_count
                ),
            });
        }
        Ok(())
    }
}

fn fp_rate_to_bits(fp: f64) -> u32 {
    ((-fp.ln() / (2.0_f64.ln().powi(2))).ceil()) as u32
}

fn parse_fsync_mode(mode: &str) -> FlushResult<FsyncMode> {
    match mode {
        "sync" => Ok(FsyncMode::Sync),
        "batch_sync" => Ok(FsyncMode::BatchSync),
        other => Err(FlushError::InvalidArgument {
            message: format!("wal_fsync_mode: must be 'sync' or 'batch_sync', got '{other}'"),
        }),
    }
}
