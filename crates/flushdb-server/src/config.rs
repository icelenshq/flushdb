use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use flushdb_types::{FlushError, FlushResult};

fn default_grpc_addr() -> SocketAddr {
    "0.0.0.0:50051".parse().expect("valid default grpc addr")
}

fn default_metrics_addr() -> SocketAddr {
    "0.0.0.0:9090".parse().expect("valid default metrics addr")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("./data")
}

fn default_partition_count() -> u32 {
    4
}

fn default_memtable_size() -> u64 {
    64
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

fn default_log_level() -> String {
    "info".to_string()
}

fn default_log_format() -> String {
    "json".to_string()
}

fn default_maintenance_interval() -> u64 {
    1000
}

fn default_stats_interval() -> u64 {
    10000
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServerConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_listen_addr: SocketAddr,
    #[serde(default = "default_metrics_addr")]
    pub metrics_listen_addr: SocketAddr,
    #[serde(default)]
    pub node_id: u16,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default)]
    pub s3_bucket: Option<String>,
    #[serde(default = "default_partition_count")]
    pub default_partition_count: u32,
    #[serde(default = "default_memtable_size")]
    pub default_memtable_size_mb: u64,
    #[serde(default = "default_wal_fsync_mode")]
    pub default_wal_fsync_mode: String,
    #[serde(default = "default_wal_group_commit_interval_us")]
    pub default_wal_group_commit_interval_us: u64,
    #[serde(default = "default_wal_batch_sync_interval_ms")]
    pub default_wal_batch_sync_interval_ms: u64,
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default = "default_maintenance_interval")]
    pub maintenance_interval_ms: u64,
    #[serde(default = "default_stats_interval")]
    pub stats_interval_ms: u64,
    #[serde(default)]
    pub default_namespaces: Vec<(String, u32)>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            grpc_listen_addr: default_grpc_addr(),
            metrics_listen_addr: default_metrics_addr(),
            node_id: 0,
            data_dir: default_data_dir(),
            s3_bucket: None,
            default_partition_count: default_partition_count(),
            default_memtable_size_mb: default_memtable_size(),
            default_wal_fsync_mode: default_wal_fsync_mode(),
            default_wal_group_commit_interval_us: default_wal_group_commit_interval_us(),
            default_wal_batch_sync_interval_ms: default_wal_batch_sync_interval_ms(),
            log_level: default_log_level(),
            log_format: default_log_format(),
            maintenance_interval_ms: default_maintenance_interval(),
            stats_interval_ms: default_stats_interval(),
            default_namespaces: Vec::new(),
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> FlushResult<Self> {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("FLUSHDB_GRPC_LISTEN_ADDR") {
            config.grpc_listen_addr =
                val.parse()
                    .map_err(|e: std::net::AddrParseError| FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_GRPC_LISTEN_ADDR: {e}"),
                    })?;
        }

        if let Ok(val) = std::env::var("FLUSHDB_METRICS_LISTEN_ADDR") {
            config.metrics_listen_addr =
                val.parse()
                    .map_err(|e: std::net::AddrParseError| FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_METRICS_LISTEN_ADDR: {e}"),
                    })?;
        }

        if let Ok(val) = std::env::var("FLUSHDB_NODE_ID") {
            config.node_id =
                val.parse()
                    .map_err(|e: std::num::ParseIntError| FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_NODE_ID: {e}"),
                    })?;
        }

        if let Ok(val) = std::env::var("FLUSHDB_DATA_DIR") {
            config.data_dir = PathBuf::from(val);
        }

        if let Ok(val) = std::env::var("FLUSHDB_S3_BUCKET") {
            config.s3_bucket = Some(val);
        }

        if let Ok(val) = std::env::var("FLUSHDB_LOG_LEVEL") {
            config.log_level = val;
        }

        if let Ok(val) = std::env::var("FLUSHDB_LOG_FORMAT") {
            config.log_format = val;
        }

        if let Ok(val) = std::env::var("FLUSHDB_DEFAULT_NAMESPACES") {
            let mut namespaces = Vec::new();
            for entry in val.split(',') {
                let entry = entry.trim();
                if entry.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = entry.splitn(2, ':').collect();
                if parts.len() != 2 {
                    return Err(FlushError::InvalidArgument {
                        message: format!(
                            "invalid FLUSHDB_DEFAULT_NAMESPACES entry '{}': expected 'name:partition_count'",
                            entry
                        ),
                    });
                }
                let name = parts[0].to_string();
                let count: u32 = parts[1].parse().map_err(|e: std::num::ParseIntError| {
                    FlushError::InvalidArgument {
                        message: format!(
                            "invalid partition count in FLUSHDB_DEFAULT_NAMESPACES entry '{}': {}",
                            entry, e
                        ),
                    }
                })?;
                namespaces.push((name, count));
            }
            config.default_namespaces = namespaces;
        }

        if let Ok(val) = std::env::var("FLUSHDB_DEFAULT_MEMTABLE_SIZE_MB") {
            config.default_memtable_size_mb =
                val.parse()
                    .map_err(|e: std::num::ParseIntError| FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_DEFAULT_MEMTABLE_SIZE_MB: {e}"),
                    })?;
        }

        if let Ok(val) = std::env::var("FLUSHDB_DEFAULT_WAL_FSYNC_MODE") {
            config.default_wal_fsync_mode = val;
        }

        if let Ok(val) = std::env::var("FLUSHDB_DEFAULT_WAL_GROUP_COMMIT_INTERVAL_US") {
            config.default_wal_group_commit_interval_us =
                val.parse()
                    .map_err(|e: std::num::ParseIntError| FlushError::InvalidArgument {
                        message: format!(
                            "invalid FLUSHDB_DEFAULT_WAL_GROUP_COMMIT_INTERVAL_US: {e}"
                        ),
                    })?;
        }

        if let Ok(val) = std::env::var("FLUSHDB_DEFAULT_WAL_BATCH_SYNC_INTERVAL_MS") {
            config.default_wal_batch_sync_interval_ms =
                val.parse()
                    .map_err(|e: std::num::ParseIntError| FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_DEFAULT_WAL_BATCH_SYNC_INTERVAL_MS: {e}"),
                    })?;
        }

        config.validate()?;
        Ok(config)
    }

    pub fn from_file(path: &Path) -> FlushResult<Self> {
        let content = std::fs::read_to_string(path).map_err(FlushError::Io)?;
        let config: Self =
            serde_json::from_str(&content).map_err(|e| FlushError::InvalidArgument {
                message: format!("invalid config JSON: {e}"),
            })?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> FlushResult<()> {
        validate_default_wal_fsync_mode(&self.default_wal_fsync_mode)?;
        if self.default_wal_batch_sync_interval_ms == 0 {
            return Err(FlushError::InvalidArgument {
                message: "default_wal_batch_sync_interval_ms: must be > 0".to_string(),
            });
        }
        Ok(())
    }
}

fn validate_default_wal_fsync_mode(mode: &str) -> FlushResult<()> {
    match mode {
        "sync" | "batch_sync" => Ok(()),
        other => Err(FlushError::InvalidArgument {
            message: format!(
                "default_wal_fsync_mode: must be 'sync' or 'batch_sync', got '{other}'"
            ),
        }),
    }
}
