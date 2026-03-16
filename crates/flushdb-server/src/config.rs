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
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default = "default_log_format")]
    pub log_format: String,
    #[serde(default = "default_maintenance_interval")]
    pub maintenance_interval_ms: u64,
    #[serde(default = "default_stats_interval")]
    pub stats_interval_ms: u64,
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
            log_level: default_log_level(),
            log_format: default_log_format(),
            maintenance_interval_ms: default_maintenance_interval(),
            stats_interval_ms: default_stats_interval(),
        }
    }
}

impl ServerConfig {
    pub fn from_env() -> FlushResult<Self> {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("FLUSHDB_GRPC_LISTEN_ADDR") {
            config.grpc_listen_addr =
                val.parse().map_err(|e: std::net::AddrParseError| {
                    FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_GRPC_LISTEN_ADDR: {e}"),
                    }
                })?;
        }

        if let Ok(val) = std::env::var("FLUSHDB_METRICS_LISTEN_ADDR") {
            config.metrics_listen_addr =
                val.parse().map_err(|e: std::net::AddrParseError| {
                    FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_METRICS_LISTEN_ADDR: {e}"),
                    }
                })?;
        }

        if let Ok(val) = std::env::var("FLUSHDB_NODE_ID") {
            config.node_id =
                val.parse().map_err(|e: std::num::ParseIntError| {
                    FlushError::InvalidArgument {
                        message: format!("invalid FLUSHDB_NODE_ID: {e}"),
                    }
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

        Ok(config)
    }

    pub fn from_file(path: &Path) -> FlushResult<Self> {
        let content = std::fs::read_to_string(path).map_err(FlushError::Io)?;
        let config: Self = serde_json::from_str(&content).map_err(|e| {
            FlushError::InvalidArgument {
                message: format!("invalid config JSON: {e}"),
            }
        })?;
        Ok(config)
    }
}
