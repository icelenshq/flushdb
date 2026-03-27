use std::net::SocketAddr;
use std::time::Duration;

use flushdb_engine::{CacheStats, Level};
use flushdb_types::FlushResult;
use metrics::{counter, gauge, histogram};
use metrics_exporter_prometheus::PrometheusBuilder;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

pub struct MetricsHandle {
    _handle: metrics_exporter_prometheus::PrometheusHandle,
}

pub fn init_tracing(log_level: &str) -> FlushResult<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(log_level));

    let log_format = std::env::var("FLUSHDB_LOG_FORMAT").unwrap_or_default();
    let is_pretty = log_format.eq_ignore_ascii_case("pretty");

    let registry = tracing_subscriber::registry().with(env_filter);

    let result = if is_pretty {
        let layer = fmt::layer()
            .pretty()
            .with_thread_ids(true)
            .with_target(true);
        registry.with(layer).try_init()
    } else {
        let layer = fmt::layer().json().with_thread_ids(true).with_target(true);
        registry.with(layer).try_init()
    };

    match result {
        Ok(()) => Ok(()),
        Err(_) => Ok(()),
    }
}

pub fn init_metrics(listen_addr: SocketAddr) -> FlushResult<MetricsHandle> {
    let handle = PrometheusBuilder::new()
        .with_http_listener(listen_addr)
        .install_recorder()
        .map_err(|e| flushdb_types::FlushError::InvalidArgument {
            message: format!("failed to install metrics recorder: {e}"),
        })?;

    Ok(MetricsHandle { _handle: handle })
}

pub fn record_write_latency(namespace: &str, operation: &str, duration: Duration) {
    counter!("flushdb_write_requests_total", "namespace" => namespace.to_owned(), "operation" => operation.to_owned()).increment(1);
    histogram!("flushdb_write_latency_seconds", "namespace" => namespace.to_owned(), "operation" => operation.to_owned()).record(duration.as_secs_f64());
}

pub fn record_read_latency(namespace: &str, operation: &str, duration: Duration) {
    counter!("flushdb_read_requests_total", "namespace" => namespace.to_owned(), "operation" => operation.to_owned()).increment(1);
    histogram!("flushdb_read_latency_seconds", "namespace" => namespace.to_owned(), "operation" => operation.to_owned()).record(duration.as_secs_f64());
}

pub fn record_flush(namespace: &str, duration: Duration, bytes: u64) {
    counter!("flushdb_flush_total", "namespace" => namespace.to_owned()).increment(1);
    histogram!("flushdb_flush_duration_seconds", "namespace" => namespace.to_owned())
        .record(duration.as_secs_f64());
    counter!("flushdb_flush_bytes_total", "namespace" => namespace.to_owned()).increment(bytes);
}

pub fn record_compaction(namespace: &str, level: &str, duration: Duration, bytes: u64) {
    counter!("flushdb_compaction_total", "namespace" => namespace.to_owned(), "level" => level.to_owned()).increment(1);
    histogram!("flushdb_compaction_duration_seconds", "namespace" => namespace.to_owned(), "level" => level.to_owned()).record(duration.as_secs_f64());
    counter!("flushdb_compaction_bytes_total", "namespace" => namespace.to_owned(), "level" => level.to_owned()).increment(bytes);
}

pub fn update_cache_stats(namespace: &str, stats: &CacheStats) {
    gauge!("flushdb_cache_hits_total", "namespace" => namespace.to_owned()).set(stats.hits as f64);
    gauge!("flushdb_cache_misses_total", "namespace" => namespace.to_owned())
        .set(stats.misses as f64);
    let ratio = if stats.hits + stats.misses > 0 {
        stats.hits as f64 / (stats.hits + stats.misses) as f64
    } else {
        0.0
    };
    gauge!("flushdb_cache_hit_ratio", "namespace" => namespace.to_owned()).set(ratio);
    gauge!("flushdb_cache_size_bytes", "namespace" => namespace.to_owned())
        .set(stats.weighted_size_bytes as f64);
}

pub fn update_level_stats(namespace: &str, levels: &[(Level, u64)], l0_count: usize) {
    gauge!("flushdb_l0_file_count", "namespace" => namespace.to_owned()).set(l0_count as f64);
    for (level, size) in levels {
        gauge!("flushdb_level_size_bytes", "namespace" => namespace.to_owned(), "level" => level.as_str().to_owned()).set(*size as f64);
    }
}

pub fn record_write_items(namespace: &str, count: u64) {
    counter!("flushdb_write_items_total", "namespace" => namespace.to_owned()).increment(count);
}

pub fn record_write_bytes(namespace: &str, bytes: u64) {
    counter!("flushdb_write_bytes_total", "namespace" => namespace.to_owned()).increment(bytes);
}

pub fn record_read_items(namespace: &str, count: u64) {
    counter!("flushdb_read_items_total", "namespace" => namespace.to_owned()).increment(count);
}

pub fn record_read_bytes(namespace: &str, bytes: u64) {
    counter!("flushdb_read_bytes_total", "namespace" => namespace.to_owned()).increment(bytes);
}

pub fn record_idempotent_dedup(namespace: &str) {
    counter!("flushdb_idempotent_dedup_total", "namespace" => namespace.to_owned()).increment(1);
}

pub fn record_slo_early_return(namespace: &str) {
    counter!("flushdb_slo_early_return_total", "namespace" => namespace.to_owned()).increment(1);
}

pub fn record_page_token_resume(namespace: &str) {
    counter!("flushdb_page_token_resumes_total", "namespace" => namespace.to_owned()).increment(1);
}

pub fn set_active_connections(count: usize) {
    gauge!("flushdb_active_connections").set(count as f64);
}

pub fn set_namespace_count(count: usize) {
    gauge!("flushdb_namespace_count").set(count as f64);
}

pub fn set_partition_count(count: usize) {
    gauge!("flushdb_partition_count").set(count as f64);
}
