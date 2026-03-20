# Task 11: Observability (Metrics + Tracing)

**Crate:** `flushdb-server`
**File:** `src/observability.rs`
**Depends on:** Nothing
**Estimated complexity:** M
**Design reference:** STORAGE_DESIGN.md §25 (Operational Concerns), Phase 7 §Future Work Considerations

---

## Goal

Set up structured logging via `tracing-subscriber` and Prometheus-compatible metrics via the `metrics` + `metrics-exporter-prometheus` crates. Instrument all critical paths: write latency, read latency, flush duration, compaction throughput, cache hit rate, and L0 file count. These metrics enable operational dashboards and alerting without retrofitting instrumentation later.

---

## What to Build

### 11.1 Tracing Setup

| Function | Signature | Behavior |
|----------|-----------|----------|
| `init_tracing` | `(log_level: &str) -> FlushResult<()>` | Initializes `tracing-subscriber` with env-filter and JSON or pretty-print formatting. |

**Configuration:**
- Default log level: `info`
- Overridable via `RUST_LOG` env var
- Format: JSON for production, pretty-print when `FLUSHDB_LOG_FORMAT=pretty`
- Include timestamps, span context, and thread IDs

### 11.2 Metrics Registry

| Function | Signature | Behavior |
|----------|-----------|----------|
| `init_metrics` | `(listen_addr: SocketAddr) -> FlushResult<MetricsHandle>` | Installs PrometheusBuilder as the metrics recorder, starts HTTP listener for `/metrics` endpoint. Returns handle for clean shutdown. |

**MetricsHandle struct:**
```
MetricsHandle {
    // Handle to the Prometheus exporter for shutdown
}
```

### 11.3 Metric Definitions

All metrics use the `metrics` crate macros (`counter!`, `histogram!`, `gauge!`).

#### Write Path Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `flushdb_write_requests_total` | Counter | `namespace`, `operation` (put/delete) | Total write requests |
| `flushdb_write_latency_seconds` | Histogram | `namespace`, `operation` | Write request latency |
| `flushdb_write_items_total` | Counter | `namespace` | Total items written |
| `flushdb_write_bytes_total` | Counter | `namespace` | Total bytes written (value + metadata) |
| `flushdb_idempotent_dedup_total` | Counter | `namespace` | Duplicate token rejections |
| `flushdb_write_stall_total` | Counter | `namespace`, `stall_type` (slowdown/stopped) | Write stall events |

#### Read Path Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `flushdb_read_requests_total` | Counter | `namespace`, `operation` (get/scan) | Total read requests |
| `flushdb_read_latency_seconds` | Histogram | `namespace`, `operation` | Read request latency |
| `flushdb_read_items_total` | Counter | `namespace` | Total items returned |
| `flushdb_read_bytes_total` | Counter | `namespace` | Total bytes returned |
| `flushdb_slo_early_return_total` | Counter | `namespace` | SLO-triggered early returns |
| `flushdb_page_token_resumes_total` | Counter | `namespace` | Page token continuation requests |

#### Storage Engine Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `flushdb_flush_duration_seconds` | Histogram | `namespace` | Memtable flush duration |
| `flushdb_flush_total` | Counter | `namespace` | Total flush operations |
| `flushdb_flush_bytes_total` | Counter | `namespace` | Total bytes flushed to SSTable |
| `flushdb_compaction_duration_seconds` | Histogram | `namespace`, `level` | Compaction duration |
| `flushdb_compaction_total` | Counter | `namespace`, `level` | Total compaction operations |
| `flushdb_compaction_bytes_total` | Counter | `namespace`, `level` | Total bytes compacted |
| `flushdb_l0_file_count` | Gauge | `namespace` | Current L0 SSTable count |
| `flushdb_level_size_bytes` | Gauge | `namespace`, `level` | Current level size in bytes |

#### Cache Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `flushdb_cache_hits_total` | Counter | `namespace`, `cache_type` (block/metadata) | Cache hits |
| `flushdb_cache_misses_total` | Counter | `namespace`, `cache_type` | Cache misses |
| `flushdb_cache_hit_ratio` | Gauge | `namespace`, `cache_type` | Hit ratio (updated periodically) |
| `flushdb_cache_size_bytes` | Gauge | `namespace`, `cache_type` | Current cache size |

#### Server Metrics

| Metric Name | Type | Labels | Description |
|-------------|------|--------|-------------|
| `flushdb_active_connections` | Gauge | — | Current gRPC connections |
| `flushdb_namespace_count` | Gauge | — | Number of registered namespaces |
| `flushdb_partition_count` | Gauge | — | Total partition count |

### 11.4 Metrics Recording Helpers

Convenience functions for recording metrics from handler and engine code:

| Function | Signature | Behavior |
|----------|-----------|----------|
| `record_write_latency` | `(namespace: &str, operation: &str, duration: Duration)` | Records write histogram and increments counter |
| `record_read_latency` | `(namespace: &str, operation: &str, duration: Duration)` | Records read histogram and increments counter |
| `record_flush` | `(namespace: &str, duration: Duration, bytes: u64)` | Records flush metrics |
| `record_compaction` | `(namespace: &str, level: &str, duration: Duration, bytes: u64)` | Records compaction metrics |
| `update_cache_stats` | `(namespace: &str, stats: &CacheStats)` | Updates cache gauges from engine CacheStats |
| `update_level_stats` | `(namespace: &str, levels: &[(Level, u64)], l0_count: usize)` | Updates level size gauges and L0 count |

### 11.5 Periodic Stats Collection

A background task that periodically collects engine stats and updates gauges:

| Function | Signature | Behavior |
|----------|-----------|----------|
| `spawn_stats_collector` | `(namespace_manager: Arc<NamespaceManager<B>>, interval: Duration) -> JoinHandle<()>` | Spawns tokio task that periodically calls `update_cache_stats` and `update_level_stats` for all namespaces |

Default interval: 10 seconds.

---

## Tests

**File:** `crates/flushdb-server/tests/observability_tests.rs`

### Tracing Tests
| Test | What It Validates |
|------|-------------------|
| `test_init_tracing_default` | Default initialization succeeds |
| `test_init_tracing_custom_level` | Custom log level applied |

### Metrics Setup Tests
| Test | What It Validates |
|------|-------------------|
| `test_init_metrics_starts_exporter` | Metrics exporter starts and binds to address |
| `test_metrics_endpoint_responds` | HTTP GET `/metrics` returns Prometheus text format |

### Metrics Recording Tests
| Test | What It Validates |
|------|-------------------|
| `test_record_write_latency` | `record_write_latency` increments counter and records histogram |
| `test_record_read_latency` | `record_read_latency` records correctly |
| `test_record_flush` | Flush counter and histogram updated |
| `test_record_compaction` | Compaction counter and histogram updated with level label |
| `test_update_cache_stats` | Cache gauges reflect CacheStats values |
| `test_update_level_stats` | Level size gauges and L0 count updated |

### Helper Tests
| Test | What It Validates |
|------|-------------------|
| `test_metrics_with_namespace_label` | Metrics correctly include namespace label |
| `test_multiple_namespaces_independent` | Metrics for namespace A don't interfere with namespace B |

---

## Done When

- [ ] `tracing-subscriber` initialized with env-filter and configurable format
- [ ] Prometheus metrics exporter serves `/metrics` endpoint
- [ ] All write path metrics defined and recording
- [ ] All read path metrics defined and recording
- [ ] Engine metrics (flush, compaction, L0 count, level sizes) tracked
- [ ] Cache hit/miss metrics tracked
- [ ] Periodic stats collector updates gauges from engine state
- [ ] All tests pass
