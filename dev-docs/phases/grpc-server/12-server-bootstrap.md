# Task 12: Server Bootstrap & Graceful Shutdown

**Crate:** `flushdb-server`
**File:** `src/server.rs`, `src/config.rs`
**Depends on:** Task 7 (NamespaceManager), Task 9 (Write Handlers), Task 10 (Read Handlers), Task 11 (Observability)
**Estimated complexity:** M
**Design reference:** Phase 7 §Future Work Considerations (Graceful Shutdown)

---

## Goal

Wire everything together into a runnable gRPC server. Handle configuration loading, namespace initialization, tonic server startup, signal handling, and multi-phase graceful shutdown. After this task, `cargo run -p flushdb-server` starts a working database server.

---

## What to Build

### 12.1 ServerConfig Struct

```
ServerConfig {
    // Network
    grpc_listen_addr:     SocketAddr,     // default 0.0.0.0:50051
    metrics_listen_addr:  SocketAddr,     // default 0.0.0.0:9090

    // Node identity
    node_id:              u16,            // default 0

    // Storage
    data_dir:             PathBuf,        // default ./data
    s3_bucket:            Option<String>, // None = use LocalFsBackend

    // Defaults for namespaces created without explicit config
    default_partition_count:    u32,      // default 4
    default_memtable_size_mb:   u64,      // default 64

    // Logging
    log_level:            String,         // default "info"
    log_format:           String,         // "json" or "pretty", default "json"

    // Maintenance
    maintenance_interval_ms:  u64,        // default 1000 (flush/compact check interval)
    stats_interval_ms:        u64,        // default 10000 (metrics collection interval)
}
```

**Derives:** `Clone, Debug, Serialize, Deserialize`

All fields use `#[serde(default)]` for optional configuration.

### 12.2 Configuration Loading

| Function | Signature | Behavior |
|----------|-----------|----------|
| `ServerConfig::from_env` | `() -> FlushResult<Self>` | Loads from env vars with `FLUSHDB_` prefix. E.g., `FLUSHDB_GRPC_LISTEN_ADDR`, `FLUSHDB_NODE_ID`, `FLUSHDB_DATA_DIR`, `FLUSHDB_S3_BUCKET`. Falls back to defaults. |
| `ServerConfig::from_file` | `(path: &Path) -> FlushResult<Self>` | Loads from JSON config file. Env vars override file values. |

### 12.3 FlushDbServer Struct

```
FlushDbServer {
    config:            ServerConfig,
    namespace_manager: Arc<NamespaceManager<B>>,
    version_generator: Arc<VersionGenerator>,
    shutdown_tx:       tokio::sync::watch::Sender<bool>,
    shutdown_rx:       tokio::sync::watch::Receiver<bool>,
}
```

### 12.4 Server Lifecycle

| Method | Signature | Behavior |
|--------|-----------|----------|
| `new` | `async (config: ServerConfig) -> FlushResult<Self>` | Creates storage backend (S3 or LocalFs based on config), VersionGenerator, NamespaceManager. Does NOT start listening. |
| `start` | `async (&self) -> FlushResult<()>` | Starts gRPC server, metrics exporter, maintenance loop, and stats collector. Blocks until shutdown signal. |
| `shutdown` | `async (&self) -> FlushResult<()>` | Triggers graceful shutdown sequence (see §12.5). |

### 12.5 Graceful Shutdown Sequence

Multi-phase shutdown triggered by SIGTERM or SIGINT:

**Phase 1 — Stop accepting writes:**
- Signal all partitions to freeze (Active → Frozen)
- New write requests receive `Status::unavailable("server shutting down")`
- Reads continue

**Phase 2 — Flush active memtables:**
- Call `namespace_manager.flush_all()`
- Wait for all in-flight flushes to complete
- Log progress: `"Flushed {N} partitions"`

**Phase 3 — Drain in-flight requests:**
- tonic graceful shutdown — stops accepting new connections, finishes in-flight RPCs
- Configurable drain timeout (default 30 seconds)

**Phase 4 — Stop partitions:**
- Call `namespace_manager.stop_all()`
- Engine.close() on each partition (finalizes WAL, releases resources)

**Phase 5 — Cleanup:**
- Stop metrics exporter
- Stop maintenance loop
- Stop stats collector
- Log: `"Server shutdown complete"`

### 12.6 Signal Handling

| Function | Signature | Behavior |
|----------|-----------|----------|
| `install_signal_handlers` | `(shutdown_tx: watch::Sender<bool>) -> JoinHandle<()>` | Listens for SIGTERM and SIGINT. On first signal, triggers graceful shutdown. On second signal within 5 seconds, forces immediate exit. |

### 12.7 Maintenance Loop

| Function | Signature | Behavior |
|----------|-----------|----------|
| `spawn_maintenance_loop` | `(namespace_manager: Arc<NamespaceManager<B>>, interval: Duration, shutdown_rx: watch::Receiver<bool>) -> JoinHandle<()>` | Periodically calls `namespace_manager.run_maintenance()` (flush + compact check). Stops on shutdown signal. |

### 12.8 main() Function

```
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::from_env()?;
    init_tracing(&config.log_level)?;
    let server = FlushDbServer::new(config).await?;
    server.start().await?;
    Ok(())
}
```

### 12.9 Backend Selection

Based on `config.s3_bucket`:
- **Some(bucket)** → Create `S3StorageBackend::from_env(bucket)?`
- **None** → Create `LocalFsBackend::new(config.data_dir.join("storage"))`

This allows local development with filesystem storage and production deployment with S3.

---

## Tests

**File:** `crates/flushdb-server/tests/server_tests.rs`

### Config Tests
| Test | What It Validates |
|------|-------------------|
| `test_config_defaults` | Default config has expected values (port 50051, node_id 0, etc.) |
| `test_config_from_env` | Env vars override defaults correctly |
| `test_config_from_file` | JSON config file loads correctly |
| `test_config_env_overrides_file` | Env vars take precedence over file values |

### Server Lifecycle Tests
| Test | What It Validates |
|------|-------------------|
| `test_server_starts_and_accepts_connections` | Server starts on configured port, accepts gRPC connection |
| `test_server_graceful_shutdown` | Shutdown completes all phases without error |
| `test_server_shutdown_flushes_data` | Data written before shutdown is flushed (not lost) |
| `test_server_double_signal_forces_exit` | Second SIGTERM within 5 seconds triggers forced exit |

### Maintenance Loop Tests
| Test | What It Validates |
|------|-------------------|
| `test_maintenance_loop_runs_periodically` | Flush/compact checks happen at configured interval |
| `test_maintenance_loop_stops_on_shutdown` | Loop terminates when shutdown signal sent |

### Backend Selection Tests
| Test | What It Validates |
|------|-------------------|
| `test_local_backend_when_no_s3` | No S3 bucket → LocalFsBackend used |
| `test_s3_backend_when_bucket_set` | S3 bucket configured → S3StorageBackend used |

---

## Done When

- [ ] Server starts and accepts gRPC connections on configured address
- [ ] Config loadable from env vars and/or JSON file
- [ ] Graceful shutdown follows all 5 phases in order
- [ ] SIGTERM/SIGINT triggers graceful shutdown
- [ ] Second signal within 5 seconds forces immediate exit
- [ ] Maintenance loop runs flush/compact checks periodically
- [ ] Stats collector updates Prometheus gauges periodically
- [ ] Backend selection works (S3 when bucket set, LocalFs otherwise)
- [ ] All tests pass
