use std::time::{Duration, Instant};

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use super::BenchConfig;
use crate::cassandra_client::CassandraClient;
use crate::client::DemoClient;
use crate::data_gen::ProductGenerator;
use crate::report;
use crate::stats::{BenchStats, StatsSnapshot};

enum Op {
    Read,
    Write,
    Update,
    Delete,
    Scan,
}

fn pick_op(
    rng: &mut impl Rng,
    write_ratio: f64,
    update_ratio: f64,
    delete_ratio: f64,
    scan_ratio: f64,
) -> Op {
    let roll: f64 = rng.random();
    let mut threshold = write_ratio;
    if roll < threshold {
        return Op::Write;
    }
    threshold += update_ratio;
    if roll < threshold {
        return Op::Update;
    }
    threshold += delete_ratio;
    if roll < threshold {
        return Op::Delete;
    }
    threshold += scan_ratio;
    if roll < threshold {
        return Op::Scan;
    }
    Op::Read
}

type StatsBundle = (BenchStats, BenchStats, BenchStats, BenchStats, BenchStats);

struct BackendResult {
    read: BenchStats,
    write: BenchStats,
    update: BenchStats,
    delete: BenchStats,
    scan: BenchStats,
}

struct BackendSnapshots {
    read: StatsSnapshot,
    write: StatsSnapshot,
    update: StatsSnapshot,
    delete: StatsSnapshot,
    scan: StatsSnapshot,
}

impl BackendResult {
    fn snapshots(&self) -> BackendSnapshots {
        BackendSnapshots {
            read: self.read.snapshot(),
            write: self.write.snapshot(),
            update: self.update.snapshot(),
            delete: self.delete.snapshot(),
            scan: self.scan.snapshot(),
        }
    }
}

/// Serializable form of a single operation's stats, written to disk after the flushdb phase
/// and read back during the cassandra phase to produce a fair cross-run comparison.
#[derive(Serialize, Deserialize)]
struct SavedSnapshot {
    total_ops: u64,
    elapsed_us: u64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
    ops_per_sec: f64,
}

#[derive(Serialize, Deserialize)]
struct SavedBackendResult {
    read: SavedSnapshot,
    write: SavedSnapshot,
    update: SavedSnapshot,
    delete: SavedSnapshot,
    scan: SavedSnapshot,
}

impl From<&StatsSnapshot> for SavedSnapshot {
    fn from(s: &StatsSnapshot) -> Self {
        SavedSnapshot {
            total_ops: s.total_ops,
            elapsed_us: s.elapsed.as_micros() as u64,
            p50_us: s.p50_us,
            p95_us: s.p95_us,
            p99_us: s.p99_us,
            max_us: s.max_us,
            ops_per_sec: s.ops_per_sec,
        }
    }
}

impl From<SavedSnapshot> for StatsSnapshot {
    fn from(s: SavedSnapshot) -> Self {
        StatsSnapshot {
            total_ops: s.total_ops,
            elapsed: Duration::from_micros(s.elapsed_us),
            p50_us: s.p50_us,
            p95_us: s.p95_us,
            p99_us: s.p99_us,
            max_us: s.max_us,
            ops_per_sec: s.ops_per_sec,
        }
    }
}

impl From<SavedBackendResult> for BackendSnapshots {
    fn from(s: SavedBackendResult) -> Self {
        BackendSnapshots {
            read: s.read.into(),
            write: s.write.into(),
            update: s.update.into(),
            delete: s.delete.into(),
            scan: s.scan.into(),
        }
    }
}

fn to_saved(result: &BackendResult) -> SavedBackendResult {
    let snaps = result.snapshots();
    SavedBackendResult {
        read: SavedSnapshot::from(&snaps.read),
        write: SavedSnapshot::from(&snaps.write),
        update: SavedSnapshot::from(&snaps.update),
        delete: SavedSnapshot::from(&snaps.delete),
        scan: SavedSnapshot::from(&snaps.scan),
    }
}

/// Phase 1: benchmark flushdb only. Saves results to `output_path` for use in phase 2.
/// Run with only minio + flushdb-server running (`docker compose --profile flushdb up`).
pub async fn run_flushdb_phase(
    config: &BenchConfig,
    output_path: &str,
    seed_products: u32,
    warmup_secs: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    validate_ratios(config)?;
    let read_ratio =
        1.0 - (config.write_ratio + config.update_ratio + config.delete_ratio + config.scan_ratio);

    print_config_banner(config, read_ratio, warmup_secs);
    println!("Phase: flushdb  |  output: {}", output_path);
    println!();

    println!("Seeding {} products to flushdb...", seed_products);
    let t = Instant::now();
    seed_flushdb(&config.server_addr, &config.namespace, seed_products).await?;
    println!("  done in {:.1}s", t.elapsed().as_secs_f64());

    if warmup_secs > 0 {
        println!(
            "\nWarmup {}s (page cache, connection pools)...",
            warmup_secs
        );
        let warmup_cfg = BenchConfig {
            duration_secs: warmup_secs,
            ..config.clone()
        };
        print!("  flushdb... ");
        bench_flushdb(&warmup_cfg).await?;
        println!("done");
    }

    println!(
        "\nMeasuring for {}s with {} workers...",
        config.duration_secs, config.concurrency
    );
    print!("  flushdb... ");
    let result = bench_flushdb(config).await?;
    println!("done");

    print_results("flushdb", &result.snapshots());

    let saved = to_saved(&result);
    let json = serde_json::to_string_pretty(&saved)?;
    std::fs::write(output_path, &json)?;
    println!("\nResults saved to {}", output_path);

    Ok(())
}

/// Phase 2: benchmark cassandra only. Loads flushdb results from `flushdb_results_path`
/// and prints a side-by-side comparison.
/// Run with only cassandra running (`docker compose --profile cassandra up`).
pub async fn run_cassandra_phase(
    config: &BenchConfig,
    cassandra_addr: &str,
    flushdb_results_path: &str,
    seed_products: u32,
    warmup_secs: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    validate_ratios(config)?;
    let read_ratio =
        1.0 - (config.write_ratio + config.update_ratio + config.delete_ratio + config.scan_ratio);

    let json = std::fs::read_to_string(flushdb_results_path)
        .map_err(|e| format!("cannot read {}: {}", flushdb_results_path, e))?;
    let saved: SavedBackendResult = serde_json::from_str(&json)
        .map_err(|e| format!("cannot parse {}: {}", flushdb_results_path, e))?;
    let flushdb_snaps: BackendSnapshots = saved.into();

    print_config_banner(config, read_ratio, warmup_secs);
    println!(
        "Phase: cassandra  |  flushdb results: {}",
        flushdb_results_path
    );
    println!();

    let cass_keyspace = format!("{}_bench", config.namespace);

    println!("Seeding {} products to Cassandra...", seed_products);
    let t = Instant::now();
    seed_cassandra(cassandra_addr, &cass_keyspace, seed_products).await?;
    println!("  done in {:.1}s", t.elapsed().as_secs_f64());

    if warmup_secs > 0 {
        println!(
            "\nWarmup {}s (JVM warm-up, connection pools)...",
            warmup_secs
        );
        let warmup_cfg = BenchConfig {
            duration_secs: warmup_secs,
            ..config.clone()
        };
        print!("  cassandra... ");
        bench_cassandra(&warmup_cfg, cassandra_addr, &cass_keyspace).await?;
        println!("done");
    }

    println!(
        "\nMeasuring for {}s with {} workers...",
        config.duration_secs, config.concurrency
    );
    print!("  cassandra... ");
    let cassandra_result = bench_cassandra(config, cassandra_addr, &cass_keyspace).await?;
    println!("done");

    print_results("Cassandra", &cassandra_result.snapshots());
    print_comparison(&flushdb_snaps, &cassandra_result.snapshots());

    Ok(())
}

fn validate_ratios(config: &BenchConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let total = config.write_ratio + config.update_ratio + config.delete_ratio + config.scan_ratio;
    if total > 1.0 {
        return Err(format!("operation ratios sum to {:.2} (must be <= 1.0)", total).into());
    }
    Ok(())
}

fn print_config_banner(config: &BenchConfig, read_ratio: f64, warmup_secs: u64) {
    println!("=== Benchmark Configuration ===");
    println!(
        "Duration:       {}s measurement + {}s warmup",
        config.duration_secs, warmup_secs
    );
    println!("Concurrency:    {} workers", config.concurrency);
    println!("Product range:  {}", config.product_range);
    println!("Environment:    benchmark client + both backends run inside Docker (docker compose)");
    println!("Resource limit: 2 CPUs / 1 GB each");
    println!(
        "  flushdb:      flushdb-server (2 CPU / 1 GB) + MinIO (1 CPU / 512 MB) — profile: flushdb",
    );
    println!(
        "  cassandra:    single node (512 MB heap, 1 token, LCS, no hints, 2 CPU / 1 GB) — profile: cassandra",
    );
    println!("  client:       flushdb-demo container, run sequentially with --no-deps");
    println!(
        "Op mix:         read={:.0}% write={:.0}% update={:.0}% delete={:.0}% scan={:.0}%",
        read_ratio * 100.0,
        config.write_ratio * 100.0,
        config.update_ratio * 100.0,
        config.delete_ratio * 100.0,
        config.scan_ratio * 100.0,
    );
}

fn print_results(label: &str, snaps: &BackendSnapshots) {
    report::print_header(&format!("{} Results", label));
    report::print_stats_row("read", &snaps.read);
    report::print_stats_row("write", &snaps.write);
    report::print_stats_row("update", &snaps.update);
    report::print_stats_row("delete", &snaps.delete);
    report::print_stats_row("scan", &snaps.scan);
}

fn print_comparison(flushdb: &BackendSnapshots, cassandra: &BackendSnapshots) {
    println!();
    println!("=== Comparison (flushdb vs Cassandra) ===");
    println!(
        "{:<12} {:>14} {:>14} {:>10}",
        "Metric", "flushdb p50", "cassandra p50", "speedup"
    );
    println!("{}", "-".repeat(54));

    let pairs: &[(&str, &StatsSnapshot, &StatsSnapshot)] = &[
        ("read", &flushdb.read, &cassandra.read),
        ("write", &flushdb.write, &cassandra.write),
        ("update", &flushdb.update, &cassandra.update),
        ("delete", &flushdb.delete, &cassandra.delete),
        ("scan", &flushdb.scan, &cassandra.scan),
    ];

    for (label, f, c) in pairs {
        if f.total_ops == 0 && c.total_ops == 0 {
            continue;
        }
        let speedup = if f.p50_us > 0 {
            c.p50_us as f64 / f.p50_us as f64
        } else {
            0.0
        };
        println!(
            "{:<12} {:>12}us {:>12}us {:>9.1}x",
            label, f.p50_us, c.p50_us, speedup
        );
    }

    println!();
    println!(
        "{:<12} {:>14} {:>14} {:>10}",
        "", "flushdb ops/s", "cass ops/s", "ratio"
    );
    println!("{}", "-".repeat(54));
    for (label, f, c) in pairs {
        if f.total_ops == 0 && c.total_ops == 0 {
            continue;
        }
        let ratio = if c.ops_per_sec > 0.0 {
            f.ops_per_sec / c.ops_per_sec
        } else {
            0.0
        };
        println!(
            "{:<12} {:>14.1} {:>14.1} {:>9.1}x",
            label, f.ops_per_sec, c.ops_per_sec, ratio
        );
    }
}

async fn seed_flushdb(
    addr: &str,
    namespace: &str,
    count: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = DemoClient::connect(addr).await?;
    let gen = ProductGenerator::new(42);
    for id in 0..count {
        let record_id = ProductGenerator::record_id(id);
        let items = gen.generate_product(id);
        client.put_product(namespace, &record_id, items).await?;
    }
    Ok(())
}

async fn seed_cassandra(
    addr: &str,
    keyspace: &str,
    count: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = CassandraClient::connect(addr, keyspace).await?;
    let gen = ProductGenerator::new(42);
    for id in 0..count {
        let record_id = ProductGenerator::record_id(id);
        let items = gen.generate_product(id);
        client.put_product(&record_id, items).await?;
    }
    Ok(())
}

async fn bench_flushdb(
    config: &BenchConfig,
) -> Result<BackendResult, Box<dyn std::error::Error + Send + Sync>> {
    let deadline = Instant::now() + std::time::Duration::from_secs(config.duration_secs);
    let write_ratio = config.write_ratio;
    let update_ratio = config.update_ratio;
    let delete_ratio = config.delete_ratio;
    let scan_ratio = config.scan_ratio;
    let mut handles = Vec::new();

    for task_id in 0..config.concurrency {
        let addr = config.server_addr.clone();
        let ns = config.namespace.clone();
        let product_range = config.product_range;

        handles.push(tokio::spawn(async move {
            let mut client = DemoClient::connect(&addr).await.expect("connect flushdb");
            let gen = ProductGenerator::new(42);
            let mut rng = StdRng::seed_from_u64(5000 + task_id as u64);
            let mut read = BenchStats::new();
            let mut write = BenchStats::new();
            let mut update = BenchStats::new();
            let mut delete = BenchStats::new();
            let mut scan = BenchStats::new();

            while Instant::now() < deadline {
                let product_id = rng.random_range(0..product_range);
                let record_id = ProductGenerator::record_id(product_id);

                match pick_op(
                    &mut rng,
                    write_ratio,
                    update_ratio,
                    delete_ratio,
                    scan_ratio,
                ) {
                    Op::Write => {
                        let items = gen.generate_product(product_id);
                        let t = Instant::now();
                        match client.put_product(&ns, &record_id, items).await {
                            Ok(_) => write.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "flushdb write error"),
                        }
                    }
                    Op::Update => {
                        let items = ProductGenerator::generate_update_items(&mut rng);
                        let t = Instant::now();
                        match client.put_product(&ns, &record_id, items).await {
                            Ok(_) => update.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "flushdb update error"),
                        }
                    }
                    Op::Delete => {
                        let t = Instant::now();
                        match client.delete_product(&ns, &record_id).await {
                            Ok(_) => delete.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "flushdb delete error"),
                        }
                    }
                    Op::Scan => {
                        let t = Instant::now();
                        match client
                            .scan_product_range(
                                &ns,
                                &record_id,
                                b"variant:".to_vec(),
                                b"variant:\xff".to_vec(),
                            )
                            .await
                        {
                            Ok(_) => scan.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "flushdb scan error"),
                        }
                    }
                    Op::Read => {
                        let keys = vec![b"info".to_vec(), b"price".to_vec()];
                        let t = Instant::now();
                        match client.get_product_keys(&ns, &record_id, keys).await {
                            Ok(_) => read.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "flushdb read error"),
                        }
                    }
                }
            }
            (read, write, update, delete, scan)
        }));
    }

    merge_handles(handles).await
}

async fn bench_cassandra(
    config: &BenchConfig,
    cassandra_addr: &str,
    keyspace: &str,
) -> Result<BackendResult, Box<dyn std::error::Error + Send + Sync>> {
    let client = CassandraClient::connect(cassandra_addr, keyspace).await?;
    let deadline = Instant::now() + std::time::Duration::from_secs(config.duration_secs);
    let write_ratio = config.write_ratio;
    let update_ratio = config.update_ratio;
    let delete_ratio = config.delete_ratio;
    let scan_ratio = config.scan_ratio;
    let mut handles = Vec::new();

    for task_id in 0..config.concurrency {
        let client = client.clone();
        let product_range = config.product_range;

        handles.push(tokio::spawn(async move {
            let gen = ProductGenerator::new(42);
            let mut rng = StdRng::seed_from_u64(5000 + task_id as u64);
            let mut read = BenchStats::new();
            let mut write = BenchStats::new();
            let mut update = BenchStats::new();
            let mut delete = BenchStats::new();
            let mut scan = BenchStats::new();

            while Instant::now() < deadline {
                let product_id = rng.random_range(0..product_range);
                let record_id = ProductGenerator::record_id(product_id);

                match pick_op(
                    &mut rng,
                    write_ratio,
                    update_ratio,
                    delete_ratio,
                    scan_ratio,
                ) {
                    Op::Write => {
                        let items = gen.generate_product(product_id);
                        let t = Instant::now();
                        match client.put_product(&record_id, items).await {
                            Ok(_) => write.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "cassandra write error"),
                        }
                    }
                    Op::Update => {
                        let items = ProductGenerator::generate_update_items(&mut rng);
                        let t = Instant::now();
                        match client.put_product(&record_id, items).await {
                            Ok(_) => update.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "cassandra update error"),
                        }
                    }
                    Op::Delete => {
                        let t = Instant::now();
                        match client.delete_product(&record_id).await {
                            Ok(_) => delete.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "cassandra delete error"),
                        }
                    }
                    Op::Scan => {
                        let t = Instant::now();
                        match client
                            .scan_product_range(
                                &record_id,
                                b"variant:".to_vec(),
                                b"variant:\xff".to_vec(),
                            )
                            .await
                        {
                            Ok(_) => scan.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "cassandra scan error"),
                        }
                    }
                    Op::Read => {
                        let keys = vec![b"info".to_vec(), b"price".to_vec()];
                        let t = Instant::now();
                        match client.get_product_keys(&record_id, keys).await {
                            Ok(_) => read.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "cassandra read error"),
                        }
                    }
                }
            }
            (read, write, update, delete, scan)
        }));
    }

    merge_handles(handles).await
}

async fn merge_handles(
    handles: Vec<tokio::task::JoinHandle<StatsBundle>>,
) -> Result<BackendResult, Box<dyn std::error::Error + Send + Sync>> {
    let mut result = BackendResult {
        read: BenchStats::new(),
        write: BenchStats::new(),
        update: BenchStats::new(),
        delete: BenchStats::new(),
        scan: BenchStats::new(),
    };
    for handle in handles {
        let (r, w, u, d, s) = handle.await?;
        result.read.merge(&r);
        result.write.merge(&w);
        result.update.merge(&u);
        result.delete.merge(&d);
        result.scan.merge(&s);
    }
    Ok(result)
}
