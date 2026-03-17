use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::BenchConfig;
use crate::cassandra_client::CassandraClient;
use crate::client::DemoClient;
use crate::data_gen::ProductGenerator;
use crate::report;
use crate::stats::BenchStats;

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

pub async fn run(
    config: &BenchConfig,
    cassandra_addr: &str,
    seed_products: u32,
    warmup_secs: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let total_ratio =
        config.write_ratio + config.update_ratio + config.delete_ratio + config.scan_ratio;
    if total_ratio > 1.0 {
        return Err(format!(
            "operation ratios sum to {:.2} (must be <= 1.0)",
            total_ratio,
        )
        .into());
    }

    let read_ratio = 1.0 - total_ratio;

    // --- Config banner ---
    println!("=== Benchmark Configuration ===");
    println!("Duration:       {}s measurement + {}s warmup per backend", config.duration_secs, warmup_secs);
    println!("Concurrency:    {} workers", config.concurrency);
    println!("Product range:  {}", config.product_range);
    println!("Seed products:  {}", seed_products);
    println!("Resource limit: 2 CPUs, 1024 MB per database (set in docker-compose.yml)");
    println!("  flushdb:      single node (2 CPU / 1 GB) + minio (1 CPU / 512 MB)");
    println!("  cassandra:    single node (2 CPU / 1 GB, 512 MB heap, 1 token, LCS, no hints)");
    println!(
        "Op mix:         read={:.0}% write={:.0}% update={:.0}% delete={:.0}% scan={:.0}%",
        read_ratio * 100.0,
        config.write_ratio * 100.0,
        config.update_ratio * 100.0,
        config.delete_ratio * 100.0,
        config.scan_ratio * 100.0,
    );
    println!();

    // --- Seed phase ---
    println!("Seeding {} products to both backends...", seed_products);
    let t = Instant::now();
    seed_flushdb(&config.server_addr, &config.namespace, seed_products).await?;
    let flushdb_seed_secs = t.elapsed().as_secs_f64();

    let t = Instant::now();
    let cass_keyspace = format!("{}_bench", config.namespace);
    seed_cassandra(cassandra_addr, &cass_keyspace, seed_products).await?;
    let cass_seed_secs = t.elapsed().as_secs_f64();

    println!("  flushdb:    {:.1}s", flushdb_seed_secs);
    println!("  cassandra:  {:.1}s", cass_seed_secs);

    // --- Warmup phase (results discarded) ---
    if warmup_secs > 0 {
        println!("\nWarmup {}s per backend (JVM warm-up, connection pools, page cache)...", warmup_secs);
        let warmup_cfg = BenchConfig {
            duration_secs: warmup_secs,
            ..config.clone()
        };
        print!("  flushdb...  ");
        bench_flushdb(&warmup_cfg).await?;
        println!("done");
        print!("  cassandra...");
        bench_cassandra(&warmup_cfg, cassandra_addr, &cass_keyspace).await?;
        println!("done");
    }

    // --- Measured phase ---
    println!(
        "\nMeasuring for {}s with {} workers per backend...",
        config.duration_secs, config.concurrency
    );
    print!("  flushdb...  ");
    let flushdb_result = bench_flushdb(config).await?;
    println!("done");
    print!("  cassandra...");
    let cassandra_result = bench_cassandra(config, cassandra_addr, &cass_keyspace).await?;
    println!("done");

    // --- Results ---
    print_results("flushdb", &flushdb_result);
    print_results("Cassandra", &cassandra_result);
    print_comparison(&flushdb_result, &cassandra_result);

    Ok(())
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

                match pick_op(&mut rng, write_ratio, update_ratio, delete_ratio, scan_ratio) {
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

                match pick_op(&mut rng, write_ratio, update_ratio, delete_ratio, scan_ratio) {
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

fn print_results(label: &str, result: &BackendResult) {
    report::print_header(&format!("{} Results", label));
    report::print_stats_row("read", &result.read.snapshot());
    report::print_stats_row("write", &result.write.snapshot());
    report::print_stats_row("update", &result.update.snapshot());
    report::print_stats_row("delete", &result.delete.snapshot());
    report::print_stats_row("scan", &result.scan.snapshot());
}

fn print_comparison(flushdb: &BackendResult, cassandra: &BackendResult) {
    println!();
    println!("=== Comparison (flushdb vs Cassandra) ===");
    println!(
        "{:<12} {:>14} {:>14} {:>10}",
        "Metric", "flushdb p50", "cassandra p50", "speedup"
    );
    println!("{}", "-".repeat(54));

    let pairs: &[(&str, &BenchStats, &BenchStats)] = &[
        ("read", &flushdb.read, &cassandra.read),
        ("write", &flushdb.write, &cassandra.write),
        ("update", &flushdb.update, &cassandra.update),
        ("delete", &flushdb.delete, &cassandra.delete),
        ("scan", &flushdb.scan, &cassandra.scan),
    ];

    for (label, f, c) in pairs {
        let f_snap = f.snapshot();
        let c_snap = c.snapshot();
        if f_snap.total_ops == 0 && c_snap.total_ops == 0 {
            continue;
        }
        let speedup = if f_snap.p50_us > 0 {
            c_snap.p50_us as f64 / f_snap.p50_us as f64
        } else {
            0.0
        };
        println!(
            "{:<12} {:>12}us {:>12}us {:>9.1}x",
            label, f_snap.p50_us, c_snap.p50_us, speedup
        );
    }

    // Throughput comparison
    println!();
    println!(
        "{:<12} {:>14} {:>14} {:>10}",
        "", "flushdb ops/s", "cass ops/s", "ratio"
    );
    println!("{}", "-".repeat(54));
    for (label, f, c) in pairs {
        let f_snap = f.snapshot();
        let c_snap = c.snapshot();
        if f_snap.total_ops == 0 && c_snap.total_ops == 0 {
            continue;
        }
        let ratio = if c_snap.ops_per_sec > 0.0 {
            f_snap.ops_per_sec / c_snap.ops_per_sec
        } else {
            0.0
        };
        println!(
            "{:<12} {:>14.1} {:>14.1} {:>9.1}x",
            label, f_snap.ops_per_sec, c_snap.ops_per_sec, ratio
        );
    }
}
