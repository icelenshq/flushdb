use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::cassandra_client::CassandraClient;
use crate::client::DemoClient;
use crate::data_gen::ProductGenerator;

pub struct IngestConfig {
    pub server_addr: String,
    pub namespace: String,
    pub cassandra_addr: String,
    pub total_products: u64,
    pub concurrency: u32,
    pub report_interval: u64,
}

struct Milestone {
    count: u64,
    elapsed_secs: f64,
    cumulative_rate: f64,
}

struct IngestResult {
    total: u64,
    elapsed: Duration,
    milestones: Vec<Milestone>,
}

fn format_count(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub async fn run(config: &IngestConfig) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("=== Ingestion Benchmark ===");
    println!("Total products:  {}", format_count(config.total_products));
    println!("Concurrency:     {} workers", config.concurrency);
    println!(
        "Report every:    {} products",
        format_count(config.report_interval)
    );
    println!("Resource limit:  2 CPUs, 1024 MB per database (docker-compose.yml)");
    println!();

    // --- flushdb ingestion ---
    println!(
        "Ingesting {} products into flushdb...",
        format_count(config.total_products)
    );
    let flushdb_result = ingest_flushdb(config).await?;
    println!(
        "  Done: {} products in {:.1}s ({} products/sec)\n",
        format_count(flushdb_result.total),
        flushdb_result.elapsed.as_secs_f64(),
        format_count((flushdb_result.total as f64 / flushdb_result.elapsed.as_secs_f64()) as u64),
    );

    // --- Cassandra ingestion ---
    println!(
        "Ingesting {} products into Cassandra...",
        format_count(config.total_products)
    );
    let cass_keyspace = format!("{}_ingest", config.namespace);
    let cass_result = ingest_cassandra(config, &cass_keyspace).await?;
    println!(
        "  Done: {} products in {:.1}s ({} products/sec)\n",
        format_count(cass_result.total),
        cass_result.elapsed.as_secs_f64(),
        format_count((cass_result.total as f64 / cass_result.elapsed.as_secs_f64()) as u64),
    );

    // --- Side-by-side milestone comparison ---
    print_milestone_comparison(&flushdb_result, &cass_result, config.report_interval);

    // --- Summary ---
    print_summary(&flushdb_result, &cass_result);

    Ok(())
}

async fn ingest_flushdb(
    config: &IngestConfig,
) -> Result<IngestResult, Box<dyn std::error::Error + Send + Sync>> {
    let counter = Arc::new(AtomicU64::new(0));
    let total = config.total_products;
    let chunk_size = total / config.concurrency as u64;
    let report_interval = config.report_interval;
    let start = Instant::now();

    // Background progress reporter that collects milestones
    let progress_counter = counter.clone();
    let reporter = tokio::spawn(async move {
        let mut milestones = Vec::new();
        let mut last_milestone: u64 = 0;
        let mut last_milestone_time = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let count = progress_counter.load(Ordering::Relaxed);
            let current_milestone = (count / report_interval) * report_interval;

            if current_milestone > last_milestone {
                let now = Instant::now();
                let elapsed = now.duration_since(start);
                let interval_elapsed = now.duration_since(last_milestone_time).as_secs_f64();
                let interval_products = current_milestone - last_milestone;
                let interval_rate = if interval_elapsed > 0.0 {
                    interval_products as f64 / interval_elapsed
                } else {
                    0.0
                };
                let cumulative_rate = current_milestone as f64 / elapsed.as_secs_f64();
                let pct = current_milestone as f64 / total as f64 * 100.0;

                println!(
                    "  [{:>5.1}%] {:>14} / {:>14}  (interval: {:>8}/s  cumulative: {:>8}/s)",
                    pct,
                    format_count(current_milestone),
                    format_count(total),
                    format_count(interval_rate as u64),
                    format_count(cumulative_rate as u64),
                );

                milestones.push(Milestone {
                    count: current_milestone,
                    elapsed_secs: elapsed.as_secs_f64(),
                    cumulative_rate,
                });

                last_milestone = current_milestone;
                last_milestone_time = now;
            }

            if count >= total {
                break;
            }
        }
        milestones
    });

    // Spawn ingestion workers
    let mut handles = Vec::new();
    for task_id in 0..config.concurrency {
        let addr = config.server_addr.clone();
        let ns = config.namespace.clone();
        let counter = counter.clone();
        let start_id = task_id as u64 * chunk_size;
        let end_id = if task_id == config.concurrency - 1 {
            total
        } else {
            start_id + chunk_size
        };

        handles.push(tokio::spawn(async move {
            let mut client = DemoClient::connect(&addr).await.expect("connect flushdb");
            let gen = ProductGenerator::new(42);
            for id in start_id..end_id {
                let product_id = id as u32;
                let record_id = ProductGenerator::record_id(product_id);
                let items = gen.generate_product(product_id);
                match client.put_product(&ns, &record_id, items).await {
                    Ok(_) => {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => tracing::warn!(id, error = %e, "flushdb ingest error"),
                }
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }
    let elapsed = start.elapsed();
    let milestones = reporter.await?;

    Ok(IngestResult {
        total: counter.load(Ordering::Relaxed),
        elapsed,
        milestones,
    })
}

async fn ingest_cassandra(
    config: &IngestConfig,
    keyspace: &str,
) -> Result<IngestResult, Box<dyn std::error::Error + Send + Sync>> {
    let client = CassandraClient::connect(&config.cassandra_addr, keyspace).await?;
    let counter = Arc::new(AtomicU64::new(0));
    let total = config.total_products;
    let chunk_size = total / config.concurrency as u64;
    let report_interval = config.report_interval;
    let start = Instant::now();

    // Background progress reporter
    let progress_counter = counter.clone();
    let reporter = tokio::spawn(async move {
        let mut milestones = Vec::new();
        let mut last_milestone: u64 = 0;
        let mut last_milestone_time = Instant::now();

        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let count = progress_counter.load(Ordering::Relaxed);
            let current_milestone = (count / report_interval) * report_interval;

            if current_milestone > last_milestone {
                let now = Instant::now();
                let elapsed = now.duration_since(start);
                let interval_elapsed = now.duration_since(last_milestone_time).as_secs_f64();
                let interval_products = current_milestone - last_milestone;
                let interval_rate = if interval_elapsed > 0.0 {
                    interval_products as f64 / interval_elapsed
                } else {
                    0.0
                };
                let cumulative_rate = current_milestone as f64 / elapsed.as_secs_f64();
                let pct = current_milestone as f64 / total as f64 * 100.0;

                println!(
                    "  [{:>5.1}%] {:>14} / {:>14}  (interval: {:>8}/s  cumulative: {:>8}/s)",
                    pct,
                    format_count(current_milestone),
                    format_count(total),
                    format_count(interval_rate as u64),
                    format_count(cumulative_rate as u64),
                );

                milestones.push(Milestone {
                    count: current_milestone,
                    elapsed_secs: elapsed.as_secs_f64(),
                    cumulative_rate,
                });

                last_milestone = current_milestone;
                last_milestone_time = now;
            }

            if count >= total {
                break;
            }
        }
        milestones
    });

    // Spawn ingestion workers
    let mut handles = Vec::new();
    for task_id in 0..config.concurrency {
        let client = client.clone();
        let counter = counter.clone();
        let start_id = task_id as u64 * chunk_size;
        let end_id = if task_id == config.concurrency - 1 {
            total
        } else {
            start_id + chunk_size
        };

        handles.push(tokio::spawn(async move {
            let gen = ProductGenerator::new(42);
            for id in start_id..end_id {
                let product_id = id as u32;
                let record_id = ProductGenerator::record_id(product_id);
                let items = gen.generate_product(product_id);
                match client.put_product(&record_id, items).await {
                    Ok(_) => {
                        counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => tracing::warn!(id, error = %e, "cassandra ingest error"),
                }
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }
    let elapsed = start.elapsed();
    let milestones = reporter.await?;

    Ok(IngestResult {
        total: counter.load(Ordering::Relaxed),
        elapsed,
        milestones,
    })
}

fn print_milestone_comparison(
    flushdb: &IngestResult,
    cassandra: &IngestResult,
    _report_interval: u64,
) {
    println!("=== Throughput at Each Milestone (products/sec) ===");
    println!(
        "{:>14}  {:>10}  {:>12}  {:>10}  {:>12}  {:>8}",
        "milestone", "flushdb s", "flushdb/s", "cass s", "cass/s", "speedup"
    );
    println!("{}", "-".repeat(76));

    // Match milestones by count
    let mut f_idx = 0;
    let mut c_idx = 0;
    let f_milestones = &flushdb.milestones;
    let c_milestones = &cassandra.milestones;

    loop {
        let f_count = f_milestones.get(f_idx).map(|m| m.count);
        let c_count = c_milestones.get(c_idx).map(|m| m.count);

        match (f_count, c_count) {
            (Some(fc), Some(cc)) if fc == cc => {
                let fm = &f_milestones[f_idx];
                let cm = &c_milestones[c_idx];
                let speedup = if cm.cumulative_rate > 0.0 {
                    fm.cumulative_rate / cm.cumulative_rate
                } else {
                    0.0
                };
                println!(
                    "{:>14}  {:>9.1}s  {:>10}/s  {:>9.1}s  {:>10}/s  {:>7.1}x",
                    format_count(fc),
                    fm.elapsed_secs,
                    format_count(fm.cumulative_rate as u64),
                    cm.elapsed_secs,
                    format_count(cm.cumulative_rate as u64),
                    speedup,
                );
                f_idx += 1;
                c_idx += 1;
            }
            (Some(fc), Some(cc)) if fc < cc => {
                f_idx += 1;
            }
            (Some(_), Some(_)) => {
                c_idx += 1;
            }
            _ => break,
        }
    }
    println!();
}

fn print_summary(flushdb: &IngestResult, cassandra: &IngestResult) {
    let f_rate = flushdb.total as f64 / flushdb.elapsed.as_secs_f64();
    let c_rate = cassandra.total as f64 / cassandra.elapsed.as_secs_f64();
    let time_speedup = cassandra.elapsed.as_secs_f64() / flushdb.elapsed.as_secs_f64();
    let rate_speedup = f_rate / c_rate;

    println!("=== Ingestion Summary ===");
    println!(
        "{:<16} {:>14} {:>14} {:>10}",
        "", "flushdb", "cassandra", "speedup"
    );
    println!("{}", "-".repeat(58));
    println!(
        "{:<16} {:>14} {:>14} {:>9.1}x",
        "Products",
        format_count(flushdb.total),
        format_count(cassandra.total),
        1.0,
    );
    println!(
        "{:<16} {:>13.1}s {:>13.1}s {:>9.1}x",
        "Total time",
        flushdb.elapsed.as_secs_f64(),
        cassandra.elapsed.as_secs_f64(),
        time_speedup,
    );
    println!(
        "{:<16} {:>12}/s {:>12}/s {:>9.1}x",
        "Throughput",
        format_count(f_rate as u64),
        format_count(c_rate as u64),
        rate_speedup,
    );
}
