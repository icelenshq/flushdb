use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::BenchConfig;
use crate::client::DemoClient;
use crate::data_gen::ProductGenerator;
use crate::report;
use crate::stats::BenchStats;

pub struct MixedResult {
    pub read: BenchStats,
    pub write: BenchStats,
    pub update: BenchStats,
    pub delete: BenchStats,
    pub scan: BenchStats,
}

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

pub async fn run(config: &BenchConfig) -> Result<MixedResult, Box<dyn std::error::Error>> {
    let total_ratio =
        config.write_ratio + config.update_ratio + config.delete_ratio + config.scan_ratio;
    if total_ratio > 1.0 {
        return Err(format!(
            "operation ratios sum to {:.2} (must be <= 1.0): write={}, update={}, delete={}, scan={}",
            total_ratio, config.write_ratio, config.update_ratio, config.delete_ratio, config.scan_ratio,
        )
        .into());
    }

    let read_ratio = 1.0 - total_ratio;
    println!(
        "Op mix: read={:.0}% write={:.0}% update={:.0}% delete={:.0}% scan={:.0}%",
        read_ratio * 100.0,
        config.write_ratio * 100.0,
        config.update_ratio * 100.0,
        config.delete_ratio * 100.0,
        config.scan_ratio * 100.0,
    );

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
            let mut client = DemoClient::connect(&addr).await.expect("connect");
            let gen = ProductGenerator::new(42);
            let mut rng = StdRng::seed_from_u64(4000 + task_id as u64);
            let mut read_stats = BenchStats::new();
            let mut write_stats = BenchStats::new();
            let mut update_stats = BenchStats::new();
            let mut delete_stats = BenchStats::new();
            let mut scan_stats = BenchStats::new();

            while Instant::now() < deadline {
                let product_id = rng.random_range(0..product_range);
                let record_id = ProductGenerator::record_id(product_id);

                match pick_op(&mut rng, write_ratio, update_ratio, delete_ratio, scan_ratio) {
                    Op::Write => {
                        let items = gen.generate_product(product_id);
                        let t = Instant::now();
                        match client.put_product(&ns, &record_id, items).await {
                            Ok(_) => write_stats.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "mixed write error"),
                        }
                    }
                    Op::Update => {
                        let items = ProductGenerator::generate_update_items(&mut rng);
                        let t = Instant::now();
                        match client.put_product(&ns, &record_id, items).await {
                            Ok(_) => update_stats.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "mixed update error"),
                        }
                    }
                    Op::Delete => {
                        let t = Instant::now();
                        match client.delete_product(&ns, &record_id).await {
                            Ok(_) => delete_stats.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "mixed delete error"),
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
                            Ok(_) => scan_stats.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "mixed scan error"),
                        }
                    }
                    Op::Read => {
                        let keys = vec![b"info".to_vec(), b"price".to_vec()];
                        let t = Instant::now();
                        match client.get_product_keys(&ns, &record_id, keys).await {
                            Ok(_) => read_stats.record(t.elapsed()),
                            Err(e) => tracing::warn!(error = %e, "mixed read error"),
                        }
                    }
                }
            }
            (read_stats, write_stats, update_stats, delete_stats, scan_stats)
        }));
    }

    let mut result = MixedResult {
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

    report::print_header("Mixed Benchmark");
    report::print_stats_row("read", &result.read.snapshot());
    report::print_stats_row("write", &result.write.snapshot());
    report::print_stats_row("update", &result.update.snapshot());
    report::print_stats_row("delete", &result.delete.snapshot());
    report::print_stats_row("scan", &result.scan.snapshot());

    let categories: &[(&str, &BenchStats)] = &[
        ("Read", &result.read),
        ("Write", &result.write),
        ("Update", &result.update),
        ("Delete", &result.delete),
        ("Scan", &result.scan),
    ];
    for (label, stats) in categories {
        let snap = stats.snapshot();
        if snap.total_ops > 0 {
            println!("\n{} totals:", label);
            report::print_summary(&snap);
        }
    }

    Ok(result)
}
