use std::time::Instant;

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use super::BenchConfig;
use crate::client::DemoClient;
use crate::data_gen::ProductGenerator;
use crate::report;
use crate::stats::BenchStats;

pub async fn run(config: &BenchConfig) -> Result<BenchStats, Box<dyn std::error::Error>> {
    let deadline = Instant::now() + std::time::Duration::from_secs(config.duration_secs);
    let mut handles = Vec::new();

    for task_id in 0..config.concurrency {
        let addr = config.server_addr.clone();
        let ns = config.namespace.clone();
        let product_range = config.product_range;

        handles.push(tokio::spawn(async move {
            let mut client = DemoClient::connect(&addr).await.expect("connect");
            let gen = ProductGenerator::new(42);
            let mut rng = StdRng::seed_from_u64(1000 + task_id as u64);
            let mut stats = BenchStats::new();

            while Instant::now() < deadline {
                let product_id = rng.random_range(0..product_range);
                let record_id = ProductGenerator::record_id(product_id);
                let items = gen.generate_product(product_id);
                let op_start = Instant::now();
                match client.put_product(&ns, &record_id, items).await {
                    Ok(_) => stats.record(op_start.elapsed()),
                    Err(e) => tracing::warn!(error = %e, "write bench error"),
                }
            }
            stats
        }));
    }

    let mut merged = BenchStats::new();
    for handle in handles {
        let task_stats = handle.await?;
        merged.merge(&task_stats);
    }

    report::print_header("Write Benchmark");
    report::print_stats_row("write", &merged.snapshot());
    report::print_summary(&merged.snapshot());
    Ok(merged)
}
