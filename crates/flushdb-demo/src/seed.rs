use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use crate::client::DemoClient;
use crate::data_gen::ProductGenerator;
use crate::report;

pub struct SeedConfig {
    pub server_addr: String,
    pub namespace: String,
    pub products: u32,
    pub concurrency: u32,
}

#[allow(dead_code)]
pub struct SeedResult {
    pub total_products: u32,
    pub elapsed: std::time::Duration,
    pub errors: u64,
}

pub async fn run_seed(config: SeedConfig) -> Result<SeedResult, Box<dyn std::error::Error>> {
    let start = Instant::now();
    let completed = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let total = config.products;
    let chunk_size = total / config.concurrency;

    let mut handles = Vec::new();

    for task_id in 0..config.concurrency {
        let range_start = task_id * chunk_size;
        let range_end = if task_id == config.concurrency - 1 {
            total
        } else {
            range_start + chunk_size
        };

        let addr = config.server_addr.clone();
        let ns = config.namespace.clone();
        let completed = completed.clone();
        let errors = errors.clone();
        let task_start = start;

        handles.push(tokio::spawn(async move {
            let mut client = match DemoClient::connect(&addr).await {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(task = task_id, error = %e, "Failed to connect");
                    errors.fetch_add((range_end - range_start) as u64, Ordering::Relaxed);
                    return;
                }
            };

            let gen = ProductGenerator::new(42);
            for product_id in range_start..range_end {
                let record_id = ProductGenerator::record_id(product_id);
                let items = gen.generate_product(product_id);
                match client.put_product(&ns, &record_id, items).await {
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(product_id, error = %e, "Put failed");
                        errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done.is_multiple_of(10_000) {
                    let elapsed = task_start.elapsed().as_secs_f64();
                    let ops_per_sec = done as f64 / elapsed;
                    report::print_seed_progress(done, total as u64, ops_per_sec);
                }
            }
        }));
    }

    for handle in handles {
        handle.await?;
    }

    let elapsed = start.elapsed();
    let total_done = completed.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    println!();
    println!(
        "Seeding complete: {} products in {:.2}s ({:.0} products/sec, {} errors)",
        total_done,
        elapsed.as_secs_f64(),
        total_done as f64 / elapsed.as_secs_f64(),
        total_errors,
    );

    Ok(SeedResult {
        total_products: total_done as u32,
        elapsed,
        errors: total_errors,
    })
}
