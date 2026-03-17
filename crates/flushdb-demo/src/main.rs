mod bench;
mod cassandra_client;
mod client;
mod data_gen;
mod report;
mod seed;
pub mod stats;
mod verify;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "flushdb-demo", about = "flushdb demo: seed, bench, verify")]
struct Cli {
    #[arg(long, default_value = "http://127.0.0.1:50051", env = "FLUSHDB_SERVER_ADDR")]
    server_addr: String,

    #[arg(long, default_value = "ecommerce", env = "FLUSHDB_NAMESPACE")]
    namespace: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Seed {
        #[arg(long, default_value_t = 1_000_000)]
        products: u32,
        #[arg(long, default_value_t = 8)]
        concurrency: u32,
    },
    Bench {
        #[command(subcommand)]
        workload: BenchWorkload,
    },
    Verify {
        #[arg(long, default_value_t = 100)]
        sample_size: u32,
        #[arg(long, default_value_t = 1_000_000)]
        product_range: u32,
    },
}

#[derive(Subcommand)]
enum BenchWorkload {
    Write {
        #[arg(long, default_value_t = 60)]
        duration: u64,
        #[arg(long, default_value_t = 8)]
        concurrency: u32,
        #[arg(long, default_value_t = 1_000_000)]
        product_range: u32,
    },
    Read {
        #[arg(long, default_value_t = 60)]
        duration: u64,
        #[arg(long, default_value_t = 8)]
        concurrency: u32,
        #[arg(long, default_value_t = 1_000_000)]
        product_range: u32,
    },
    Scan {
        #[arg(long, default_value_t = 60)]
        duration: u64,
        #[arg(long, default_value_t = 8)]
        concurrency: u32,
        #[arg(long, default_value_t = 1_000_000)]
        product_range: u32,
    },
    Mixed {
        #[arg(long, default_value_t = 60)]
        duration: u64,
        #[arg(long, default_value_t = 8)]
        concurrency: u32,
        #[arg(long, default_value_t = 1_000_000)]
        product_range: u32,
        #[arg(long, default_value_t = 0.15)]
        write_ratio: f64,
        #[arg(long, default_value_t = 0.15)]
        update_ratio: f64,
        #[arg(long, default_value_t = 0.05)]
        delete_ratio: f64,
        #[arg(long, default_value_t = 0.05)]
        scan_ratio: f64,
    },
    Ingest {
        #[arg(long, default_value_t = 15_000_000)]
        total_products: u64,
        #[arg(long, default_value_t = 8)]
        concurrency: u32,
        #[arg(long, default_value_t = 500_000)]
        report_interval: u64,
        #[arg(long, default_value = "127.0.0.1:9042", env = "CASSANDRA_ADDR")]
        cassandra_addr: String,
    },
    Compare {
        #[arg(long, default_value_t = 60)]
        duration: u64,
        #[arg(long, default_value_t = 8)]
        concurrency: u32,
        #[arg(long, default_value_t = 1_000_000)]
        product_range: u32,
        #[arg(long, default_value_t = 0.15)]
        write_ratio: f64,
        #[arg(long, default_value_t = 0.15)]
        update_ratio: f64,
        #[arg(long, default_value_t = 0.05)]
        delete_ratio: f64,
        #[arg(long, default_value_t = 0.05)]
        scan_ratio: f64,
        #[arg(long, default_value = "127.0.0.1:9042", env = "CASSANDRA_ADDR")]
        cassandra_addr: String,
        #[arg(long, default_value_t = 1000)]
        seed_products: u32,
        #[arg(long, default_value_t = 10)]
        warmup_secs: u64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("flushdb_demo=info".parse().expect("valid directive")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Seed {
            products,
            concurrency,
        } => {
            println!("Seeding {} products with concurrency {}...", products, concurrency);
            seed::run_seed(seed::SeedConfig {
                server_addr: cli.server_addr,
                namespace: cli.namespace,
                products,
                concurrency,
            })
            .await?;
        }
        Commands::Bench { workload } => match workload {
            BenchWorkload::Write {
                duration,
                concurrency,
                product_range,
            } => {
                let cfg = bench::BenchConfig {
                    server_addr: cli.server_addr,
                    namespace: cli.namespace,
                    duration_secs: duration,
                    concurrency,
                    product_range,
                    write_ratio: 1.0,
                    update_ratio: 0.0,
                    delete_ratio: 0.0,
                    scan_ratio: 0.0,
                };
                bench::write::run(&cfg).await?;
            }
            BenchWorkload::Read {
                duration,
                concurrency,
                product_range,
            } => {
                let cfg = bench::BenchConfig {
                    server_addr: cli.server_addr,
                    namespace: cli.namespace,
                    duration_secs: duration,
                    concurrency,
                    product_range,
                    write_ratio: 0.0,
                    update_ratio: 0.0,
                    delete_ratio: 0.0,
                    scan_ratio: 0.0,
                };
                bench::read::run(&cfg).await?;
            }
            BenchWorkload::Scan {
                duration,
                concurrency,
                product_range,
            } => {
                let cfg = bench::BenchConfig {
                    server_addr: cli.server_addr,
                    namespace: cli.namespace,
                    duration_secs: duration,
                    concurrency,
                    product_range,
                    write_ratio: 0.0,
                    update_ratio: 0.0,
                    delete_ratio: 0.0,
                    scan_ratio: 0.0,
                };
                bench::scan::run(&cfg).await?;
            }
            BenchWorkload::Mixed {
                duration,
                concurrency,
                product_range,
                write_ratio,
                update_ratio,
                delete_ratio,
                scan_ratio,
            } => {
                let cfg = bench::BenchConfig {
                    server_addr: cli.server_addr,
                    namespace: cli.namespace,
                    duration_secs: duration,
                    concurrency,
                    product_range,
                    write_ratio,
                    update_ratio,
                    delete_ratio,
                    scan_ratio,
                };
                bench::mixed::run(&cfg).await?;
            }
            BenchWorkload::Ingest {
                total_products,
                concurrency,
                report_interval,
                cassandra_addr,
            } => {
                let cfg = bench::ingest::IngestConfig {
                    server_addr: cli.server_addr,
                    namespace: cli.namespace,
                    cassandra_addr,
                    total_products,
                    concurrency,
                    report_interval,
                };
                bench::ingest::run(&cfg)
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error> { e })?;
            }
            BenchWorkload::Compare {
                duration,
                concurrency,
                product_range,
                write_ratio,
                update_ratio,
                delete_ratio,
                scan_ratio,
                cassandra_addr,
                seed_products,
                warmup_secs,
            } => {
                let cfg = bench::BenchConfig {
                    server_addr: cli.server_addr,
                    namespace: cli.namespace,
                    duration_secs: duration,
                    concurrency,
                    product_range,
                    write_ratio,
                    update_ratio,
                    delete_ratio,
                    scan_ratio,
                };
                bench::compare::run(&cfg, &cassandra_addr, seed_products, warmup_secs)
                    .await
                    .map_err(|e| -> Box<dyn std::error::Error> { e })?;
            }
        },
        Commands::Verify {
            sample_size,
            product_range,
        } => {
            verify::run_verify(verify::VerifyConfig {
                server_addr: cli.server_addr,
                namespace: cli.namespace,
                sample_size,
                product_range,
            })
            .await?;
        }
    }

    Ok(())
}
