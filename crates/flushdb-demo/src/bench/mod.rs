pub mod compare;
pub mod ingest;
pub mod mixed;
pub mod read;
pub mod scan;
pub mod write;

#[derive(Clone)]
pub struct BenchConfig {
    pub server_addr: String,
    pub namespace: String,
    pub duration_secs: u64,
    pub concurrency: u32,
    pub product_range: u32,
    pub write_ratio: f64,
    pub update_ratio: f64,
    pub delete_ratio: f64,
    pub scan_ratio: f64,
}
