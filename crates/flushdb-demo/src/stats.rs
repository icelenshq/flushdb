use std::time::{Duration, Instant};

use hdrhistogram::Histogram;

pub struct StatsSnapshot {
    pub total_ops: u64,
    pub elapsed: Duration,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub ops_per_sec: f64,
}

pub struct BenchStats {
    histogram: Histogram<u64>,
    start: Instant,
    ops: u64,
}

impl Default for BenchStats {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchStats {
    pub fn new() -> Self {
        Self {
            histogram: Histogram::new_with_bounds(1, 60_000_000, 3)
                .expect("valid histogram bounds"),
            start: Instant::now(),
            ops: 0,
        }
    }

    pub fn record(&mut self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        let clamped = micros.clamp(1, 60_000_000);
        self.histogram.record(clamped).ok();
        self.ops += 1;
    }

    pub fn snapshot(&self) -> StatsSnapshot {
        let elapsed = self.start.elapsed();
        let secs = elapsed.as_secs_f64();
        StatsSnapshot {
            total_ops: self.ops,
            elapsed,
            p50_us: self.histogram.value_at_quantile(0.50),
            p95_us: self.histogram.value_at_quantile(0.95),
            p99_us: self.histogram.value_at_quantile(0.99),
            max_us: self.histogram.max(),
            ops_per_sec: if secs > 0.0 {
                self.ops as f64 / secs
            } else {
                0.0
            },
        }
    }

    pub fn merge(&mut self, other: &BenchStats) {
        self.histogram.add(&other.histogram).ok();
        self.ops += other.ops;
    }
}
