use crate::stats::StatsSnapshot;

pub fn print_header(title: &str) {
    println!();
    println!("=== {} ===", title);
    println!(
        "{:<12} {:>10} {:>10} {:>10} {:>10} {:>12}",
        "Metric", "p50", "p95", "p99", "max", "ops/sec"
    );
    println!("{}", "-".repeat(68));
}

pub fn print_stats_row(label: &str, snap: &StatsSnapshot) {
    println!(
        "{:<12} {:>8}us {:>8}us {:>8}us {:>8}us {:>12.1}",
        label, snap.p50_us, snap.p95_us, snap.p99_us, snap.max_us, snap.ops_per_sec,
    );
}

pub fn print_summary(snap: &StatsSnapshot) {
    println!();
    println!("Total ops:    {}", snap.total_ops);
    println!("Elapsed:      {:.2}s", snap.elapsed.as_secs_f64());
    println!("Throughput:   {:.1} ops/sec", snap.ops_per_sec);
    println!(
        "Latency:      p50={} us  p95={} us  p99={} us  max={} us",
        snap.p50_us, snap.p95_us, snap.p99_us, snap.max_us,
    );
}

pub fn print_seed_progress(completed: u64, total: u64, ops_per_sec: f64) {
    let pct = (completed as f64 / total as f64) * 100.0;
    let remaining = total - completed;
    let eta_secs = if ops_per_sec > 0.0 {
        remaining as f64 / ops_per_sec
    } else {
        f64::INFINITY
    };
    println!(
        "  [{:>6.1}%] {}/{} products  ({:.0} products/sec, ETA {:.0}s)",
        pct, completed, total, ops_per_sec, eta_secs,
    );
}
