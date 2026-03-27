# flushdb vs Cassandra Benchmark Results

This document is the benchmark record for the full 10-step Docker ladder run completed on March 27, 2026.

## Benchmark Profile

Environment:

| Component | Configuration |
|-----------|---------------|
| flushdb | Single node, 2 CPU / 1 GiB, Docker container |
| MinIO | `RELEASE.2025-02-28T09-55-16Z`, 1 CPU / 512 MiB, Docker container |
| Cassandra 4.1 | Single node, 2 CPU / 1 GiB, 512 MiB heap, 1 token, LeveledCompactionStrategy, hints disabled, Docker container |
| Benchmark client | `flushdb-demo` via `docker compose run --rm --no-deps` |
| Host OS | Darwin 25.2.0 (macOS) |

flushdb benchmark profile overrides:

| Setting | Value |
|---------|-------|
| Namespace partitions | `ecommerce:8` |
| WAL fsync mode | `batch_sync` |
| WAL batch sync interval | `10ms` |

Measured ladder parameters:

| Parameter | Value |
|-----------|-------|
| Scale steps | Full 10-step ladder (`1,000/100,000` through `100,000/10,000,000`) |
| Measurement window | 180s |
| Warmup | 1s per backend |
| Concurrency | 8 workers |
| Op mix | read=60% / write=15% / update=15% / delete=5% / scan=5% |
| Artifact directory | `benchmark-artifacts/2026-03-27-full-ladder-5s1s` |

## End Result

All ten scale steps completed successfully. Cassandra experienced sporadic write timeouts at steps 1, 8, and 10 (LocalQuorum consistency not met), but all steps ran to completion.

Tables below show `flushdb / cassandra`, followed by the ratio in parentheses.

### p50 Latency (`flushdb us / cassandra us`)

| Seed | Range | Read | Write | Update | Delete | Scan |
|------|-------|------|-------|--------|--------|------|
| 1,000 | 100,000 | `185 / 979` (`5.3x`) | `261 / 974` (`3.7x`) | `245 / 975` (`4.0x`) | `246 / 975` (`4.0x`) | `483 / 975` (`2.0x`) |
| 2,500 | 250,000 | `181 / 980` (`5.4x`) | `260 / 984` (`3.8x`) | `244 / 983` (`4.0x`) | `244 / 983` (`4.0x`) | `551 / 976` (`1.8x`) |
| 5,000 | 500,000 | `164 / 976` (`6.0x`) | `249 / 985` (`4.0x`) | `235 / 983` (`4.2x`) | `235 / 982` (`4.2x`) | `647 / 975` (`1.5x`) |
| 10,000 | 1,000,000 | `153 / 971` (`6.3x`) | `243 / 985` (`4.1x`) | `229 / 980` (`4.3x`) | `231 / 982` (`4.3x`) | `874 / 970` (`1.1x`) |
| 15,000 | 1,500,000 | `144 / 977` (`6.8x`) | `236 / 988` (`4.2x`) | `221 / 987` (`4.5x`) | `223 / 986` (`4.4x`) | `936 / 975` (`1.0x`) |
| 25,000 | 2,500,000 | `138 / 976` (`7.1x`) | `233 / 990` (`4.2x`) | `218 / 988` (`4.5x`) | `219 / 987` (`4.5x`) | `1024 / 975` (`1.0x`) |
| 40,000 | 4,000,000 | `135 / 975` (`7.2x`) | `231 / 989` (`4.3x`) | `216 / 986` (`4.6x`) | `217 / 986` (`4.5x`) | `1109 / 974` (`0.9x`) |
| 50,000 | 5,000,000 | `131 / 972` (`7.4x`) | `223 / 988` (`4.4x`) | `209 / 985` (`4.7x`) | `210 / 985` (`4.7x`) | `1097 / 972` (`0.9x`) |
| 75,000 | 7,500,000 | `127 / 971` (`7.6x`) | `220 / 988` (`4.5x`) | `205 / 984` (`4.8x`) | `205 / 984` (`4.8x`) | `1148 / 972` (`0.8x`) |
| 100,000 | 10,000,000 | `127 / 970` (`7.6x`) | `220 / 986` (`4.5x`) | `205 / 983` (`4.8x`) | `206 / 983` (`4.8x`) | `1145 / 969` (`0.8x`) |

### Throughput (`flushdb ops/s / cassandra ops/s`)

| Seed | Range | Read | Write | Update | Delete | Scan |
|------|-------|------|-------|--------|--------|------|
| 1,000 | 100,000 | `7473.6 / 4356.7` (`1.7x`) | `1873.7 / 1094.0` (`1.7x`) | `1869.0 / 1090.9` (`1.7x`) | `625.3 / 364.2` (`1.7x`) | `620.4 / 363.0` (`1.7x`) |
| 2,500 | 250,000 | `7275.3 / 4471.4` (`1.6x`) | `1824.3 / 1122.7` (`1.6x`) | `1819.9 / 1119.4` (`1.6x`) | `608.5 / 373.9` (`1.6x`) | `604.2 / 372.5` (`1.6x`) |
| 5,000 | 500,000 | `6874.6 / 4352.9` (`1.6x`) | `1724.9 / 1093.1` (`1.6x`) | `1720.3 / 1089.7` (`1.6x`) | `575.0 / 364.1` (`1.6x`) | `571.2 / 362.6` (`1.6x`) |
| 10,000 | 1,000,000 | `6970.4 / 4548.0` (`1.5x`) | `1748.6 / 1142.0` (`1.5x`) | `1744.5 / 1138.6` (`1.5x`) | `583.2 / 380.4` (`1.5x`) | `579.2 / 378.5` (`1.5x`) |
| 15,000 | 1,500,000 | `7115.4 / 4380.2` (`1.6x`) | `1784.5 / 1099.8` (`1.6x`) | `1780.9 / 1096.8` (`1.6x`) | `595.1 / 366.5` (`1.6x`) | `591.2 / 364.8` (`1.6x`) |
| 25,000 | 2,500,000 | `7361.5 / 4255.6` (`1.7x`) | `1846.2 / 1068.5` (`1.7x`) | `1840.9 / 1064.4` (`1.7x`) | `615.9 / 355.6` (`1.7x`) | `611.1 / 354.2` (`1.7x`) |
| 40,000 | 4,000,000 | `7486.0 / 4396.6` (`1.7x`) | `1877.7 / 1103.9` (`1.7x`) | `1872.8 / 1101.0` (`1.7x`) | `626.3 / 367.5` (`1.7x`) | `621.6 / 366.0` (`1.7x`) |
| 50,000 | 5,000,000 | `7626.8 / 4189.3` (`1.8x`) | `1911.2 / 1051.0` (`1.8x`) | `1907.9 / 1048.5` (`1.8x`) | `637.4 / 349.6` (`1.8x`) | `633.1 / 348.4` (`1.8x`) |
| 75,000 | 7,500,000 | `7505.2 / 4191.5` (`1.8x`) | `1881.2 / 1051.7` (`1.8x`) | `1878.3 / 1049.1` (`1.8x`) | `627.5 / 349.7` (`1.8x`) | `622.9 / 348.8` (`1.8x`) |
| 100,000 | 10,000,000 | `7385.8 / 4012.1` (`1.8x`) | `1851.5 / 1005.3` (`1.8x`) | `1847.1 / 1004.8` (`1.8x`) | `618.0 / 335.6` (`1.8x`) | `613.4 / 333.6` (`1.8x`) |

## Summary

- The full 10-step 180s / 1s ladder completed end-to-end; no step failed.
- **Read p50 latency:** flushdb wins every step, improving from `5.3x` at small scale to `7.6x` at the top step (`127us` vs `970us` at `100,000 / 10,000,000`).
- **Write/Update/Delete p50 latency:** Consistent `3.7x`-`4.8x` advantage across all scales. At the top step, write p50 is `220us` vs `986us` (`4.5x`), update p50 is `205us` vs `983us` (`4.8x`).
- **Scan p50 latency:** flushdb wins at smaller scales (`2.0x` at step 1) but Cassandra pulls ahead at larger scales (steps 7-10 show `0.8x`-`0.9x`). At `100,000 / 10,000,000`, scan p50 is `1145us` vs `969us` -- Cassandra's scan p50 is flat (~970us) while flushdb's grows with dataset size.
- **Throughput:** Stable `1.5x`-`1.8x` advantage across all ops and scales. At the top step, read throughput is `7385.8 ops/s` vs `4012.1 ops/s`, write throughput is `1851.5 ops/s` vs `1005.3 ops/s`.
- **Cassandra stability:** Sporadic write timeouts (LocalQuorum not met) observed at steps 1, 8, and 10 under sustained 180s load, suggesting Cassandra's single-node 1 GiB / 512 MiB heap configuration is under pressure at longer measurement windows.
- The benchmark profile matters: `batch_sync` is a closer durability match to Cassandra's periodic commit log behavior than fully synchronous fsync on every write.

## Commands Used

```bash
bash scripts/benchmark.sh --duration 180 --warmup 1 --skip-smoke --output-dir benchmark-artifacts/2026-03-27-full-ladder-5s1s
```
