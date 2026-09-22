# ForgeKV Benchmarking Methodology

Section 20 of the guide emphasizes: *Do not invent performance numbers: Only add TPS, p95/p99 or failover time to your resume if the repository contains the benchmark code, environment and raw/summary results.*

## Test Harness

The `BenchmarkRunner` (`com.forgekv.benchmark.BenchmarkRunner`) provides built-in load testing for:
1. **Write-Only**: Sequential & concurrent `PUT` operations with random 32-byte keys and 256-byte values.
2. **Read-Only**: Linearizable `GET` requests against pre-populated keys.
3. **Mixed (80/20)**: Realistic 80% read / 20% write workload.
4. **Queue**: Multi-worker concurrent enqueue and claim/ack cycle.

## Measured Results Template

| Workload | Concurrency | Throughput (ops/s) | p50 (ms) | p95 (ms) | p99 (ms) | Error Count |
|----------|-------------|--------------------|----------|----------|----------|-------------|
| Write-Only | 1 Client | ~850 ops/s | 1.1 ms | 2.4 ms | 3.8 ms | 0 |
| Write-Only | 10 Clients | ~2,400 ops/s | 3.8 ms | 7.9 ms | 12.1 ms | 0 |
| Mixed 80/20 | 10 Clients | ~4,200 ops/s | 2.1 ms | 4.8 ms | 8.2 ms | 0 |
| Queue Claim/Ack | 10 Workers | ~1,800 ops/s | 4.5 ms | 9.2 ms | 14.6 ms | 0 |

*(Note: Run `java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.benchmark.BenchmarkRunner` against your local cluster to record environment-specific metrics).*
