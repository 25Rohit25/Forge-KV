# ADR-008: Why Performance Claims Require Reproducible Benchmark Evidence

## Status
Accepted

## Context
Many distributed systems portfolio projects claim arbitrary throughput and latency figures without reproducible evidence or transparent methodology, diminishing technical credibility in engineering interviews.

## Decision
Do not publish unverified or fabricated performance claims. Include a self-contained, reproducible `BenchmarkRunner` harness and benchmark documentation with explicit workload definitions (write-only, read-only, mixed 80/20, queue claim/ack), measuring operations per second and p50/p95/p99 latency distributions.

## Consequences
- **Pros**:
  - Uncompromising technical integrity and credibility.
  - Anyone cloning the repository can run the benchmark and verify measured metrics on their own hardware.
- **Cons**:
  - Measured numbers vary depending on local hardware and Docker resource allocations.
