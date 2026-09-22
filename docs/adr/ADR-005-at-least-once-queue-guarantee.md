# ADR-005: Why the Queue Guarantee is At-Least-Once

## Status
Accepted

## Context
In distributed job systems, network partitions or process crashes can occur while a worker is processing a task or right after it completes the task before its acknowledgement (ACK) reaches the server.

## Decision
Design ForgeKV queue semantics around at-least-once delivery with worker leases, timeouts, and consumer idempotency, rather than claiming impossible end-to-end exactly-once side-effects.

## Consequences
- **Pros**:
  - Honest and sound engineering design: acknowledges the two-generals problem.
  - Workers use leases to prevent duplicate concurrent executions, while exponential backoff and DLQ guard against poisonous jobs.
- **Cons**:
  - Downstream consumers must implement idempotent processing for duplicate job deliveries.
