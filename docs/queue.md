# Durable Job Queue Semantics

ForgeKV incorporates an at-least-once durable job queue directly onto the replicated state machine.

## Job Lifecycle

```
                    CLAIM
READY ------------------------------------> PROCESSING
  ^                                            |     |
  |             timeout / NACK                 |     | ACK
  +--------------------------------------------+     v
                                                 COMPLETED
PROCESSING --- max attempts exceeded ---> DEAD (DLQ)
```

## Operations

| Operation | Replicated Command | Description |
|-----------|--------------------|-------------|
| `enqueue` | `ENQUEUE_JOB` | Persists new job in READY state with optional idempotency key. |
| `claim` | `CLAIM_JOB` | Atomically leases one available READY job whose `availableAt <= leaderNow`. |
| `ack` | `ACK_JOB` | Marks job as COMPLETED if workerId matches active lease. |
| `nack` | `NACK_JOB` | Increments attempts, schedules retry with delay, or moves to DEAD. |
| `extendLease` | `EXTEND_LEASE` | Grants additional processing time to active lease owner. |
| `lease expiry` | `REQUEUE_JOB` / `MOVE_TO_DLQ` | Periodic scanner on leader detects expired leases and proposes requeue or DLQ. |

## Invariants

- **Mutual Exclusion**: At most one worker can hold an active lease on a given job.
- **Clock Authority**: Followers never independently evaluate lease expiration; only the Raft leader evaluates local wall-clock time and proposes explicit replicated commands.
- **Idempotency**: Repeated enqueues with the same `idempotencyKey` return the existing job ID.
- **Terminal States**: `COMPLETED` and `DEAD` are terminal states. Repeated ACKs for completed jobs are safely idempotent.
