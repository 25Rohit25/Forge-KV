# ADR-006: Why the Leader Computes Lease Timestamps

## Status
Accepted

## Context
Physical wall-clock time differs across nodes due to clock drift. If each follower independently checks its local system clock to determine whether a lease has expired or whether a job is claimable, state machine divergence will occur, breaking the fundamental replicated state machine invariant.

## Decision
Followers never independently consult their local wall clock to mutate state. The leader observes current epoch time and embeds deterministic timestamps (`leaderTimestampEpochMs`) inside replicated commands (`CLAIM_JOB`, `REQUEUE_JOB`, `MOVE_TO_DLQ`).

## Consequences
- **Pros**:
  - Ensures 100% deterministic state machine execution on every node.
  - Applying the same committed log prefix on an empty node produces identical queue state.
- **Cons**:
  - The leader must run a background lease scanner to actively propose requeue commands when leases elapse.
