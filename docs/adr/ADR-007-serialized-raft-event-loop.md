# ADR-007: Why One Serialized Raft Event Loop is Used

## Status
Accepted

## Context
Multi-threaded distributed consensus code frequently suffers from race conditions between incoming RPCs, scheduled heartbeat/election timers, and client proposals. Ad-hoc synchronized blocks or complex lock hierarchies are notoriously error-prone.

## Decision
Serialize all Raft state transitions on each node using a dedicated single-threaded `ScheduledExecutorService` (`raftExecutor`). All timer deadlines, inbound RPC handlers (`handleRequestVote`, `handleAppendEntries`), client proposals, and peer callbacks submit tasks to this single event loop.

## Consequences
- **Pros**:
  - Eliminates thread interleaving bugs, deadlocks, and missed state updates.
  - Radical code simplification: Raft node internal state does not require fine-grained concurrent locking.
- **Cons**:
  - Long blocking operations (such as disk fsync) must not block the event loop; RocksDB writes are optimized and IO callbacks are decoupled where necessary.
