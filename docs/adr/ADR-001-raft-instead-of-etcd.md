# ADR-001: Why Raft Instead of Delegating Consensus to etcd/PostgreSQL

## Status
Accepted

## Context
A distributed storage system requires consensus to coordinate writes, elect leaders, and guarantee linearizable log replication. Many projects delegate this responsibility to an external system such as etcd, ZooKeeper, or PostgreSQL advisory locks.

## Decision
Implement the Raft consensus algorithm from scratch directly within ForgeKV rather than delegating consensus to an external system.

## Consequences
- **Pros**:
  - Eliminates external operational dependencies.
  - Demonstrates mastery of core distributed systems fundamentals (e.g. leader election, log consistency, quorum replication, failover).
  - Tightly integrates log replication with local RocksDB engine and state machine.
- **Cons**:
  - Requires maintaining custom consensus logic and extensive invariant validation tests.
