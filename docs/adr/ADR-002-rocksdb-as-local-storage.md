# ADR-002: Why RocksDB is Local Storage and Not the Distributed Database Itself

## Status
Accepted

## Context
RocksDB is an embedded, persistent, Log-Structured Merge-tree (LSM) key-value engine. It provides high-performance local disk persistence, but does not provide network transport, replication, or distributed consensus.

## Decision
Use RocksDB strictly as a per-node node-local storage engine behind a clean `StorageEngine` interface, while using Raft to handle all distributed replication and ordering.

## Consequences
- **Pros**:
  - Clear separation of concerns: RocksDB guarantees durability and fast key-value lookups on disk; Raft guarantees distributed linearizability.
  - Testability: Storage can be mocked or replaced without altering consensus logic.
- **Cons**:
  - Writes must be written to the Raft log and committed before mutating state machine keys in RocksDB.
