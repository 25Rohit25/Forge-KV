# ADR-003: Why V1 Has a Fixed 3-Node Cluster

## Status
Accepted

## Context
Raft dynamic cluster membership changes (such as Joint Consensus) introduce considerable complexity and edge-case hazards during recovery.

## Decision
For V1, fix the cluster topology to 3 nodes (`node1`, `node2`, `node3`).

## Consequences
- **Pros**:
  - Provides fault tolerance against 1 node failure (majority quorum of 2 out of 3).
  - Keeps consensus transitions clear and interview-defensible without joint-consensus state machine complications.
- **Cons**:
  - Expanding or shrinking cluster membership dynamically requires restarting nodes with updated configuration in V1.
