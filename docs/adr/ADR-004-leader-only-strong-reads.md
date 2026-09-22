# ADR-004: Why All Strongly Consistent Reads Go to the Leader

## Status
Accepted

## Context
In a Raft cluster, followers may lag behind the leader due to network delays or slow disk I/O. Reading directly from a follower without consensus barriers can expose stale data.

## Decision
Route all strongly consistent reads (GETs) directly to the current Raft leader. Followers respond with `NOT_LEADER` accompanied by a `leader_id` redirection hint. The client automatically redirects to the indicated leader.

## Consequences
- **Pros**:
  - Guarantees linearizability and avoids dirty or stale reads.
  - Follower read lag does not compromise consistency contracts.
- **Cons**:
  - Read throughput is bound by leader capacity. (Follower stale reads can be added in future versions as an explicit option).
