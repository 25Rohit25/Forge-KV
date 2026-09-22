# ForgeKV: Distributed Systems Interview Guide

This guide contains comprehensive, production-grade answers to the 15 core distributed systems interview questions defined in **Section 30 of the ForgeKV Specification Guide**.

---

### 1. Why is a majority required?
In any cluster of $N$ nodes, a majority is defined as $\lfloor N/2 \rfloor + 1$. By the Pigeonhole Principle, any two majorities must overlap in at least one node. 
This overlap guarantees two foundational Raft safety invariants:
1. **Election Safety**: Two candidates cannot both win an election in the same term, because no two independent candidate majorities can exist simultaneously without sharing at least one common voting node.
2. **Log Completeness**: A candidate must secure votes from a majority; therefore, at least one node in that majority holds every previously committed entry. The log freshness check ensures this node will refuse to vote for any candidate whose log is older, guaranteeing the elected leader contains all committed entries.

---

### 2. Why can an isolated former leader not safely commit writes?
If a network partition isolates the leader with a minority of nodes (e.g. 1 node in a 3-node cluster), the isolated leader cannot achieve quorum replication ($\ge 2$ nodes) for any new writes. 
If the isolated leader acknowledged writes locally without quorum confirmation, those writes would be lost when the partition heals and the isolated leader discovers a newer term leader elected by the majority partition. Thus, writes must only complete after quorum commit.

---

### 3. What is the difference between appended, committed, and applied?
- **Appended**: The log entry has been written to the local persistent Raft log (in RocksDB) on a node. It has not yet achieved quorum agreement.
- **Committed**: The entry is known by the leader to be replicated across a majority of nodes in the leader's current term. The entry is now guaranteed to never be overwritten or lost.
- **Applied**: The committed command has been executed against the deterministic state machine (`ForgeKVStateMachine`). Its side-effects (mutating key-values or queue records) are visible to clients and the client's pending `CompletableFuture` is resolved.

---

### 4. Why does Raft compare last log term before last log index when voting?
Raft's freshness rule states:
```
candidate.lastLogTerm > voter.lastLogTerm ||
(candidate.lastLogTerm == voter.lastLogTerm && candidate.lastLogIndex >= voter.lastLogIndex)
```
A higher term number represents a strictly newer generation of the cluster. A node might have a longer log in an older term (e.g. an uncommitted sequence from an un-replicated leader), but an entry from a newer term supersedes older uncommitted terms. Only when the terms are equal does log length (`lastLogIndex`) determine completeness.

---

### 5. What happens to an uncommitted leader entry after a new leader is elected?
If an entry was only appended locally on an old leader and never replicated to a majority before that leader crashed:
- The new leader will not have this entry.
- When the old leader rejoins as a follower, the new leader sends `AppendEntries` with its own log entries.
- The follower detects a term mismatch at `prevLogIndex` and truncates its uncommitted suffix, permanently discarding the uncommitted entry.

---

### 6. Why persist `currentTerm` and `votedFor`?
Both `currentTerm` and `votedFor` must be persisted to disk (in RocksDB) before sending a response:
- If `currentTerm` were volatile, a restarted node might revert to an older term, accept stale RPCs, or disrupt active terms.
- If `votedFor` were volatile, a restarted node could vote for Candidate A in term $T$, crash, restart, forget its vote, and vote for Candidate B in the same term $T$, resulting in split-brain with two leaders in one term.

---

### 7. What does `nextIndex` mean?
`nextIndex[peer]` is a volatile leader-only state variable representing the next log index the leader will send to that follower. 
When a leader is elected, it initializes `nextIndex[peer] = lastLogIndex + 1`. If an `AppendEntries` RPC fails due to log inconsistency, the leader decrements `nextIndex[peer]` and retries, searching backward until it finds the point where the follower's log matches the leader's log.

---

### 8. How does a follower repair a conflicting suffix?
When a follower receives `AppendEntries(prevLogIndex, prevLogTerm, entries)`:
1. It inspects its local log at `prevLogIndex`.
2. If an entry exists at `prevLogIndex` but its term differs from `prevLogTerm`, the follower calls `raftLog.truncateSuffix(prevLogIndex)` which deletes the conflicting entry and all subsequent entries from both memory and RocksDB.
3. It appends the leader's entries starting from `prevLogIndex + 1`.

---

### 9. What consistency does your GET provide?
ForgeKV provides **strong linearizable reads** by routing all `GET` requests to the leader. 
Followers respond with `NOT_LEADER` along with a `leader_id` hint, prompting the client to redirect. To prevent stale reads from partitioned former leaders, the leader commits a no-op entry upon term assumption, ensuring all previous entries are applied before serving reads.

---

### 10. Why is your queue at-least-once instead of exactly-once?
True distributed exactly-once end-to-end processing is impossible due to the Two Generals Paradox: a worker may finish processing an external side-effect (e.g. charging a payment gateway or sending an email), but crash or lose network connectivity right before its ACK reaches the cluster. 
Because the server must redeliver the job when the lease expires to prevent data loss, delivery is inherently at-least-once. Consumers must handle idempotency.

---

### 11. What happens if a worker finishes but its ACK is lost?
The job remains in `PROCESSING` status until its lease expires. The leader's lease scanner detects the expired lease and proposes a `REQUEUE_JOB` command. The job becomes `READY` and is redelivered to another worker. The consumer's idempotency key prevents duplicate execution of side-effects.

---

### 12. How do you prevent two workers from owning the same active job?
When `claim()` is called, the leader proposes a `CLAIM_JOB` command through Raft. 
Because all Raft state machine transitions are serialized and deterministic, the state machine leases the job to the first worker, sets `status = PROCESSING`, records the worker's ID, and sets `leaseUntilEpochMs`. Any simultaneous or subsequent claim requests for the same job will observe `status != READY` and will not be granted the job.

---

### 13. Why must the leader choose lease timestamps?
Physical clocks across distributed machines suffer from clock skew and drift. If followers independently checked their local system clock (`System.currentTimeMillis()`) to determine lease expiration, follower A might consider a lease expired while follower B considers it active, causing state machine divergence. 
By having the leader compute `leaseUntil` and embed it inside the replicated command, all followers apply identical state transitions deterministically.

---

### 14. What bottleneck prevents this single-Raft-group design from scaling writes indefinitely?
In a single Raft group, **all writes must be processed by a single leader and replicated across all nodes**. 
Write throughput is constrained by:
1. Single-leader CPU and network bandwidth.
2. Quorum network round-trip latency.
3. RocksDB disk write throughput and fsync latency on each node.
Scaling writes horizontally requires range-based or hash-based partitioning across multiple independent Raft consensus groups (Multi-Raft).

---

### 15. How did you test failure safety rather than only the happy path?
ForgeKV employs automated fault injection testing:
1. `ClusterLeaderElectionTest`: Dynamically shuts down active leaders, verifying quorum election of a new leader and orderly rejoining of restarted leaders.
2. `ClusterReplicationAndFailoverTest`: Writes data, abruptly kills the leader container/process before client return, and verifies that committed values remain 100% durable on the newly elected leader.
3. `ClusterQueueLeaseTest`: Simulates worker crashes by withholding ACKs, verifying automated background lease expiry, requeueing, and successful recovery by alternate workers.
