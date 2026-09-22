# Failure Model and Recovery Invariants

## Failure Scenarios Tested

| Failure Mode | Expected System Behavior | Implementation Verification |
|--------------|--------------------------|-----------------------------|
| **Leader crash** | Followers detect heartbeat timeout, elect new leader with majority; committed data preserved. | `ClusterReplicationAndFailoverTest` |
| **Old leader rejoins** | Discovers higher term in peer RPC, steps down to FOLLOWER, reconciles missing log suffix. | `ClusterLeaderElectionTest` |
| **Minority partition** | 1 node isolated cannot form majority (2 required); cannot commit new writes. | Consensus quorum check |
| **Worker crash with active lease** | Job lease expires; leader lease scanner proposes `REQUEUE_JOB`; new worker claims job. | `ClusterQueueLeaseTest` |
| **Duplicate client request** | Client retries identical `requestId` after network timeout; returns cached result without re-executing. | `StateMachineTest` |
| **Node restart from storage** | Re-opens RocksDB, recovers `currentTerm`, `votedFor`, and log entries; catches up commit index. | `StorageEngineTest`, `RaftLogTest` |

## Safety Rules
- **Non-Byzantine**: Nodes are fail-stop or crash-recovery; Byzantine behaviors (message forgery) are outside the model.
- **Durable Write Order**: Local term, vote, and log entry must be flushed to RocksDB before sending RPC responses.
- **Client Acknowledgement Rule**: No client write is acknowledged until quorum replication has been reached and the entry has been applied to the deterministic state machine.
