# Raft Consensus Implementation Details

ForgeKV implements the Raft consensus protocol as defined by Ongaro & Ousterhout, optimized for low-latency Java network services.

## Node States and Invariants

At any time, a node is in one of three roles:
- `FOLLOWER`: Passively responds to RPCs and resets election deadlines on heartbeats from valid leaders.
- `CANDIDATE`: Increments term, votes for self, and requests votes from peers when election timeout fires.
- `LEADER`: Handles all client writes, manages log replication, broadcasts periodic heartbeats, and determines committed indices.

```
FOLLOWER ----(timeout)----> CANDIDATE ----(majority votes)----> LEADER
   ^                             |                                |
   |                             |                                |
   +-----------------------------+--------------------------------+
                  (higher term detected / stepped down)
```

## Log Freshness Comparison

A voter node grants a vote only if:
1. `votedFor == null || votedFor == candidateId`
2. The candidate's log is at least as up-to-date as the voter's log:
   ```java
   boolean upToDate = candidate.lastLogTerm > my.lastLogTerm ||
                      (candidate.lastLogTerm == my.lastLogTerm && candidate.lastLogIndex >= my.lastLogIndex);
   ```

## AppendEntries and Log Consistency

When receiving `AppendEntriesArgs`:
1. Reply false if `term < currentTerm`.
2. Step down if `term > currentTerm`.
3. Check consistency at `prevLogIndex`:
   - If `raftLog.getLastLogIndex() < prevLogIndex`, reject.
   - If `raftLog.getTerm(prevLogIndex) != prevLogTerm`, delete conflicting local suffix and reject.
4. Overwrite any conflicting entries and append new entries.
5. If `leaderCommit > commitIndex`, set `commitIndex = min(leaderCommit, lastNewEntryIndex)` and apply committed entries in increasing order.

## Quorum Commit Rule

The leader advances `commitIndex` to index `N` if:
1. `N > commitIndex`
2. `raftLog.getTerm(N) == currentTerm` (entries from previous terms are committed only as a side-effect of committing an entry from the leader's current term).
3. A majority of nodes (including the leader) have `matchIndex >= N`.
