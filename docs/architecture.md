# ForgeKV Architecture

ForgeKV is an interview-ready, distributed, fault-tolerant key-value store and durable job queue built from scratch in Java 17 using gRPC, RocksDB, and custom Raft consensus.

## High-Level Topology

```
                      +--------------------+
                      |   ForgeKV Client   |
                      +---------+----------+
                                |
                              gRPC
                                |
             +------------------v------------------+
             |               Node 1                |
             |   gRPC -> Raft -> Replicated Log    |
             |              |                      |
             |        State Machine                |
             |         /          \                |
             |        KV          Queue            |
             |         \          /                |
             |            RocksDB                  |
             +------------------+------------------+
                                |
                    AppendEntries / Heartbeats
                              /   \
                             /     \
             +--------------v---+ +--v---------------+
             |      Node 2      | |      Node 3       |
             |follower/candidate| |follower/candidate |
             |  log + RocksDB   | |   log + RocksDB   |
             +------------------+ +-------------------+
```

## Core Components

1. **gRPC Protocol Boundary**:
   - `KeyValueService`: PUT, GET, DELETE operations with leader redirection hints.
   - `RaftService`: Consensus protocol transport (`RequestVote`, `AppendEntries`).
   - `QueueService`: Durable at-least-once task queue operations (`Enqueue`, `Claim`, `Ack`, `Nack`, `ExtendLease`, `GetJob`).

2. **Raft Consensus Layer (`com.forgekv.raft`)**:
   - Single-threaded event loop per node (`ScheduledExecutorService`) eliminating lock contention and concurrency hazards.
   - Randomized election timeout (350ms - 650ms), heartbeat intervals (100ms).
   - Strict log freshness comparison before granting votes.
   - Quorum replication with backtrack repair for lagging or conflicting follower logs.
   - Initial leader no-op commit to ensure safe linearization of previous term entries.

3. **Deterministic State Machine (`com.forgekv.statemachine`)**:
   - Applies committed commands strictly in increasing log-index order.
   - Leader embeds timestamps in commands, ensuring followers evaluate temporal conditions (e.g. lease timeouts) deterministically.
   - Bounded request deduplication table (`client/dedup/<requestId>`) for idempotent retries.

4. **Durable Storage Layer (`com.forgekv.storage`)**:
   - Backed by RocksDB JNI (`RocksDBStorageEngine`).
   - Key namespaces:
     - `meta/`: Raft term and vote state (`meta/currentTerm`, `meta/votedFor`).
     - `raft/log/%020d`: Zero-padded 20-digit log indices ensuring lexicographical order.
     - `kv/<user-key>`: Key-value data entries.
     - `queue/job/<job-id>`: Serialized queue jobs.
     - `queue/idempotency/<key>`: Idempotency deduplication mapping.
     - `client/dedup/<request-id>`: Command result cache.
