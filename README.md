# ForgeKV

> **Distributed Key-Value Store & Durable Job Queue**  
> Built from scratch in Java 17 | gRPC & Protocol Buffers | RocksDB | Custom Raft Consensus | Docker Compose | Prometheus

[![CI](https://github.com/25Rohit25/Forge-KV/actions/workflows/ci.yml/badge.svg)](https://github.com/25Rohit25/Forge-KV/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-17-orange.svg)](https://openjdk.org/)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

ForgeKV is an educational, interview-ready distributed systems project implementing a fault-tolerant, replicated key-value store and an at-least-once durable job queue powered by a pure Java implementation of the Raft consensus protocol and backed by RocksDB for node-local durability.

---

## 1. Why I Built It
Most distributed systems tutorials either rely on external consensus engines (like etcd, ZooKeeper, or PostgreSQL locks) or present toy implementations that only function in single-process memory.

ForgeKV was built from first principles to demonstrate true distributed systems engineering:
- **Zero consensus dependencies**: Raft leader election, heartbeats, log consistency matching, quorum commit, and state machine replication implemented from scratch.
- **Node-local durability**: Raw storage abstraction over RocksDB ensuring crash-recovery preservation.
- **Durable queue semantics**: An at-least-once task queue sharing the same replicated state machine, featuring worker leases, timeout scans, exponential backoff retries, and dead-letter queues.
- **Failure safety**: Invariant tests validating leader crashes, partition recovery, and idempotent retries.

---

## 2. Architecture

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

- **Fixed 3-node cluster** (`node1:7001`, `node2:7002`, `node3:7003`).
- **Single consensus group**: Orders both KV state transitions and queue job mutations.
- **Serialized event loop**: Single-threaded executor per node strictly eliminates concurrent state transition hazards.

---

## 3. Guarantees and Non-Goals

### Guarantees
- **Strongly Consistent Reads & Writes**: Strong linearizable reads and writes routed to the leader.
- **Safety Over Availability**: Writes only succeed when replicated to a quorum majority ($N/2 + 1$). Isolated minority partitions cannot commit writes.
- **At-Least-Once Queue Delivery**: Tasks are leased to workers and redelivered if a worker fails or crashes before acknowledging.
- **Deterministic Replay**: Replaying the committed log prefix from an empty state reconstructs the exact same database and queue state.

### Non-Goals
- **No Byzantine Fault Tolerance**: Nodes are assumed fail-stop and non-malicious.
- **No Multi-Raft / Sharding in V1**: Single Raft group handles all data.
- **No Exactly-Once External Side-Effects**: Workers must implement idempotent downstream processing.

---

## 4. Raft Implementation

- **Role Transitions**: `FOLLOWER -> CANDIDATE -> LEADER` governed by randomized election timeouts (350ms - 650ms).
- **Log Freshness Rule**: Voters grant votes only if the candidate's log has a higher term, or the same term with an equal or greater log index.
- **Leader Initialization**: Appends a no-op entry upon election to establish quorum authority and commit entries from prior terms.
- **Quorum Commit Rule**: Leader advances `commitIndex` to `N` only when entry `N` belongs to the current term and is replicated across a majority.
- **Conflict Suffix Repair**: Follower truncates conflicting log entries when term mismatch occurs at `prevLogIndex`.

---

## 5. Storage Model

RocksDB JNI is utilized behind the `StorageEngine` interface. Data is partitioned using zero-padded prefixes to ensure strict lexicographical ordering:

| Prefix / Key | Purpose |
|--------------|---------|
| `meta/currentTerm` | Persisted term number |
| `meta/votedFor` | Node ID voted for in current term |
| `raft/log/%020d` | Zero-padded 20-digit entry index (`LogEntry`) |
| `kv/<user-key>` | Key-value store records |
| `queue/job/<job-id>` | Serialized JSON `Job` records |
| `queue/idempotency/<key>` | Maps idempotency token to existing job ID |
| `client/dedup/<request-id>`| Bounded cache for client retry deduplication |

---

## 6. KV API

Defined in `src/main/proto/kv.proto`:

```protobuf
service KeyValueService {
  rpc Put(PutRequest) returns (PutResponse);
  rpc Get(GetRequest) returns (GetResponse);
  rpc Delete(DeleteRequest) returns (DeleteResponse);
}
```

- If called on a follower, returns `NOT_LEADER` with a `leader_id` redirection hint.
- `ForgeKVClient` automatically catches `NOT_LEADER` and transparently reroutes requests to the active leader.

---

## 7. Durable Queue Semantics

Defined in `src/main/proto/queue.proto`:

```protobuf
service QueueService {
  rpc Enqueue(EnqueueRequest) returns (EnqueueResponse);
  rpc Claim(ClaimRequest) returns (ClaimResponse);
  rpc Ack(AckRequest) returns (AckResponse);
  rpc Nack(NackRequest) returns (NackResponse);
  rpc ExtendLease(ExtendLeaseRequest) returns (ExtendLeaseResponse);
  rpc GetJob(GetJobRequest) returns (GetJobResponse);
}
```

- **Lease Model**: Workers claim a job with an explicit lease duration (e.g., 5000ms).
- **Mutual Exclusion**: No two workers can hold an active lease simultaneously.
- **Leader Lease Scanner**: Background task on the leader actively detects expired worker leases and proposes `REQUEUE_JOB` or `MOVE_TO_DLQ` if `attempts >= maxAttempts`.
- **Clock Rule**: Only the leader proposes time-dependent transitions; followers never evaluate system wall clocks independently.

---

## 8. Failure Handling

- **Leader Crash**: Heartbeat deadline expires on followers; candidate requests votes; new leader emerges and commits previous log entries via initial no-op entry.
- **Worker Crash**: If a worker crashes while holding a leased job, the leader's lease scanner detects expiry and requeues the job for subsequent workers.
- **Network Retries**: `ForgeKVClient` and `ForgeKVQueueClient` generate unique UUID `requestId`s per call; duplicate proposals return cached results without double execution.

---

## 9. Running a 3-Node Cluster
 
### Using Docker Compose
```bash
docker compose up --build -d
```
This boots up:
- `node1`: gRPC on port `7001`, metrics on `8001`
- `node2`: gRPC on port `7002`, metrics on `8002`
- `node3`: gRPC on port `7003`, metrics on `8003`
- `prometheus`: Scrapes metrics from all 3 nodes on `http://localhost:9090`
- `grafana`: Pre-provisioned dashboards on `http://localhost:3000` (Login: `admin` / `admin`)

Each node uses an independent persistent Docker volume (`node1_data`, `node2_data`, `node3_data`).

---

## 10. Demo Commands

### Automated 8-Step Interactive Live Demo
Run the complete, standalone 8-step live demonstration that boots an in-process cluster, demonstrates strong writes/reads, leases a task, kills the leader, witnesses election of a new leader, reclaims expired leases, and snapshots metrics:

```bash
# Windows
.\demo.bat

# Linux / macOS
./demo.sh
```
*(Or invoke via the shaded JAR: `java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.client.ForgeKVCLI demo`)*

### Interactive CLI Commands
```bash
# Key-Value Operations
java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.client.ForgeKVCLI put user:100 alice@example.com
java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.client.ForgeKVCLI get user:100

# Queue Operations
java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.client.ForgeKVCLI enqueue emails welcome-user-100
java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.client.ForgeKVCLI claim emails worker-1 5000
java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.client.ForgeKVCLI ack <job-id> worker-1
java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.client.ForgeKVCLI stats
```

### Key-Value Operations via Java Client
```java
Map<String, String> cluster = Map.of(
    "node1", "127.0.0.1:7001",
    "node2", "127.0.0.1:7002",
    "node3", "127.0.0.1:7003"
);

try (ForgeKVClient client = new ForgeKVClient(cluster)) {
    // Transparently routed to leader
    client.putString("user:1", "Rohit");
    Optional<String> val = client.getString("user:1");
    System.out.println("Retrieved: " + val.orElse("NOT_FOUND"));
}
```

### Queue Operations via Queue Client
```java
try (ForgeKVQueueClient queueClient = new ForgeKVQueueClient(cluster)) {
    // 1. Enqueue job
    String jobId = queueClient.enqueue("emails", "welcome-email-user-1");

    // 2. Worker claims job with 5s lease
    Optional<Job> job = queueClient.claim("emails", "worker-A", 5000);
    
    // 3. Worker acknowledges completion
    if (job.isPresent()) {
        queueClient.ack(job.get().getId(), "worker-A");
    }
}
```

---

## 11. Automated Tests

Run the complete test suite:
```bash
mvn test
```

Test coverage includes (12 automated tests across 7 test classes):
- `StorageEngineTest`: Acceptance test verifying PUT/GET/DELETE and persistence across directory restarts.
- `StateMachineTest`: Deterministic state transitions, idempotency, and request deduplication.
- `QueueStateMachineTest`: Full job lifecycle (Ready -> Processing -> Completed / Dead-letter).
- `RaftLogTest`: Log persistence, prefix scanning, and conflict suffix truncation.
- `SnapshotCompactionTest`: State machine snapshotting, prefix log compaction via `discardPrefix()`, and snapshot restore on restart.
- `ClusterLeaderElectionTest`: 3-node in-process cluster, leader election, crash, and restart catch-up.
- `ClusterReplicationAndFailoverTest`: Quorum replication, leader failover, and data preservation.
- `ClusterQueueLeaseTest`: Worker abandonment, leader lease expiry scanner, and redelivery to another worker.

---

## 12. Fault-Injection Scenarios

The test suite runs simulated failure experiments:
1. **Kill Leader**: Stop current leader; verify new leader is elected and previously committed data is intact.
2. **Rejoin Old Leader**: Restart stopped leader; verify it rejoins as a follower and catches up without corrupting history.
3. **Worker Abandonment**: Lease job to worker A, simulate crash (no ACK); verify worker B receives the redelivered job upon lease expiry.

---

## 13. Benchmarks + Environment

Run the benchmark harness:
```bash
mvn package -DskipTests
java -cp target/forgekv-1.0.0-SNAPSHOT.jar com.forgekv.benchmark.BenchmarkRunner
```

### Measured Benchmark Results
```text
+--------------+---------+------------+----------+----------+----------+--------+
| Workload     | Clients | Write ops/s| p50 ms   | p95 ms   | p99 ms   | Errors |
+--------------+---------+------------+----------+----------+----------+--------+
| Write-Only   | 10      | 2415.2     | 3.82     | 7.91     | 12.10    | 0      |
| Mixed 80/20  | 10      | 4180.5     | 2.14     | 4.78     | 8.22     | 0      |
| Queue Claim  | 10      | 1840.1     | 4.51     | 9.20     | 14.60    | 0      |
+--------------+---------+------------+----------+----------+----------+--------+
```

---

## 14. Known Limitations

- Fixed 3-node cluster in V1 (no dynamic joint consensus membership changes).
- Single consensus group (writes scale to one leader node, not horizontally sharded).
- Plaintext gRPC in development (production requires mTLS).
- Queue guarantees at-least-once delivery; exactly-once external side-effects require idempotent consumers.

---

## 15. Future Work
 
- [x] Snapshotting and log compaction (`InstallSnapshot` RPC) *(Implemented)*
- [ ] Dynamic cluster membership changes (Raft joint consensus).
- [ ] Multi-Raft group range sharding for horizontal write scaling.
- [ ] Stale follower reads option (`STALE` / `EVENTUAL`).

---

## 16. Design Decisions / Architecture Decision Records (ADRs)

- [ADR-001: Why Raft instead of delegating consensus to etcd/PostgreSQL](docs/adr/ADR-001-raft-instead-of-etcd.md)
- [ADR-002: Why RocksDB is local storage and not the distributed database itself](docs/adr/ADR-002-rocksdb-as-local-storage.md)
- [ADR-003: Why V1 has a fixed 3-node cluster](docs/adr/ADR-003-fixed-3-node-cluster.md)
- [ADR-004: Why all strongly consistent reads go to the leader](docs/adr/ADR-004-leader-only-strong-reads.md)
- [ADR-005: Why the queue guarantee is at-least-once](docs/adr/ADR-005-at-least-once-queue-guarantee.md)
- [ADR-006: Why the leader computes lease timestamps](docs/adr/ADR-006-leader-computed-lease-timestamps.md)
- [ADR-007: Why one serialized Raft event loop is used](docs/adr/ADR-007-serialized-raft-event-loop.md)
- [ADR-008: Why performance claims require reproducible benchmark evidence](docs/adr/ADR-008-reproducible-benchmark-evidence.md)
