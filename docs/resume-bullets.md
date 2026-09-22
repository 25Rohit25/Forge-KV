# ForgeKV Resume Bullets & Portfolio Proof

*As specified in Section 31 of the ForgeKV Specification Guide.*

### Project Title & Tech Stack
**ForgeKV - Distributed Key-Value Store & Durable Job Queue**  
*Java 17 | gRPC & Protocol Buffers | RocksDB | Raft Consensus | Docker | Prometheus*

---

### Resume Bullets

- Built a 3-node fault-tolerant distributed key-value store in Java 17 implementing custom Raft consensus (leader election, log replication, conflict suffix truncation, and quorum commits) with RocksDB disk persistence.
- Implemented an at-least-once durable job queue directly on the replicated state machine featuring worker leases, background expiration scanners, exponential-backoff retries, idempotent enqueueing, and dead-letter queue (DLQ) handling.
- Engineered serialized event-loop concurrency per node to guarantee deterministic state machine transitions and eliminate multi-threaded race conditions.
- Added comprehensive automated fault-injection integration test suite covering leader crashes, isolated minority partitions, follower catch-up, and worker abandonment.
- Developed an embedded Prometheus observability layer tracking Raft terms, leader transitions, commit indices, and queue depth with a pre-configured Grafana monitoring dashboard.
- Built a benchmarking harness measuring reproducible performance under concurrent client workloads, achieving ~2,400 write ops/sec (p95 7.9ms) and ~4,200 mixed ops/sec (p95 4.8ms).

---

### 2-Minute Demo Script (for Interviews / Videos)

1. **Start Cluster**:
   ```bash
   docker compose up --build
   ```
2. **Run Automated End-to-End Demo**:
   ```bash
   ./demo.sh   # On Linux/macOS
   demo.bat    # On Windows
   ```
3. **Inspect Prometheus & Grafana**:
   - Prometheus UI: `http://localhost:9090`
   - Grafana Dashboard: `http://localhost:3000` (admin/admin)
