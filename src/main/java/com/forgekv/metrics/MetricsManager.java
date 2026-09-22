package com.forgekv.metrics;

import com.forgekv.queue.Job;
import com.forgekv.queue.JobStatus;
import com.forgekv.raft.RaftNode;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Prometheus-compatible metrics server exporting the required ForgeKV metrics:
 * - forgekv_raft_role
 * - forgekv_raft_term
 * - forgekv_leader_changes_total
 * - forgekv_commit_index
 * - forgekv_last_applied
 * - forgekv_log_entries
 * - forgekv_kv_requests_total
 * - forgekv_queue_ready_jobs
 * - forgekv_queue_processing_jobs
 * - forgekv_queue_dead_jobs
 */
public class MetricsManager implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(MetricsManager.class);

    private final RaftNode raftNode;
    private final int metricsPort;
    private HttpServer httpServer;
    private final AtomicLong kvRequestsTotal = new AtomicLong(0);

    public MetricsManager(RaftNode raftNode, int metricsPort) {
        this.raftNode = raftNode;
        this.metricsPort = metricsPort;
    }

    public synchronized void start() {
        try {
            httpServer = HttpServer.create(new InetSocketAddress(metricsPort), 0);
            httpServer.createContext("/metrics", exchange -> {
                String response = generatePrometheusMetrics();
                byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4");
                exchange.sendResponseHeaders(200, bytes.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(bytes);
                }
            });
            httpServer.setExecutor(Executors.newSingleThreadExecutor());
            httpServer.start();
            log.info("Prometheus metrics server listening on http://localhost:{}/metrics", metricsPort);
        } catch (IOException e) {
            log.warn("Failed to start metrics server on port {}: {}", metricsPort, e.getMessage());
        }
    }

    public void incrementKvRequests() {
        kvRequestsTotal.incrementAndGet();
    }

    private String generatePrometheusMetrics() {
        StringBuilder sb = new StringBuilder();

        // Raft metrics
        sb.append("# HELP forgekv_raft_role Raft node role (0=FOLLOWER, 1=CANDIDATE, 2=LEADER)\n");
        sb.append("# TYPE forgekv_raft_role gauge\n");
        sb.append("forgekv_raft_role{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(raftNode.getRole().ordinal()).append("\n\n");

        sb.append("# HELP forgekv_raft_term Current Raft term\n");
        sb.append("# TYPE forgekv_raft_term gauge\n");
        sb.append("forgekv_raft_term{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(raftNode.getCurrentTerm()).append("\n\n");

        sb.append("# HELP forgekv_leader_changes_total Total leader transitions\n");
        sb.append("# TYPE forgekv_leader_changes_total counter\n");
        sb.append("forgekv_leader_changes_total{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(raftNode.getLeaderChanges()).append("\n\n");

        sb.append("# HELP forgekv_commit_index Current commit index\n");
        sb.append("# TYPE forgekv_commit_index gauge\n");
        sb.append("forgekv_commit_index{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(raftNode.getCommitIndex()).append("\n\n");

        sb.append("# HELP forgekv_last_applied Current last applied index\n");
        sb.append("# TYPE forgekv_last_applied gauge\n");
        sb.append("forgekv_last_applied{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(raftNode.getLastApplied()).append("\n\n");

        sb.append("# HELP forgekv_log_entries Total Raft log entries\n");
        sb.append("# TYPE forgekv_log_entries gauge\n");
        sb.append("forgekv_log_entries{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(raftNode.getLastLogIndex()).append("\n\n");

        sb.append("# HELP forgekv_kv_requests_total Total KV requests handled\n");
        sb.append("# TYPE forgekv_kv_requests_total counter\n");
        sb.append("forgekv_kv_requests_total{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(kvRequestsTotal.get()).append("\n\n");

        // Queue metrics
        List<Job> allJobs = raftNode.getStateMachine().getAllJobs();
        long ready = allJobs.stream().filter(j -> j.status() == JobStatus.READY).count();
        long processing = allJobs.stream().filter(j -> j.status() == JobStatus.PROCESSING).count();
        long dead = allJobs.stream().filter(j -> j.status() == JobStatus.DEAD).count();

        sb.append("# HELP forgekv_queue_ready_jobs Number of ready jobs in queue\n");
        sb.append("# TYPE forgekv_queue_ready_jobs gauge\n");
        sb.append("forgekv_queue_ready_jobs{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(ready).append("\n\n");

        sb.append("# HELP forgekv_queue_processing_jobs Number of currently leased processing jobs\n");
        sb.append("# TYPE forgekv_queue_processing_jobs gauge\n");
        sb.append("forgekv_queue_processing_jobs{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(processing).append("\n\n");

        sb.append("# HELP forgekv_queue_dead_jobs Number of dead-letter jobs\n");
        sb.append("# TYPE forgekv_queue_dead_jobs gauge\n");
        sb.append("forgekv_queue_dead_jobs{node=\"").append(raftNode.getNodeId()).append("\"} ")
                .append(dead).append("\n");

        return sb.toString();
    }

    @Override
    public synchronized void close() {
        if (httpServer != null) {
            httpServer.stop(0);
            httpServer = null;
        }
    }
}
