package com.forgekv.integration;

import com.forgekv.client.ForgeKVQueueClient;
import com.forgekv.queue.proto.Job;
import com.forgekv.queue.proto.JobStatus;
import com.forgekv.raft.Role;
import com.forgekv.server.ForgeKVServer;
import com.forgekv.server.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterQueueLeaseTest {

    private final Map<String, ForgeKVServer> cluster = new HashMap<>();

    @AfterEach
    public void tearDown() {
        for (ForgeKVServer server : cluster.values()) {
            try {
                server.close();
            } catch (Exception ignored) {}
        }
        cluster.clear();
    }

    @Test
    public void testQueueLeaseExpiryAndRedelivery(@TempDir File tempBase) throws Exception {
        Map<String, String> peers = Map.of(
                "node1", "localhost:9301",
                "node2", "localhost:9302",
                "node3", "localhost:9303"
        );

        startNode("node1", 9301, 10301, new File(tempBase, "node1"), peers);
        startNode("node2", 9302, 10302, new File(tempBase, "node2"), peers);
        startNode("node3", 9303, 10303, new File(tempBase, "node3"), peers);

        String leaderId = awaitLeader(6000);
        assertThat(leaderId).isNotNull();

        try (ForgeKVQueueClient client = new ForgeKVQueueClient(peers)) {
            // 1. Enqueue job
            String jobId = client.enqueue("tasks", "task-payload-99");
            assertThat(jobId).isNotBlank();

            // 2. Worker A claims the job with a short lease of 800ms
            Optional<Job> claimA = client.claim("tasks", "worker-A", 800);
            assertThat(claimA).isPresent();
            assertThat(claimA.get().getId()).isEqualTo(jobId);
            assertThat(claimA.get().getStatus()).isEqualTo(JobStatus.PROCESSING);

            // 3. Worker A simulates crash (does NOT ACK). Wait for lease expiry + scanner (1.5s)
            Thread.sleep(2000);

            // 4. Worker B claims the redelivered job!
            Optional<Job> claimB = client.claim("tasks", "worker-B", 5000);
            assertThat(claimB).isPresent();
            assertThat(claimB.get().getId()).isEqualTo(jobId);
            assertThat(claimB.get().getWorkerId()).isEqualTo("worker-B");

            // 5. Worker B ACKs the job
            boolean acked = client.ack(jobId, "worker-B");
            assertThat(acked).isTrue();

            // 6. Verify job is COMPLETED
            Optional<Job> finalJob = client.getJob(jobId);
            assertThat(finalJob).isPresent();
            assertThat(finalJob.get().getStatus()).isEqualTo(JobStatus.COMPLETED);
        }
    }

    private void startNode(String nodeId, int port, int metricsPort, File dataDir, Map<String, String> peers) throws Exception {
        ServerConfig cfg = new ServerConfig(nodeId, port, metricsPort, dataDir, peers);
        ForgeKVServer server = new ForgeKVServer(cfg);
        server.start();
        cluster.put(nodeId, server);
    }

    private String awaitLeader(long timeoutMs) throws InterruptedException {
        long end = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < end) {
            for (Map.Entry<String, ForgeKVServer> e : cluster.entrySet()) {
                if (e.getValue().getRaftNode().getRole() == Role.LEADER) {
                    return e.getKey();
                }
            }
            Thread.sleep(100);
        }
        return null;
    }
}
