package com.forgekv.integration;

import com.forgekv.raft.RaftNode;
import com.forgekv.raft.Role;
import com.forgekv.server.ForgeKVServer;
import com.forgekv.server.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterLeaderElectionTest {

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
    public void testLeaderElectionAndFailover(@TempDir File tempBase) throws Exception {
        Map<String, String> peers = Map.of(
                "node1", "localhost:9101",
                "node2", "localhost:9102",
                "node3", "localhost:9103"
        );

        // Start 3 nodes with fast election timeouts for testing (200-400ms)
        startNode("node1", 9101, 10101, new File(tempBase, "node1"), peers);
        startNode("node2", 9102, 10102, new File(tempBase, "node2"), peers);
        startNode("node3", 9103, 10103, new File(tempBase, "node3"), peers);

        // 1. Wait for exactly one stable leader to emerge
        String initialLeaderId = awaitLeader(5000);
        assertThat(initialLeaderId).isNotNull();

        long leaderCount = cluster.values().stream()
                .filter(s -> s.getRaftNode().getRole() == Role.LEADER)
                .count();
        assertThat(leaderCount).isEqualTo(1);

        // 2. Stop the leader node
        ForgeKVServer killedLeader = cluster.remove(initialLeaderId);
        killedLeader.close();

        // 3. One remaining node becomes leader with majority
        String newLeaderId = awaitLeader(6000);
        assertThat(newLeaderId).isNotNull();
        assertThat(newLeaderId).isNotEqualTo(initialLeaderId);

        // 4. Restart the old leader: it rejoins as follower
        int oldPort = initialLeaderId.equals("node1") ? 9101 : (initialLeaderId.equals("node2") ? 9102 : 9103);
        int oldMetrics = oldPort + 1000;
        startNode(initialLeaderId, oldPort, oldMetrics, new File(tempBase, initialLeaderId), peers);

        ForgeKVServer restartedServer = cluster.get(initialLeaderId);
        // Rule 34 & 35: Start role as FOLLOWER and do not preserve LEADER role across restart
        assertThat(restartedServer.getRaftNode().getRole()).isNotEqualTo(Role.LEADER);
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
