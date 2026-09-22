package com.forgekv.integration;

import com.forgekv.client.ForgeKVClient;
import com.forgekv.raft.Role;
import com.forgekv.server.ForgeKVServer;
import com.forgekv.server.ServerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ClusterReplicationAndFailoverTest {

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
    public void testReplicationAndLeaderFailoverPreservesData(@TempDir File tempBase) throws Exception {
        Map<String, String> peers = Map.of(
                "node1", "127.0.0.1:9201",
                "node2", "127.0.0.1:9202",
                "node3", "127.0.0.1:9203"
        );

        startNode("node1", 9201, 10201, new File(tempBase, "node1"), peers);
        startNode("node2", 9202, 10202, new File(tempBase, "node2"), peers);
        startNode("node3", 9203, 10203, new File(tempBase, "node3"), peers);

        String leaderId = awaitLeader(6000);
        assertThat(leaderId).isNotNull();

        // Client connects to all nodes and automatically routes to leader
        try (ForgeKVClient client = new ForgeKVClient(peers)) {
            client.putString("user:100", "Alice");
            client.putString("user:200", "Bob");

            Optional<String> val1 = client.getString("user:100");
            assertThat(val1).contains("Alice");

            Optional<String> val2 = client.getString("user:200");
            assertThat(val2).contains("Bob");

            // Stop leader node
            ForgeKVServer leaderServer = cluster.remove(leaderId);
            leaderServer.close();

            // Await failover
            String newLeaderId = awaitLeader(6000);
            assertThat(newLeaderId).isNotNull();
            assertThat(newLeaderId).isNotEqualTo(leaderId);
            Thread.sleep(500); // Allow new leader no-op entry to commit and apply

            // Verify committed data is completely intact on the new leader
            Optional<String> valAfterFailover = client.getString("user:100");
            assertThat(valAfterFailover).contains("Alice");

            Optional<String> val2AfterFailover = client.getString("user:200");
            assertThat(val2AfterFailover).contains("Bob");

            // Write new data to the new leader
            client.putString("user:300", "Charlie");
            assertThat(client.getString("user:300")).contains("Charlie");
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
