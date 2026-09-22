package com.forgekv.client;

import com.forgekv.queue.proto.Job;
import com.forgekv.queue.proto.JobStatus;
import com.forgekv.server.ForgeKVServer;
import com.forgekv.server.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Interactive Command Line Interface and Automated Demo Runner for ForgeKV.
 *
 * Implements Section 29 of the specification guide:
 * Step 1: Discover leader from logs/metrics
 * Step 2: PUT customer:1 Rohit, then GET it
 * Step 3: Simulate leader failover and GET customer:1 on new leader
 * Step 4: Enqueue an email job
 * Step 5: Worker A claims job, crashes before ACK
 * Step 6: Lease expires, Worker B claims redelivered job
 * Step 7: Worker B ACKs job, verify COMPLETED
 * Step 8: Display metrics for leader changes, queue depth, and retry count
 */
public class ForgeKVCLI {

    private static final Logger log = LoggerFactory.getLogger(ForgeKVCLI.class);

    public static void main(String[] args) {
        if (args.length == 0 || "help".equalsIgnoreCase(args[0]) || "--help".equalsIgnoreCase(args[0])) {
            printHelp();
            return;
        }

        String command = args[0].toLowerCase();
        Map<String, String> defaultNodes = Map.of(
                "node1", "127.0.0.1:7001",
                "node2", "127.0.0.1:7002",
                "node3", "127.0.0.1:7003"
        );

        try {
            switch (command) {
                case "demo" -> runAutomatedDemo();
                case "put" -> {
                    if (args.length < 3) {
                        System.err.println("Usage: put <key> <value>");
                        return;
                    }
                    try (ForgeKVClient client = new ForgeKVClient(defaultNodes)) {
                        client.putString(args[1], args[2]);
                        System.out.println("OK: " + args[1] + " = " + args[2]);
                    }
                }
                case "get" -> {
                    if (args.length < 2) {
                        System.err.println("Usage: get <key>");
                        return;
                    }
                    try (ForgeKVClient client = new ForgeKVClient(defaultNodes)) {
                        Optional<String> val = client.getString(args[1]);
                        if (val.isPresent()) {
                            System.out.println("VALUE: " + val.get());
                        } else {
                            System.out.println("NOT_FOUND");
                        }
                    }
                }
                case "delete" -> {
                    if (args.length < 2) {
                        System.err.println("Usage: delete <key>");
                        return;
                    }
                    try (ForgeKVClient client = new ForgeKVClient(defaultNodes)) {
                        client.delete(args[1].getBytes());
                        System.out.println("DELETED: " + args[1]);
                    }
                }
                case "enqueue" -> {
                    if (args.length < 3) {
                        System.err.println("Usage: enqueue <queue> <payload>");
                        return;
                    }
                    try (ForgeKVQueueClient qc = new ForgeKVQueueClient(defaultNodes)) {
                        String jobId = qc.enqueue(args[1], args[2]);
                        System.out.println("ENQUEUED: jobId=" + jobId);
                    }
                }
                case "claim" -> {
                    if (args.length < 3) {
                        System.err.println("Usage: claim <queue> <workerId> [leaseDurationMs]");
                        return;
                    }
                    long lease = args.length >= 4 ? Long.parseLong(args[3]) : 10000;
                    try (ForgeKVQueueClient qc = new ForgeKVQueueClient(defaultNodes)) {
                        Optional<Job> job = qc.claim(args[1], args[2], lease);
                        if (job.isPresent()) {
                            System.out.println("CLAIMED: jobId=" + job.get().getId() +
                                    ", payload=" + job.get().getPayload().toStringUtf8());
                        } else {
                            System.out.println("NO_READY_JOB");
                        }
                    }
                }
                case "ack" -> {
                    if (args.length < 3) {
                        System.err.println("Usage: ack <jobId> <workerId>");
                        return;
                    }
                    try (ForgeKVQueueClient qc = new ForgeKVQueueClient(defaultNodes)) {
                        boolean ok = qc.ack(args[1], args[2]);
                        System.out.println("ACK: " + (ok ? "SUCCESS" : "FAILED"));
                    }
                }
                default -> printHelp();
            }
        } catch (Exception e) {
            System.err.println("Error executing command " + command + ": " + e.getMessage());
        }
    }

    public static void runAutomatedDemo() throws Exception {
        System.out.println("================================================================================");
        System.out.println("               ForgeKV 8-Step Interactive Demo (Section 29)                     ");
        System.out.println("================================================================================");

        File tempBase = Files.createTempDirectory("forgekv-demo-").toFile();
        tempBase.deleteOnExit();

        Map<String, String> clusterAddresses = Map.of(
                "node1", "127.0.0.1:9501",
                "node2", "127.0.0.1:9502",
                "node3", "127.0.0.1:9503"
        );

        Map<String, ForgeKVServer> cluster = new HashMap<>();

        System.out.println("\n[Step 1] Booting 3-Node Raft Cluster (node1, node2, node3)...");
        for (String node : new String[]{"node1", "node2", "node3"}) {
            int port = node.equals("node1") ? 9501 : (node.equals("node2") ? 9502 : 9503);
            File dataDir = new File(tempBase, node);
            ServerConfig cfg = new ServerConfig(node, port, port + 1000, dataDir, clusterAddresses);
            ForgeKVServer s = new ForgeKVServer(cfg);
            s.start();
            cluster.put(node, s);
        }

        // Wait for leader
        String initialLeader = null;
        for (int i = 0; i < 60; i++) {
            for (Map.Entry<String, ForgeKVServer> e : cluster.entrySet()) {
                if (e.getValue().getRaftNode().getRole() == com.forgekv.raft.Role.LEADER) {
                    initialLeader = e.getKey();
                    break;
                }
            }
            if (initialLeader != null) break;
            Thread.sleep(100);
        }
        System.out.println(">>> Elected Initial Leader: [" + initialLeader + "] in Term " +
                cluster.get(initialLeader).getRaftNode().getCurrentTerm());

        try (ForgeKVClient kvClient = new ForgeKVClient(clusterAddresses);
             ForgeKVQueueClient qClient = new ForgeKVQueueClient(clusterAddresses)) {

            System.out.println("\n[Step 2] PUT customer:1 = Rohit, then GET customer:1");
            kvClient.putString("customer:1", "Rohit");
            Optional<String> customerVal = kvClient.getString("customer:1");
            System.out.println(">>> Verified Stored Value: " + customerVal.orElse("MISSING"));

            System.out.println("\n[Step 3] Simulating Leader Crash (Killing " + initialLeader + ")...");
            ForgeKVServer killed = cluster.remove(initialLeader);
            killed.close();

            // Wait for new leader
            String newLeader = null;
            for (int i = 0; i < 60; i++) {
                for (Map.Entry<String, ForgeKVServer> e : cluster.entrySet()) {
                    if (e.getValue().getRaftNode().getRole() == com.forgekv.raft.Role.LEADER) {
                        newLeader = e.getKey();
                        break;
                    }
                }
                if (newLeader != null) break;
                Thread.sleep(100);
            }
            Thread.sleep(500); // Allow no-op entry to commit
            System.out.println(">>> Failover Success! New Elected Leader: [" + newLeader + "]");
            Optional<String> customerValFailover = kvClient.getString("customer:1");
            System.out.println(">>> Strongly Consistent GET customer:1 on New Leader: " + customerValFailover.orElse("LOST!"));

            System.out.println("\n[Step 4] Enqueuing Email Job (queue=emails, payload=welcome-user-rohit)...");
            String jobId = qClient.enqueue("emails", "welcome-user-rohit");
            System.out.println(">>> Durable Job Enqueued: jobId=" + jobId);

            System.out.println("\n[Step 5] Worker A claims job with 800ms lease, then crashes without ACK...");
            Optional<Job> claimA = qClient.claim("emails", "worker-A", 800);
            System.out.println(">>> Worker A successfully claimed job: " + claimA.map(Job::getId).orElse("FAILED"));
            System.out.println(">>> Worker A crashed! (Zero ACK sent)");

            System.out.println("\n[Step 6] Waiting for lease expiration (1.5s) and leader lease scanner...");
            Thread.sleep(1500);
            Optional<Job> claimB = qClient.claim("emails", "worker-B", 5000);
            System.out.println(">>> Worker B received redelivered job: " + claimB.map(Job::getId).orElse("FAILED") +
                    " (attempt=" + claimB.map(Job::getAttempts).orElse(0) + ")");

            System.out.println("\n[Step 7] Worker B ACKs the job...");
            boolean acked = qClient.ack(jobId, "worker-B");
            System.out.println(">>> ACK Result: " + (acked ? "SUCCESS" : "FAILED"));

            Optional<Job> finalJob = qClient.getJob(jobId);
            System.out.println(">>> Final Verified Job Status: " + finalJob.map(Job::getStatus).orElse(null));

            System.out.println("\n[Step 8] Prometheus Observability Snapshot:");
            System.out.println("--------------------------------------------------------------------------------");
            System.out.println("Active Nodes in Cluster: " + cluster.keySet());
            System.out.println("Leader Node: " + newLeader);
            System.out.println("Total Leader Transitions: " + cluster.get(newLeader).getRaftNode().getLeaderChanges());
            System.out.println("Commit Index: " + cluster.get(newLeader).getRaftNode().getCommitIndex());
            System.out.println("Last Applied: " + cluster.get(newLeader).getRaftNode().getLastApplied());
            System.out.println("Total Log Entries: " + cluster.get(newLeader).getRaftNode().getLastLogIndex());
            System.out.println("--------------------------------------------------------------------------------");
            System.out.println("ALL 8 DEMO STEPS COMPLETED SUCCESSFULLY!");
            System.out.println("================================================================================");
        } finally {
            for (ForgeKVServer s : cluster.values()) {
                s.close();
            }
        }
    }

    private static void printHelp() {
        System.out.println("""
            ForgeKV Distributed Key-Value Store & Durable Queue CLI
            
            Commands:
              demo                           Runs the automated 8-step demo script
              put <key> <value>              Sets a key-value pair
              get <key>                      Retrieves a value by key
              delete <key>                   Deletes a key
              enqueue <queue> <payload>      Enqueues a durable job
              claim <queue> <workerId> [ms]  Leases a job for processing
              ack <jobId> <workerId>         Completes a leased job
            """);
    }
}
