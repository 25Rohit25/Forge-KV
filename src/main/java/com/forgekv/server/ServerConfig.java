package com.forgekv.server;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public record ServerConfig(
        String nodeId,
        int port,
        int metricsPort,
        File dataDir,
        Map<String, String> peerTargets
) {
    public static ServerConfig fromEnv() {
        String nodeId = getEnvOrDefault("NODE_ID", "node1");
        int port = Integer.parseInt(getEnvOrDefault("PORT", "7001"));
        int metricsPort = Integer.parseInt(getEnvOrDefault("METRICS_PORT", String.valueOf(port + 1000)));
        String dataDirPath = getEnvOrDefault("DATA_DIR", "./data/" + nodeId);
        String peersStr = getEnvOrDefault("PEERS", "");

        Map<String, String> peerTargets = new HashMap<>();
        if (!peersStr.isBlank()) {
            String[] peerEntries = peersStr.split(",");
            for (String p : peerEntries) {
                String[] parts = p.trim().split(":");
                if (parts.length == 2) {
                    // format: host:port (derive nodeId from host or port)
                    peerTargets.put(parts[0], p.trim());
                } else if (parts.length == 3) {
                    // format: peerId:host:port
                    peerTargets.put(parts[0], parts[1] + ":" + parts[2]);
                }
            }
        }

        return new ServerConfig(nodeId, port, metricsPort, new File(dataDirPath), peerTargets);
    }

    private static String getEnvOrDefault(String key, String defaultValue) {
        String val = System.getenv(key);
        if (val == null || val.isBlank()) {
            val = System.getProperty(key);
        }
        return (val != null && !val.isBlank()) ? val : defaultValue;
    }
}
