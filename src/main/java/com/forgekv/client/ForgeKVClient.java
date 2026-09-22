package com.forgekv.client;

import com.forgekv.kv.proto.DeleteRequest;
import com.forgekv.kv.proto.DeleteResponse;
import com.forgekv.kv.proto.GetRequest;
import com.forgekv.kv.proto.GetResponse;
import com.forgekv.kv.proto.KeyValueServiceGrpc;
import com.forgekv.kv.proto.PutRequest;
import com.forgekv.kv.proto.PutResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Robust ForgeKV client featuring:
 * - Transparent leader redirection (NOT_LEADER handling)
 * - Automatic retry with exponential backoff
 * - Idempotent request IDs across retries
 */
public class ForgeKVClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ForgeKVClient.class);

    private final Map<String, String> nodeAddresses; // nodeId -> host:port
    private final List<String> nodeIds;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, KeyValueServiceGrpc.KeyValueServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private volatile String currentLeaderId;
    private final int maxRetries;

    public ForgeKVClient(Map<String, String> nodeAddresses, int maxRetries) {
        this.nodeAddresses = new ConcurrentHashMap<>(nodeAddresses);
        this.nodeIds = new ArrayList<>(nodeAddresses.keySet());
        this.maxRetries = maxRetries;
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("nodeAddresses must not be empty");
        }
        this.currentLeaderId = nodeIds.get(0);
    }

    public ForgeKVClient(Map<String, String> nodeAddresses) {
        this(nodeAddresses, 5);
    }

    private synchronized KeyValueServiceGrpc.KeyValueServiceBlockingStub getStub(String targetNodeId) {
        return stubs.computeIfAbsent(targetNodeId, id -> {
            String target = nodeAddresses.get(id);
            if (target == null) {
                target = nodeAddresses.values().iterator().next();
            }
            String host = "127.0.0.1";
            int port = 7001;
            if (target.contains(":")) {
                String[] parts = target.split(":");
                host = parts[0];
                port = Integer.parseInt(parts[1]);
            }
            if ("localhost".equalsIgnoreCase(host)) {
                host = "127.0.0.1";
            }
            ManagedChannel ch = ManagedChannelBuilder.forAddress(host, port)
                    .usePlaintext()
                    .build();
            channels.put(id, ch);
            return KeyValueServiceGrpc.newBlockingStub(ch);
        });
    }

    public void put(byte[] key, byte[] value) {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        String reqId = UUID.randomUUID().toString();

        executeWithRetry("PUT", stub -> {
            PutRequest req = PutRequest.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setValue(ByteString.copyFrom(value))
                    .setRequestId(reqId)
                    .build();
            PutResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).put(req);
            if (resp.getSuccess()) {
                return true;
            }
            handleNotLeader(resp.getLeaderId());
            return null; // triggers retry
        });
    }

    public Optional<byte[]> get(byte[] key) {
        Objects.requireNonNull(key);
        String reqId = UUID.randomUUID().toString();

        return executeWithRetry("GET", stub -> {
            GetRequest req = GetRequest.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setRequestId(reqId)
                    .build();
            GetResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).get(req);
            if ("NOT_LEADER".equals(resp.getErrorMessage())) {
                handleNotLeader(resp.getLeaderId());
                return null;
            }
            if (resp.getFound()) {
                return Optional.of(resp.getValue().toByteArray());
            } else {
                return Optional.empty();
            }
        });
    }

    public void delete(byte[] key) {
        Objects.requireNonNull(key);
        String reqId = UUID.randomUUID().toString();

        executeWithRetry("DELETE", stub -> {
            DeleteRequest req = DeleteRequest.newBuilder()
                    .setKey(ByteString.copyFrom(key))
                    .setRequestId(reqId)
                    .build();
            DeleteResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).delete(req);
            if (resp.getSuccess()) {
                return true;
            }
            handleNotLeader(resp.getLeaderId());
            return null;
        });
    }

    public void putString(String key, String value) {
        put(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8));
    }

    public Optional<String> getString(String key) {
        return get(key.getBytes(StandardCharsets.UTF_8))
                .map(bytes -> new String(bytes, StandardCharsets.UTF_8));
    }

    @FunctionalInterface
    private interface StubOperation<T> {
        T execute(KeyValueServiceGrpc.KeyValueServiceBlockingStub stub) throws Exception;
    }

    private <T> T executeWithRetry(String opName, StubOperation<T> op) {
        int attempt = 0;
        long backoffMs = 100;

        while (attempt < maxRetries) {
            attempt++;
            String targetNode = currentLeaderId;
            if (targetNode == null || !nodeAddresses.containsKey(targetNode)) {
                targetNode = nodeIds.get(attempt % nodeIds.size());
            }

            try {
                KeyValueServiceGrpc.KeyValueServiceBlockingStub stub = getStub(targetNode);
                T result = op.execute(stub);
                if (result != null) {
                    return result;
                }
            } catch (Exception ex) {
                log.warn("RPC {} failed on attempt {} against node {}: {}", opName, attempt, targetNode, ex.getMessage());
                // Switch to next candidate node
                currentLeaderId = nodeIds.get(attempt % nodeIds.size());
            }

            try {
                Thread.sleep(backoffMs);
                backoffMs = Math.min(2000, backoffMs * 2);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Operation interrupted", e);
            }
        }
        throw new RuntimeException("Failed to execute " + opName + " after " + maxRetries + " attempts");
    }

    private void handleNotLeader(String leaderHint) {
        if (leaderHint != null && !leaderHint.isBlank() && nodeAddresses.containsKey(leaderHint)) {
            log.info("Redirecting to leader: {}", leaderHint);
            this.currentLeaderId = leaderHint;
        } else {
            int nextIdx = (nodeIds.indexOf(currentLeaderId) + 1) % nodeIds.size();
            this.currentLeaderId = nodeIds.get(nextIdx);
        }
    }

    @Override
    public synchronized void close() {
        for (ManagedChannel ch : channels.values()) {
            try {
                ch.shutdown().awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                ch.shutdownNow();
            }
        }
        channels.clear();
        stubs.clear();
    }
}
