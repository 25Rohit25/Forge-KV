package com.forgekv.client;

import com.forgekv.queue.proto.AckRequest;
import com.forgekv.queue.proto.AckResponse;
import com.forgekv.queue.proto.ClaimRequest;
import com.forgekv.queue.proto.ClaimResponse;
import com.forgekv.queue.proto.EnqueueRequest;
import com.forgekv.queue.proto.EnqueueResponse;
import com.forgekv.queue.proto.ExtendLeaseRequest;
import com.forgekv.queue.proto.ExtendLeaseResponse;
import com.forgekv.queue.proto.GetJobRequest;
import com.forgekv.queue.proto.GetJobResponse;
import com.forgekv.queue.proto.Job;
import com.forgekv.queue.proto.NackRequest;
import com.forgekv.queue.proto.NackResponse;
import com.forgekv.queue.proto.QueueServiceGrpc;
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
 * Robust ForgeKV Queue Client featuring:
 * - Transparent leader redirection
 * - Automatic retry with exponential backoff
 * - Durable queue operations (Enqueue, Claim, Ack, Nack, ExtendLease, GetJob)
 */
public class ForgeKVQueueClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ForgeKVQueueClient.class);

    private final Map<String, String> nodeAddresses;
    private final List<String> nodeIds;
    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final Map<String, QueueServiceGrpc.QueueServiceBlockingStub> stubs = new ConcurrentHashMap<>();

    private volatile String currentLeaderId;
    private final int maxRetries;

    public ForgeKVQueueClient(Map<String, String> nodeAddresses, int maxRetries) {
        this.nodeAddresses = new ConcurrentHashMap<>(nodeAddresses);
        this.nodeIds = new ArrayList<>(nodeAddresses.keySet());
        this.maxRetries = maxRetries;
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("nodeAddresses must not be empty");
        }
        this.currentLeaderId = nodeIds.get(0);
    }

    public ForgeKVQueueClient(Map<String, String> nodeAddresses) {
        this(nodeAddresses, 5);
    }

    private synchronized QueueServiceGrpc.QueueServiceBlockingStub getStub(String targetNodeId) {
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
            return QueueServiceGrpc.newBlockingStub(ch);
        });
    }

    public String enqueue(String queue, byte[] payload, int maxAttempts, long delayMs, String idempotencyKey) {
        Objects.requireNonNull(queue);
        Objects.requireNonNull(payload);
        String reqId = UUID.randomUUID().toString();

        return executeWithRetry("ENQUEUE", stub -> {
            EnqueueRequest req = EnqueueRequest.newBuilder()
                    .setQueue(queue)
                    .setPayload(ByteString.copyFrom(payload))
                    .setMaxAttempts(maxAttempts)
                    .setDelayMs(delayMs)
                    .setIdempotencyKey(idempotencyKey != null ? idempotencyKey : "")
                    .setRequestId(reqId)
                    .build();
            EnqueueResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).enqueue(req);
            if (resp.getSuccess()) {
                return resp.getJobId();
            }
            handleNotLeader(resp.getLeaderId());
            return null;
        });
    }

    public String enqueue(String queue, String stringPayload) {
        return enqueue(queue, stringPayload.getBytes(StandardCharsets.UTF_8), 3, 0, null);
    }

    public Optional<Job> claim(String queue, String workerId, long leaseDurationMs) {
        Objects.requireNonNull(queue);
        Objects.requireNonNull(workerId);
        String reqId = UUID.randomUUID().toString();

        return executeWithRetry("CLAIM", stub -> {
            ClaimRequest req = ClaimRequest.newBuilder()
                    .setQueue(queue)
                    .setWorkerId(workerId)
                    .setLeaseDurationMs(leaseDurationMs)
                    .setRequestId(reqId)
                    .build();
            ClaimResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).claim(req);
            if ("NOT_LEADER".equals(resp.getErrorMessage())) {
                handleNotLeader(resp.getLeaderId());
                return null;
            }
            if (resp.getFound()) {
                return Optional.of(resp.getJob());
            } else {
                return Optional.empty();
            }
        });
    }

    public boolean ack(String jobId, String workerId) {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(workerId);
        String reqId = UUID.randomUUID().toString();

        return executeWithRetry("ACK", stub -> {
            AckRequest req = AckRequest.newBuilder()
                    .setJobId(jobId)
                    .setWorkerId(workerId)
                    .setRequestId(reqId)
                    .build();
            AckResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).ack(req);
            if ("NOT_LEADER".equals(resp.getErrorMessage())) {
                handleNotLeader(resp.getLeaderId());
                return null;
            }
            return resp.getSuccess();
        });
    }

    public boolean nack(String jobId, String workerId, String error) {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(workerId);
        String reqId = UUID.randomUUID().toString();

        return executeWithRetry("NACK", stub -> {
            NackRequest req = NackRequest.newBuilder()
                    .setJobId(jobId)
                    .setWorkerId(workerId)
                    .setErrorMessage(error != null ? error : "")
                    .setRequestId(reqId)
                    .build();
            NackResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).nack(req);
            if ("NOT_LEADER".equals(resp.getErrorMessage())) {
                handleNotLeader(resp.getLeaderId());
                return null;
            }
            return resp.getSuccess();
        });
    }

    public boolean extendLease(String jobId, String workerId, long additionalMs) {
        Objects.requireNonNull(jobId);
        Objects.requireNonNull(workerId);
        String reqId = UUID.randomUUID().toString();

        return executeWithRetry("EXTEND_LEASE", stub -> {
            ExtendLeaseRequest req = ExtendLeaseRequest.newBuilder()
                    .setJobId(jobId)
                    .setWorkerId(workerId)
                    .setAdditionalDurationMs(additionalMs)
                    .setRequestId(reqId)
                    .build();
            ExtendLeaseResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).extendLease(req);
            if ("NOT_LEADER".equals(resp.getErrorMessage())) {
                handleNotLeader(resp.getLeaderId());
                return null;
            }
            return resp.getSuccess();
        });
    }

    public Optional<Job> getJob(String jobId) {
        Objects.requireNonNull(jobId);
        String reqId = UUID.randomUUID().toString();

        return executeWithRetry("GET_JOB", stub -> {
            GetJobRequest req = GetJobRequest.newBuilder()
                    .setJobId(jobId)
                    .setRequestId(reqId)
                    .build();
            GetJobResponse resp = stub.withDeadlineAfter(3, TimeUnit.SECONDS).getJob(req);
            if ("NOT_LEADER".equals(resp.getErrorMessage())) {
                handleNotLeader(resp.getLeaderId());
                return null;
            }
            if (resp.getFound()) {
                return Optional.of(resp.getJob());
            } else {
                return Optional.empty();
            }
        });
    }

    @FunctionalInterface
    private interface StubOperation<T> {
        T execute(QueueServiceGrpc.QueueServiceBlockingStub stub) throws Exception;
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
                QueueServiceGrpc.QueueServiceBlockingStub stub = getStub(targetNode);
                T result = op.execute(stub);
                if (result != null) {
                    return result;
                }
            } catch (Exception ex) {
                log.warn("RPC {} failed on attempt {} against node {}: {}", opName, attempt, targetNode, ex.getMessage());
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
