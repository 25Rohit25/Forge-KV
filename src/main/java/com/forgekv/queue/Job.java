package com.forgekv.queue;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable Job entity matching ForgeKV specification.
 */
public record Job(
        UUID id,
        String queue,
        byte[] payload,
        JobStatus status,
        int attempts,
        int maxAttempts,
        long availableAtEpochMs,
        long leaseUntilEpochMs,
        String workerId,
        String idempotencyKey,
        String lastError
) {
    public Job {
        Objects.requireNonNull(id, "id must not be null");
        Objects.requireNonNull(queue, "queue must not be null");
        if (payload == null) {
            payload = new byte[0];
        }
        if (status == null) {
            status = JobStatus.READY;
        }
    }

    public Job withClaim(String workerId, long leaseUntilEpochMs) {
        return new Job(id, queue, payload, JobStatus.PROCESSING, attempts, maxAttempts,
                availableAtEpochMs, leaseUntilEpochMs, workerId, idempotencyKey, lastError);
    }

    public Job withAck() {
        return new Job(id, queue, payload, JobStatus.COMPLETED, attempts, maxAttempts,
                availableAtEpochMs, 0, workerId, idempotencyKey, lastError);
    }

    public Job withNack(int newAttempts, long newAvailableAtEpochMs, String error, boolean dead) {
        return new Job(id, queue, payload, dead ? JobStatus.DEAD : JobStatus.READY,
                newAttempts, maxAttempts, newAvailableAtEpochMs, 0, null, idempotencyKey, error);
    }

    public Job withExtendLease(long newLeaseUntilEpochMs) {
        return new Job(id, queue, payload, status, attempts, maxAttempts,
                availableAtEpochMs, newLeaseUntilEpochMs, workerId, idempotencyKey, lastError);
    }

    public Job withRequeue(int newAttempts, long nextAvailableAtEpochMs, String reason, boolean dead) {
        return new Job(id, queue, payload, dead ? JobStatus.DEAD : JobStatus.READY,
                newAttempts, maxAttempts, nextAvailableAtEpochMs, 0, null, idempotencyKey, reason);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Job other)) return false;
        return attempts == other.attempts &&
                maxAttempts == other.maxAttempts &&
                availableAtEpochMs == other.availableAtEpochMs &&
                leaseUntilEpochMs == other.leaseUntilEpochMs &&
                id.equals(other.id) &&
                queue.equals(other.queue) &&
                status == other.status &&
                Arrays.equals(payload, other.payload) &&
                Objects.equals(workerId, other.workerId) &&
                Objects.equals(idempotencyKey, other.idempotencyKey) &&
                Objects.equals(lastError, other.lastError);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, queue, status, attempts, maxAttempts, availableAtEpochMs, leaseUntilEpochMs, workerId, idempotencyKey, lastError);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
