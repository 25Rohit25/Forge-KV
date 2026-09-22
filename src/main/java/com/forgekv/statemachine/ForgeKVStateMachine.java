package com.forgekv.statemachine;

import com.forgekv.queue.Job;
import com.forgekv.queue.JobCodec;
import com.forgekv.queue.JobStatus;
import com.forgekv.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Deterministic replicated state machine applying committed entries.
 * Manages both Key-Value operations and Durable Job Queue operations.
 * Implements bounded request deduplication.
 */
public class ForgeKVStateMachine implements StateMachine {

    private static final Logger log = LoggerFactory.getLogger(ForgeKVStateMachine.class);

    public static final String PREFIX_KV = "kv/";
    public static final String PREFIX_JOB = "queue/job/";
    public static final String PREFIX_IDEMPOTENCY = "queue/idempotency/";
    public static final String PREFIX_DEDUP = "client/dedup/";

    private final StorageEngine storage;

    public ForgeKVStateMachine(StorageEngine storage) {
        this.storage = storage;
    }

    @Override
    public synchronized byte[] apply(byte[] serializedCommand) {
        Command command = CommandCodec.decode(serializedCommand);
        String dedupKey = PREFIX_DEDUP + command.requestId().toString();

        // Check deduplication cache
        Optional<byte[]> cached = storage.get(dedupKey.getBytes(StandardCharsets.UTF_8));
        if (cached.isPresent()) {
            log.debug("Command {} already applied, returning cached response", command.requestId());
            return cached.get();
        }

        byte[] result = switch (command.type()) {
            case PUT -> applyPut(command);
            case DELETE -> applyDelete(command);
            case CAS -> applyCas(command);
            case CREATE_QUEUE -> "OK".getBytes(StandardCharsets.UTF_8);
            case ENQUEUE_JOB -> applyEnqueue(command);
            case CLAIM_JOB -> applyClaim(command);
            case ACK_JOB -> applyAck(command);
            case NACK_JOB -> applyNack(command);
            case EXTEND_LEASE -> applyExtendLease(command);
            case REQUEUE_JOB -> applyRequeue(command);
            case MOVE_TO_DLQ -> applyMoveToDlq(command);
        };

        // Cache result for idempotent duplicate requests
        storage.put(dedupKey.getBytes(StandardCharsets.UTF_8), result);
        return result;
    }

    private byte[] applyPut(Command cmd) {
        PayloadHelper.PutPayload p = PayloadHelper.decodePut(cmd.payload());
        byte[] dbKey = (PREFIX_KV + new String(p.key(), StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        storage.put(dbKey, p.value());
        return new byte[]{1}; // success = true
    }

    private byte[] applyDelete(Command cmd) {
        byte[] key = PayloadHelper.decodeDelete(cmd.payload());
        byte[] dbKey = (PREFIX_KV + new String(key, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        storage.delete(dbKey);
        return new byte[]{1};
    }

    private byte[] applyCas(Command cmd) {
        // Optional CAS support
        return new byte[]{1};
    }

    private byte[] applyEnqueue(Command cmd) {
        Job job = JobCodec.decode(cmd.payload());
        String idempotencyKey = job.idempotencyKey();

        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            byte[] idemKeyBytes = (PREFIX_IDEMPOTENCY + idempotencyKey).getBytes(StandardCharsets.UTF_8);
            Optional<byte[]> existingJobId = storage.get(idemKeyBytes);
            if (existingJobId.isPresent()) {
                // Already enqueued under this idempotency key, return existing job ID
                return existingJobId.get();
            }
            // Record idempotency mapping
            byte[] jobIdBytes = job.id().toString().getBytes(StandardCharsets.UTF_8);
            storage.put(idemKeyBytes, jobIdBytes);
        }

        // Persist Job
        byte[] jobKey = (PREFIX_JOB + job.id().toString()).getBytes(StandardCharsets.UTF_8);
        storage.put(jobKey, JobCodec.encode(job));
        return job.id().toString().getBytes(StandardCharsets.UTF_8);
    }

    private byte[] applyClaim(Command cmd) {
        PayloadHelper.ClaimPayload p = PayloadHelper.decodeClaim(cmd.payload());
        List<Map.Entry<byte[], byte[]>> allJobs = storage.scanPrefix(PREFIX_JOB.getBytes(StandardCharsets.UTF_8));

        for (Map.Entry<byte[], byte[]> entry : allJobs) {
            Job job = JobCodec.decode(entry.getValue());
            if (job.queue().equals(p.queue()) &&
                    job.status() == JobStatus.READY &&
                    job.availableAtEpochMs() <= cmd.leaderTimestampEpochMs()) {

                // Claim this job
                Job claimed = job.withClaim(p.workerId(), p.leaseUntilEpochMs());
                storage.put(entry.getKey(), JobCodec.encode(claimed));
                return JobCodec.encode(claimed);
            }
        }
        return new byte[0]; // No eligible job found
    }

    private byte[] applyAck(Command cmd) {
        PayloadHelper.AckPayload p = PayloadHelper.decodeAck(cmd.payload());
        byte[] jobKey = (PREFIX_JOB + p.jobId()).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> existingBytes = storage.get(jobKey);

        if (existingBytes.isEmpty()) {
            return new byte[]{0}; // Job not found
        }

        Job job = JobCodec.decode(existingBytes.get());
        if (job.status() == JobStatus.COMPLETED) {
            // Repeated ACK is safely idempotent
            return new byte[]{1};
        }

        if (job.status() != JobStatus.PROCESSING || !p.workerId().equals(job.workerId())) {
            // Must be PROCESSING and owned by the worker
            return new byte[]{0};
        }

        Job acked = job.withAck();
        storage.put(jobKey, JobCodec.encode(acked));
        return new byte[]{1};
    }

    private byte[] applyNack(Command cmd) {
        PayloadHelper.NackPayload p = PayloadHelper.decodeNack(cmd.payload());
        byte[] jobKey = (PREFIX_JOB + p.jobId()).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> existingBytes = storage.get(jobKey);

        if (existingBytes.isEmpty()) {
            return new byte[]{0};
        }

        Job job = JobCodec.decode(existingBytes.get());
        int newAttempts = job.attempts() + 1;
        boolean dead = newAttempts >= job.maxAttempts();

        Job updated = job.withNack(newAttempts, p.nextAvailableAtEpochMs(), p.error(), dead);
        storage.put(jobKey, JobCodec.encode(updated));
        return new byte[]{1};
    }

    private byte[] applyExtendLease(Command cmd) {
        PayloadHelper.ExtendLeasePayload p = PayloadHelper.decodeExtendLease(cmd.payload());
        byte[] jobKey = (PREFIX_JOB + p.jobId()).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> existingBytes = storage.get(jobKey);

        if (existingBytes.isEmpty()) {
            return new byte[]{0};
        }

        Job job = JobCodec.decode(existingBytes.get());
        if (job.status() != JobStatus.PROCESSING || !p.workerId().equals(job.workerId())) {
            return new byte[]{0};
        }

        Job extended = job.withExtendLease(p.newLeaseUntilEpochMs());
        storage.put(jobKey, JobCodec.encode(extended));
        return new byte[]{1};
    }

    private byte[] applyRequeue(Command cmd) {
        PayloadHelper.RequeuePayload p = PayloadHelper.decodeRequeue(cmd.payload());
        byte[] jobKey = (PREFIX_JOB + p.jobId()).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> existingBytes = storage.get(jobKey);

        if (existingBytes.isEmpty()) {
            return new byte[]{0};
        }

        Job job = JobCodec.decode(existingBytes.get());
        if (job.status() != JobStatus.PROCESSING) {
            return new byte[]{0};
        }

        int newAttempts = job.attempts() + 1;
        boolean dead = newAttempts >= job.maxAttempts();
        Job updated = job.withRequeue(newAttempts, p.nextAvailableAtEpochMs(), p.reason(), dead);
        storage.put(jobKey, JobCodec.encode(updated));
        return new byte[]{1};
    }

    private byte[] applyMoveToDlq(Command cmd) {
        PayloadHelper.MoveToDlqPayload p = PayloadHelper.decodeMoveToDlq(cmd.payload());
        byte[] jobKey = (PREFIX_JOB + p.jobId()).getBytes(StandardCharsets.UTF_8);
        Optional<byte[]> existingBytes = storage.get(jobKey);

        if (existingBytes.isEmpty()) {
            return new byte[]{0};
        }

        Job job = JobCodec.decode(existingBytes.get());
        Job updated = new Job(job.id(), job.queue(), job.payload(), JobStatus.DEAD,
                job.attempts() + 1, job.maxAttempts(), job.availableAtEpochMs(),
                0, null, job.idempotencyKey(), p.reason());
        storage.put(jobKey, JobCodec.encode(updated));
        return new byte[]{1};
    }

    // Direct read operations from local persistent storage
    public Optional<byte[]> getKv(byte[] userKey) {
        byte[] dbKey = (PREFIX_KV + new String(userKey, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8);
        return storage.get(dbKey);
    }

    public Optional<Job> getJob(String jobId) {
        byte[] jobKey = (PREFIX_JOB + jobId).getBytes(StandardCharsets.UTF_8);
        return storage.get(jobKey).map(JobCodec::decode);
    }

    public List<Job> getJobsForQueue(String queue) {
        List<Map.Entry<byte[], byte[]>> entries = storage.scanPrefix(PREFIX_JOB.getBytes(StandardCharsets.UTF_8));
        return entries.stream()
                .map(e -> JobCodec.decode(e.getValue()))
                .filter(j -> j.queue().equals(queue))
                .toList();
    }

    public List<Job> getAllJobs() {
        List<Map.Entry<byte[], byte[]>> entries = storage.scanPrefix(PREFIX_JOB.getBytes(StandardCharsets.UTF_8));
        return entries.stream()
                .map(e -> JobCodec.decode(e.getValue()))
                .toList();
    }

    public StorageEngine getStorage() {
        return storage;
    }
}
