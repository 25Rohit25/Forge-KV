package com.forgekv.unit;

import com.forgekv.queue.Job;
import com.forgekv.queue.JobCodec;
import com.forgekv.queue.JobStatus;
import com.forgekv.statemachine.Command;
import com.forgekv.statemachine.CommandCodec;
import com.forgekv.statemachine.CommandType;
import com.forgekv.statemachine.ForgeKVStateMachine;
import com.forgekv.statemachine.PayloadHelper;
import com.forgekv.storage.RocksDBStorageEngine;
import com.forgekv.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class QueueStateMachineTest {

    @Test
    public void testJobQueueFullLifecycle(@TempDir File tempDir) {
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            ForgeKVStateMachine fsm = new ForgeKVStateMachine(storage);

            UUID jobId = UUID.randomUUID();
            Job job = new Job(
                    jobId,
                    "orders",
                    "order-payload-123".getBytes(StandardCharsets.UTF_8),
                    JobStatus.READY,
                    0,
                    2, // max attempts = 2
                    1000L,
                    0L,
                    null,
                    "idemp-order-1",
                    null
            );

            // 1. Enqueue job
            Command enqueueCmd = new Command(UUID.randomUUID(), CommandType.ENQUEUE_JOB, 1000L, JobCodec.encode(job));
            fsm.apply(CommandCodec.encode(enqueueCmd));

            Optional<Job> storedJob = fsm.getJob(jobId.toString());
            assertThat(storedJob).isPresent();
            assertThat(storedJob.get().status()).isEqualTo(JobStatus.READY);

            // Verify idempotency
            Command duplicateEnqueue = new Command(UUID.randomUUID(), CommandType.ENQUEUE_JOB, 1000L, JobCodec.encode(job));
            byte[] idempResult = fsm.apply(CommandCodec.encode(duplicateEnqueue));
            assertThat(new String(idempResult, StandardCharsets.UTF_8)).isEqualTo(jobId.toString());

            // 2. Claim job by Worker-A (at time 1500, lease until 5000)
            Command claimCmd1 = new Command(UUID.randomUUID(), CommandType.CLAIM_JOB, 1500L,
                    PayloadHelper.encodeClaim("orders", "worker-A", 5000L));
            byte[] claimResult1 = fsm.apply(CommandCodec.encode(claimCmd1));
            Job claimedJob1 = JobCodec.decode(claimResult1);
            assertThat(claimedJob1.id()).isEqualTo(jobId);
            assertThat(claimedJob1.status()).isEqualTo(JobStatus.PROCESSING);
            assertThat(claimedJob1.workerId()).isEqualTo("worker-A");

            // 3. Second simultaneous claim by Worker-B must find NO job (mutual exclusion)
            Command claimCmd2 = new Command(UUID.randomUUID(), CommandType.CLAIM_JOB, 1500L,
                    PayloadHelper.encodeClaim("orders", "worker-B", 5000L));
            byte[] claimResult2 = fsm.apply(CommandCodec.encode(claimCmd2));
            assertThat(claimResult2).isEmpty();

            // 4. Wrong worker attempts to ACK (must fail)
            Command ackWrong = new Command(UUID.randomUUID(), CommandType.ACK_JOB, 2000L,
                    PayloadHelper.encodeAck(jobId.toString(), "worker-B"));
            byte[] ackWrongRes = fsm.apply(CommandCodec.encode(ackWrong));
            assertThat(ackWrongRes).containsExactly(0);

            // 5. Worker-A ACKs the job (must succeed)
            Command ackRight = new Command(UUID.randomUUID(), CommandType.ACK_JOB, 2000L,
                    PayloadHelper.encodeAck(jobId.toString(), "worker-A"));
            byte[] ackRightRes = fsm.apply(CommandCodec.encode(ackRight));
            assertThat(ackRightRes).containsExactly(1);

            Job finalJob = fsm.getJob(jobId.toString()).orElseThrow();
            assertThat(finalJob.status()).isEqualTo(JobStatus.COMPLETED);

            // Double ACK is safely idempotent
            byte[] doubleAckRes = fsm.apply(CommandCodec.encode(ackRight));
            assertThat(doubleAckRes).containsExactly(1);
        }
    }

    @Test
    public void testJobMaxAttemptsToDLQ(@TempDir File tempDir) {
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            ForgeKVStateMachine fsm = new ForgeKVStateMachine(storage);

            UUID jobId = UUID.randomUUID();
            Job job = new Job(jobId, "emails", "send-email".getBytes(StandardCharsets.UTF_8),
                    JobStatus.READY, 0, 1, 1000L, 0L, null, null, null);

            // Enqueue with maxAttempts = 1
            fsm.apply(CommandCodec.encode(new Command(UUID.randomUUID(), CommandType.ENQUEUE_JOB, 1000L, JobCodec.encode(job))));

            // Claim by worker-1
            fsm.apply(CommandCodec.encode(new Command(UUID.randomUUID(), CommandType.CLAIM_JOB, 1100L,
                    PayloadHelper.encodeClaim("emails", "worker-1", 2000L))));

            // NACK by worker-1 (attempt count becomes 1 >= maxAttempts 1 -> moved to DEAD/DLQ)
            Command nack = new Command(UUID.randomUUID(), CommandType.NACK_JOB, 1200L,
                    PayloadHelper.encodeNack(jobId.toString(), "worker-1", 5000L, "SMTP Connection Refused"));
            fsm.apply(CommandCodec.encode(nack));

            Job deadJob = fsm.getJob(jobId.toString()).orElseThrow();
            assertThat(deadJob.status()).isEqualTo(JobStatus.DEAD);
            assertThat(deadJob.lastError()).isEqualTo("SMTP Connection Refused");
        }
    }
}
