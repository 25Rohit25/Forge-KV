package com.forgekv.queue;

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
import com.forgekv.queue.proto.NackRequest;
import com.forgekv.queue.proto.NackResponse;
import com.forgekv.queue.proto.QueueServiceGrpc;
import com.forgekv.raft.NotLeaderException;
import com.forgekv.raft.RaftNode;
import com.forgekv.raft.Role;
import com.forgekv.statemachine.Command;
import com.forgekv.statemachine.CommandCodec;
import com.forgekv.statemachine.CommandType;
import com.forgekv.statemachine.PayloadHelper;
import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;

/**
 * gRPC service implementation for the Durable Job Queue.
 * Replicates all queue mutations through Raft consensus.
 */
public class QueueServiceImpl extends QueueServiceGrpc.QueueServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(QueueServiceImpl.class);

    private final RaftNode raftNode;

    public QueueServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void enqueue(EnqueueRequest request, StreamObserver<EnqueueResponse> responseObserver) {
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(EnqueueResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID reqId = parseOrCreateUuid(request.getRequestId());
        UUID jobId = UUID.randomUUID();
        long now = System.currentTimeMillis();
        long availableAt = now + Math.max(0, request.getDelayMs());
        int maxAttempts = request.getMaxAttempts() > 0 ? request.getMaxAttempts() : 3;

        Job job = new Job(
                jobId,
                request.getQueue(),
                request.getPayload().toByteArray(),
                JobStatus.READY,
                0,
                maxAttempts,
                availableAt,
                0L,
                null,
                request.getIdempotencyKey().isBlank() ? null : request.getIdempotencyKey(),
                null
        );

        Command cmd = new Command(reqId, CommandType.ENQUEUE_JOB, now, JobCodec.encode(job));
        raftNode.propose(CommandCodec.encode(cmd)).whenComplete((res, ex) -> {
            if (ex != null) {
                String leaderHint = ex instanceof NotLeaderException ? ((NotLeaderException) ex).getLeaderId() : raftNode.getLeaderId();
                responseObserver.onNext(EnqueueResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(nullToEmpty(leaderHint))
                        .setErrorMessage(ex.getMessage())
                        .build());
            } else {
                String assignedJobId = new String(res, StandardCharsets.UTF_8);
                responseObserver.onNext(EnqueueResponse.newBuilder()
                        .setSuccess(true)
                        .setJobId(assignedJobId)
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void claim(ClaimRequest request, StreamObserver<ClaimResponse> responseObserver) {
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(ClaimResponse.newBuilder()
                    .setFound(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID reqId = parseOrCreateUuid(request.getRequestId());
        long now = System.currentTimeMillis();
        long leaseDuration = request.getLeaseDurationMs() > 0 ? request.getLeaseDurationMs() : 10000L;
        long leaseUntil = now + leaseDuration;

        Command cmd = new Command(reqId, CommandType.CLAIM_JOB, now,
                PayloadHelper.encodeClaim(request.getQueue(), request.getWorkerId(), leaseUntil));

        raftNode.propose(CommandCodec.encode(cmd)).whenComplete((res, ex) -> {
            if (ex != null) {
                String leaderHint = ex instanceof NotLeaderException ? ((NotLeaderException) ex).getLeaderId() : raftNode.getLeaderId();
                responseObserver.onNext(ClaimResponse.newBuilder()
                        .setFound(false)
                        .setLeaderId(nullToEmpty(leaderHint))
                        .setErrorMessage(ex.getMessage())
                        .build());
            } else if (res.length == 0) {
                responseObserver.onNext(ClaimResponse.newBuilder()
                        .setFound(false)
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            } else {
                Job claimedJob = JobCodec.decode(res);
                responseObserver.onNext(ClaimResponse.newBuilder()
                        .setFound(true)
                        .setJob(toProto(claimedJob))
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void ack(AckRequest request, StreamObserver<AckResponse> responseObserver) {
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(AckResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID reqId = parseOrCreateUuid(request.getRequestId());
        Command cmd = new Command(reqId, CommandType.ACK_JOB, System.currentTimeMillis(),
                PayloadHelper.encodeAck(request.getJobId(), request.getWorkerId()));

        raftNode.propose(CommandCodec.encode(cmd)).whenComplete((res, ex) -> {
            if (ex != null) {
                responseObserver.onNext(AckResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                        .setErrorMessage(ex.getMessage())
                        .build());
            } else {
                boolean ok = res.length > 0 && res[0] == 1;
                responseObserver.onNext(AckResponse.newBuilder()
                        .setSuccess(ok)
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void nack(NackRequest request, StreamObserver<NackResponse> responseObserver) {
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(NackResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID reqId = parseOrCreateUuid(request.getRequestId());
        long now = System.currentTimeMillis();
        long retryDelay = 2000L; // default 2s retry delay
        long nextAvailable = now + retryDelay;

        Command cmd = new Command(reqId, CommandType.NACK_JOB, now,
                PayloadHelper.encodeNack(request.getJobId(), request.getWorkerId(), nextAvailable, request.getErrorMessage()));

        raftNode.propose(CommandCodec.encode(cmd)).whenComplete((res, ex) -> {
            if (ex != null) {
                responseObserver.onNext(NackResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                        .setErrorMessage(ex.getMessage())
                        .build());
            } else {
                boolean ok = res.length > 0 && res[0] == 1;
                responseObserver.onNext(NackResponse.newBuilder()
                        .setSuccess(ok)
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void extendLease(ExtendLeaseRequest request, StreamObserver<ExtendLeaseResponse> responseObserver) {
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(ExtendLeaseResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID reqId = parseOrCreateUuid(request.getRequestId());
        long now = System.currentTimeMillis();
        long extra = request.getAdditionalDurationMs() > 0 ? request.getAdditionalDurationMs() : 10000L;
        long newLease = now + extra;

        Command cmd = new Command(reqId, CommandType.EXTEND_LEASE, now,
                PayloadHelper.encodeExtendLease(request.getJobId(), request.getWorkerId(), newLease));

        raftNode.propose(CommandCodec.encode(cmd)).whenComplete((res, ex) -> {
            if (ex != null) {
                responseObserver.onNext(ExtendLeaseResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                        .setErrorMessage(ex.getMessage())
                        .build());
            } else {
                boolean ok = res.length > 0 && res[0] == 1;
                responseObserver.onNext(ExtendLeaseResponse.newBuilder()
                        .setSuccess(ok)
                        .setNewLeaseUntilEpochMs(newLease)
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getJob(GetJobRequest request, StreamObserver<GetJobResponse> responseObserver) {
        Optional<Job> jobOpt = raftNode.getStateMachine().getJob(request.getJobId());
        if (jobOpt.isPresent()) {
            responseObserver.onNext(GetJobResponse.newBuilder()
                    .setFound(true)
                    .setJob(toProto(jobOpt.get()))
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .build());
        } else {
            responseObserver.onNext(GetJobResponse.newBuilder()
                    .setFound(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .build());
        }
        responseObserver.onCompleted();
    }

    public static com.forgekv.queue.proto.Job toProto(Job j) {
        return com.forgekv.queue.proto.Job.newBuilder()
                .setId(j.id().toString())
                .setQueue(j.queue())
                .setPayload(ByteString.copyFrom(j.payload()))
                .setStatus(com.forgekv.queue.proto.JobStatus.forNumber(j.status().ordinal()))
                .setAttempts(j.attempts())
                .setMaxAttempts(j.maxAttempts())
                .setAvailableAtEpochMs(j.availableAtEpochMs())
                .setLeaseUntilEpochMs(j.leaseUntilEpochMs())
                .setWorkerId(nullToEmpty(j.workerId()))
                .setIdempotencyKey(nullToEmpty(j.idempotencyKey()))
                .setLastError(nullToEmpty(j.lastError()))
                .build();
    }

    private static UUID parseOrCreateUuid(String id) {
        if (id == null || id.isBlank()) {
            return UUID.randomUUID();
        }
        try {
            return UUID.fromString(id);
        } catch (Exception e) {
            return UUID.nameUUIDFromBytes(id.getBytes());
        }
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
