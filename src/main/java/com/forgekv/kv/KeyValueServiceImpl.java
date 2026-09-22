package com.forgekv.kv;

import com.forgekv.kv.proto.DeleteRequest;
import com.forgekv.kv.proto.DeleteResponse;
import com.forgekv.kv.proto.GetRequest;
import com.forgekv.kv.proto.GetResponse;
import com.forgekv.kv.proto.KeyValueServiceGrpc;
import com.forgekv.kv.proto.PutRequest;
import com.forgekv.kv.proto.PutResponse;
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

import java.util.Optional;
import java.util.UUID;

/**
 * KeyValue gRPC service handling client Put, Get, and Delete operations.
 * Implements leader verification and leader redirection hints.
 */
public class KeyValueServiceImpl extends KeyValueServiceGrpc.KeyValueServiceImplBase {

    private static final Logger log = LoggerFactory.getLogger(KeyValueServiceImpl.class);

    private final RaftNode raftNode;

    public KeyValueServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(PutResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID reqId = parseOrCreateUuid(request.getRequestId());
        Command cmd = new Command(reqId, CommandType.PUT, System.currentTimeMillis(),
                PayloadHelper.encodePut(request.getKey().toByteArray(), request.getValue().toByteArray()));

        raftNode.propose(CommandCodec.encode(cmd)).whenComplete((res, ex) -> {
            if (ex != null) {
                String leaderHint = ex instanceof NotLeaderException ? ((NotLeaderException) ex).getLeaderId() : raftNode.getLeaderId();
                responseObserver.onNext(PutResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(nullToEmpty(leaderHint))
                        .setErrorMessage(ex.getMessage())
                        .build());
            } else {
                responseObserver.onNext(PutResponse.newBuilder()
                        .setSuccess(true)
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            }
            responseObserver.onCompleted();
        });
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
        // Section 10.1: All strongly consistent GETs route to the leader
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(GetResponse.newBuilder()
                    .setFound(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        Optional<byte[]> val = raftNode.getStateMachine().getKv(request.getKey().toByteArray());
        if (val.isPresent()) {
            responseObserver.onNext(GetResponse.newBuilder()
                    .setFound(true)
                    .setValue(ByteString.copyFrom(val.get()))
                    .setLeaderId(raftNode.getNodeId())
                    .build());
        } else {
            responseObserver.onNext(GetResponse.newBuilder()
                    .setFound(false)
                    .setLeaderId(raftNode.getNodeId())
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
        if (raftNode.getRole() != Role.LEADER) {
            responseObserver.onNext(DeleteResponse.newBuilder()
                    .setSuccess(false)
                    .setLeaderId(nullToEmpty(raftNode.getLeaderId()))
                    .setErrorMessage("NOT_LEADER")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        UUID reqId = parseOrCreateUuid(request.getRequestId());
        Command cmd = new Command(reqId, CommandType.DELETE, System.currentTimeMillis(),
                PayloadHelper.encodeDelete(request.getKey().toByteArray()));

        raftNode.propose(CommandCodec.encode(cmd)).whenComplete((res, ex) -> {
            if (ex != null) {
                String leaderHint = ex instanceof NotLeaderException ? ((NotLeaderException) ex).getLeaderId() : raftNode.getLeaderId();
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setSuccess(false)
                        .setLeaderId(nullToEmpty(leaderHint))
                        .setErrorMessage(ex.getMessage())
                        .build());
            } else {
                responseObserver.onNext(DeleteResponse.newBuilder()
                        .setSuccess(true)
                        .setLeaderId(raftNode.getNodeId())
                        .build());
            }
            responseObserver.onCompleted();
        });
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
