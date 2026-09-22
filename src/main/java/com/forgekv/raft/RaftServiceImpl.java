package com.forgekv.raft;

import com.forgekv.raft.proto.AppendEntriesArgs;
import com.forgekv.raft.proto.AppendEntriesReply;
import com.forgekv.raft.proto.RaftServiceGrpc;
import com.forgekv.raft.proto.RequestVoteArgs;
import com.forgekv.raft.proto.RequestVoteReply;
import io.grpc.stub.StreamObserver;

/**
 * gRPC service implementation for Raft inter-node consensus communication.
 */
public class RaftServiceImpl extends RaftServiceGrpc.RaftServiceImplBase {

    private final RaftNode raftNode;

    public RaftServiceImpl(RaftNode raftNode) {
        this.raftNode = raftNode;
    }

    @Override
    public void requestVote(RequestVoteArgs request, StreamObserver<RequestVoteReply> responseObserver) {
        raftNode.handleRequestVote(request).whenComplete((reply, ex) -> {
            if (ex != null) {
                responseObserver.onError(ex);
            } else {
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void appendEntries(AppendEntriesArgs request, StreamObserver<AppendEntriesReply> responseObserver) {
        raftNode.handleAppendEntries(request).whenComplete((reply, ex) -> {
            if (ex != null) {
                responseObserver.onError(ex);
            } else {
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });
    }

    @Override
    public void installSnapshot(com.forgekv.raft.proto.InstallSnapshotArgs request, StreamObserver<com.forgekv.raft.proto.InstallSnapshotReply> responseObserver) {
        raftNode.handleInstallSnapshot(request).whenComplete((reply, ex) -> {
            if (ex != null) {
                responseObserver.onError(ex);
            } else {
                responseObserver.onNext(reply);
                responseObserver.onCompleted();
            }
        });
    }
}
