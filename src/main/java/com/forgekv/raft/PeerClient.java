package com.forgekv.raft;

import com.forgekv.raft.proto.AppendEntriesArgs;
import com.forgekv.raft.proto.AppendEntriesReply;
import com.forgekv.raft.proto.RaftServiceGrpc;
import com.forgekv.raft.proto.RequestVoteArgs;
import com.forgekv.raft.proto.RequestVoteReply;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client representing a peer node in the Raft cluster.
 */
public class PeerClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PeerClient.class);

    private final String peerId;
    private final String target;
    private final ManagedChannel channel;
    private final RaftServiceGrpc.RaftServiceFutureStub stub;

    public PeerClient(String peerId, String target) {
        this.peerId = peerId;
        this.target = target;
        this.channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();
        this.stub = RaftServiceGrpc.newFutureStub(channel);
    }

    public CompletableFuture<RequestVoteReply> requestVote(RequestVoteArgs args, long timeoutMs) {
        CompletableFuture<RequestVoteReply> future = new CompletableFuture<>();
        try {
            ListenableFuture<RequestVoteReply> lf = stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .requestVote(args);
            Futures.addCallback(lf, new FutureCallback<>() {
                @Override
                public void onSuccess(RequestVoteReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    public CompletableFuture<AppendEntriesReply> appendEntries(AppendEntriesArgs args, long timeoutMs) {
        CompletableFuture<AppendEntriesReply> future = new CompletableFuture<>();
        try {
            ListenableFuture<AppendEntriesReply> lf = stub.withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .appendEntries(args);
            Futures.addCallback(lf, new FutureCallback<>() {
                @Override
                public void onSuccess(AppendEntriesReply result) {
                    future.complete(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, MoreExecutors.directExecutor());
        } catch (Exception e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    public String getPeerId() {
        return peerId;
    }

    public String getTarget() {
        return target;
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
