package com.forgekv.raft;

import com.forgekv.queue.Job;
import com.forgekv.queue.JobStatus;
import com.forgekv.raft.proto.AppendEntriesArgs;
import com.forgekv.raft.proto.AppendEntriesReply;
import com.forgekv.raft.proto.LogEntry;
import com.forgekv.raft.proto.RequestVoteArgs;
import com.forgekv.raft.proto.RequestVoteReply;
import com.forgekv.statemachine.Command;
import com.forgekv.statemachine.CommandCodec;
import com.forgekv.statemachine.CommandType;
import com.forgekv.statemachine.ForgeKVStateMachine;
import com.forgekv.statemachine.PayloadHelper;
import com.forgekv.storage.StorageEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Raft Consensus Engine implementing leader election, log replication,
 * quorum commits, and persistent recovery.
 *
 * All state mutations are strictly serialized on an internal event-loop executor.
 */
public class RaftNode implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    public static final String KEY_CURRENT_TERM = "meta/currentTerm";
    public static final String KEY_VOTED_FOR = "meta/votedFor";

    private final String nodeId;
    private final StorageEngine storage;
    private final RaftLog raftLog;
    private final ForgeKVStateMachine stateMachine;
    private final Map<String, PeerClient> peers = new ConcurrentHashMap<>();
    private final int clusterSize;

    // Persistent state
    private long currentTerm = 0;
    private String votedFor = null;

    // Volatile state on all servers
    private long commitIndex = 0;
    private long lastApplied = 0;
    private Role role = Role.FOLLOWER;
    private String leaderId = null;

    // Leader-only volatile state
    private final Map<String, Long> nextIndex = new HashMap<>();
    private final Map<String, Long> matchIndex = new HashMap<>();
    private final Map<Long, CompletableFuture<byte[]>> pendingFutures = new ConcurrentHashMap<>();

    // Timers & Execution
    private final ScheduledExecutorService raftExecutor;
    private final Random random = new Random();
    private final long minElectionTimeoutMs;
    private final long maxElectionTimeoutMs;
    private final long heartbeatIntervalMs;
    private volatile long electionDeadline;
    private ScheduledFuture<?> electionCheckTask;
    private ScheduledFuture<?> heartbeatTask;
    private ScheduledFuture<?> leaseScannerTask;

    // Metrics counters
    private final AtomicLong leaderChanges = new AtomicLong(0);

    public RaftNode(String nodeId,
                    StorageEngine storage,
                    ForgeKVStateMachine stateMachine,
                    Map<String, String> peerTargets,
                    long minElectionTimeoutMs,
                    long maxElectionTimeoutMs,
                    long heartbeatIntervalMs) {

        this.nodeId = Objects.requireNonNull(nodeId);
        this.storage = Objects.requireNonNull(storage);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.raftLog = new RaftLog(storage);
        this.minElectionTimeoutMs = minElectionTimeoutMs;
        this.maxElectionTimeoutMs = maxElectionTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;

        for (Map.Entry<String, String> entry : peerTargets.entrySet()) {
            if (!entry.getKey().equals(nodeId)) {
                peers.put(entry.getKey(), new PeerClient(entry.getKey(), entry.getValue()));
            }
        }
        this.clusterSize = peers.size() + 1;

        // Dedicated single-thread event loop for serialized state transitions
        this.raftExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "raft-node-" + nodeId);
            t.setDaemon(true);
            return t;
        });

        loadPersistentMeta();
    }

    public RaftNode(String nodeId, StorageEngine storage, ForgeKVStateMachine stateMachine, Map<String, String> peerTargets) {
        this(nodeId, storage, stateMachine, peerTargets, 350, 650, 100);
    }

    private void loadPersistentMeta() {
        Optional<byte[]> termBytes = storage.get(KEY_CURRENT_TERM.getBytes(StandardCharsets.UTF_8));
        termBytes.ifPresent(bytes -> this.currentTerm = Long.parseLong(new String(bytes, StandardCharsets.UTF_8)));

        Optional<byte[]> voteBytes = storage.get(KEY_VOTED_FOR.getBytes(StandardCharsets.UTF_8));
        voteBytes.ifPresent(bytes -> this.votedFor = new String(bytes, StandardCharsets.UTF_8));

        log.info("Node {} initialized. Loaded persistent metadata: term={}, votedFor={}, logSize={}",
                nodeId, currentTerm, votedFor, raftLog.size());
    }

    public synchronized void start() {
        log.info("Starting RaftNode {}", nodeId);
        resetElectionDeadline();

        // Check election timeout periodically
        electionCheckTask = raftExecutor.scheduleWithFixedDelay(
                () -> checkElectionTimeout(),
                50, 50, TimeUnit.MILLISECONDS
        );

        // Heartbeat timer (fires only when LEADER)
        heartbeatTask = raftExecutor.scheduleWithFixedDelay(
                () -> sendHeartbeatsIfLeader(),
                heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS
        );

        // Queue lease expiry scanner (runs only when LEADER)
        leaseScannerTask = raftExecutor.scheduleWithFixedDelay(
                () -> scanQueueLeasesIfLeader(),
                100, 100, TimeUnit.MILLISECONDS
        );
    }

    private void resetElectionDeadline() {
        long timeout = minElectionTimeoutMs + random.nextInt((int) (maxElectionTimeoutMs - minElectionTimeoutMs + 1));
        this.electionDeadline = System.currentTimeMillis() + timeout;
    }

    private void checkElectionTimeout() {
        if (role != Role.LEADER && System.currentTimeMillis() >= electionDeadline) {
            startElection();
        }
    }

    private void startElection() {
        currentTerm++;
        role = Role.CANDIDATE;
        votedFor = nodeId;
        persistMeta();
        resetElectionDeadline();
        leaderChanges.incrementAndGet();

        log.info("{\"event\":\"election_started\",\"node\":\"{}\",\"term\":{}}", nodeId, currentTerm);

        if (clusterSize == 1) {
            becomeLeader();
            return;
        }

        final long electionTerm = currentTerm;
        final long lastLogIndex = raftLog.getLastLogIndex();
        final long lastLogTerm = raftLog.getLastLogTerm();

        RequestVoteArgs args = RequestVoteArgs.newBuilder()
                .setTerm(electionTerm)
                .setCandidateId(nodeId)
                .setLastLogIndex(lastLogIndex)
                .setLastLogTerm(lastLogTerm)
                .build();

        AtomicLong votes = new AtomicLong(1); // Vote for self

        for (PeerClient peer : peers.values()) {
            peer.requestVote(args, minElectionTimeoutMs).thenAcceptAsync(reply -> {
                raftExecutor.execute(() -> {
                    if (role != Role.CANDIDATE || currentTerm != electionTerm) {
                        return;
                    }
                    if (reply.getTerm() > currentTerm) {
                        stepDown(reply.getTerm());
                        return;
                    }
                    if (reply.getVoteGranted() && reply.getTerm() == currentTerm) {
                        long count = votes.incrementAndGet();
                        if (count >= majority()) {
                            becomeLeader();
                        }
                    }
                });
            }, raftExecutor).exceptionally(ex -> null);
        }
    }

    private void becomeLeader() {
        if (role == Role.LEADER) return;
        role = Role.LEADER;
        leaderId = nodeId;
        log.info("{\"event\":\"became_leader\",\"node\":\"{}\",\"term\":{}}", nodeId, currentTerm);

        long next = raftLog.getLastLogIndex() + 1;
        for (String peerId : peers.keySet()) {
            nextIndex.put(peerId, next);
            matchIndex.put(peerId, 0L);
        }

        sendAppendEntriesToAll();

        // Standard Raft leader initialization: commit a no-op entry in current term to commit prior entries
        Command noOp = new Command(UUID.randomUUID(), CommandType.CAS, System.currentTimeMillis(), new byte[0]);
        propose(CommandCodec.encode(noOp));
    }

    private void stepDown(long newTerm) {
        log.info("Node {} stepping down to FOLLOWER from term {} to {}", nodeId, currentTerm, newTerm);
        currentTerm = newTerm;
        role = Role.FOLLOWER;
        votedFor = null;
        persistMeta();
        resetElectionDeadline();

        // Fail any pending client requests
        for (CompletableFuture<byte[]> f : pendingFutures.values()) {
            f.completeExceptionally(new NotLeaderException(leaderId));
        }
        pendingFutures.clear();
    }

    private void sendHeartbeatsIfLeader() {
        if (role == Role.LEADER) {
            sendAppendEntriesToAll();
        }
    }

    private void sendAppendEntriesToAll() {
        for (PeerClient peer : peers.values()) {
            sendAppendEntriesToPeer(peer);
        }
    }

    private void sendAppendEntriesToPeer(PeerClient peer) {
        if (role != Role.LEADER) return;

        String peerId = peer.getPeerId();
        long prevIndex = nextIndex.getOrDefault(peerId, raftLog.getLastLogIndex() + 1) - 1;
        long prevTerm = raftLog.getTerm(prevIndex);

        List<LogEntry> entriesToSend = raftLog.getEntriesFrom(prevIndex + 1, 100);

        AppendEntriesArgs args = AppendEntriesArgs.newBuilder()
                .setTerm(currentTerm)
                .setLeaderId(nodeId)
                .setPrevLogIndex(prevIndex)
                .setPrevLogTerm(prevTerm)
                .addAllEntries(entriesToSend)
                .setLeaderCommit(commitIndex)
                .build();

        long sentTerm = currentTerm;

        peer.appendEntries(args, heartbeatIntervalMs * 2).thenAcceptAsync(reply -> {
            raftExecutor.execute(() -> {
                if (role != Role.LEADER || currentTerm != sentTerm) {
                    return;
                }
                if (reply.getTerm() > currentTerm) {
                    stepDown(reply.getTerm());
                    return;
                }
                if (reply.getSuccess()) {
                    long matched = reply.getMatchIndex();
                    matchIndex.put(peerId, Math.max(matchIndex.getOrDefault(peerId, 0L), matched));
                    nextIndex.put(peerId, matched + 1);
                    checkAndUpdateCommit();
                } else {
                    // Backtrack nextIndex on rejection
                    long currentNext = nextIndex.getOrDefault(peerId, 1L);
                    if (currentNext > 1) {
                        nextIndex.put(peerId, currentNext - 1);
                        sendAppendEntriesToPeer(peer);
                    }
                }
            });
        }, raftExecutor).exceptionally(ex -> null);
    }

    private void checkAndUpdateCommit() {
        if (role != Role.LEADER) return;

        for (long N = raftLog.getLastLogIndex(); N > commitIndex; N--) {
            // Raft commit rule: entry must belong to current term
            if (raftLog.getTerm(N) != currentTerm) {
                continue;
            }
            int replicated = 1; // leader itself
            for (String peerId : peers.keySet()) {
                if (matchIndex.getOrDefault(peerId, 0L) >= N) {
                    replicated++;
                }
            }
            if (replicated >= majority()) {
                commitIndex = N;
                applyEntries();
                sendAppendEntriesToAll();
                break;
            }
        }
    }

    private void applyEntries() {
        while (lastApplied < commitIndex) {
            lastApplied++;
            long idx = lastApplied;
            Optional<LogEntry> entryOpt = raftLog.getEntry(idx);
            if (entryOpt.isPresent()) {
                LogEntry entry = entryOpt.get();
                byte[] result = stateMachine.apply(entry.getCommand().toByteArray());
                log.info("{\"event\":\"entry_committed\",\"index\":{},\"term\":{}}", idx, entry.getTerm());

                CompletableFuture<byte[]> future = pendingFutures.remove(idx);
                if (future != null) {
                    future.complete(result);
                }
            }
        }
    }

    // Inbound RequestVote RPC handler
    public CompletableFuture<RequestVoteReply> handleRequestVote(RequestVoteArgs req) {
        CompletableFuture<RequestVoteReply> future = new CompletableFuture<>();
        raftExecutor.execute(() -> {
            if (req.getTerm() > currentTerm) {
                stepDown(req.getTerm());
            }

            boolean voteGranted = false;
            if (req.getTerm() == currentTerm) {
                boolean canVote = (votedFor == null || votedFor.equals(req.getCandidateId()));
                boolean upToDate = isLogUpToDate(req.getLastLogTerm(), req.getLastLogIndex());

                if (canVote && upToDate) {
                    voteGranted = true;
                    votedFor = req.getCandidateId();
                    persistMeta();
                    resetElectionDeadline();
                }
            }

            RequestVoteReply reply = RequestVoteReply.newBuilder()
                    .setTerm(currentTerm)
                    .setVoteGranted(voteGranted)
                    .build();
            future.complete(reply);
        });
        return future;
    }

    private boolean isLogUpToDate(long candidateLastTerm, long candidateLastIndex) {
        long myLastTerm = raftLog.getLastLogTerm();
        long myLastIndex = raftLog.getLastLogIndex();

        if (candidateLastTerm != myLastTerm) {
            return candidateLastTerm > myLastTerm;
        }
        return candidateLastIndex >= myLastIndex;
    }

    // Inbound AppendEntries RPC handler
    public CompletableFuture<AppendEntriesReply> handleAppendEntries(AppendEntriesArgs req) {
        CompletableFuture<AppendEntriesReply> future = new CompletableFuture<>();
        raftExecutor.execute(() -> {
            if (req.getTerm() > currentTerm) {
                stepDown(req.getTerm());
            }

            if (req.getTerm() < currentTerm) {
                future.complete(AppendEntriesReply.newBuilder()
                        .setTerm(currentTerm)
                        .setSuccess(false)
                        .setMatchIndex(raftLog.getLastLogIndex())
                        .build());
                return;
            }

            // Valid current leader
            if (role != Role.FOLLOWER) {
                role = Role.FOLLOWER;
            }
            leaderId = req.getLeaderId();
            resetElectionDeadline();

            // Log consistency check at prevLogIndex
            long prevIndex = req.getPrevLogIndex();
            long prevTerm = req.getPrevLogTerm();

            if (prevIndex > 0) {
                if (raftLog.getLastLogIndex() < prevIndex) {
                    future.complete(AppendEntriesReply.newBuilder()
                            .setTerm(currentTerm)
                            .setSuccess(false)
                            .setMatchIndex(raftLog.getLastLogIndex())
                            .build());
                    return;
                }
                if (raftLog.getTerm(prevIndex) != prevTerm) {
                    // Conflicting log entry at prevIndex: delete suffix
                    raftLog.truncateSuffix(prevIndex);
                    future.complete(AppendEntriesReply.newBuilder()
                            .setTerm(currentTerm)
                            .setSuccess(false)
                            .setMatchIndex(raftLog.getLastLogIndex())
                            .build());
                    return;
                }
            }

            // Append any new entries
            for (LogEntry newEntry : req.getEntriesList()) {
                Optional<LogEntry> existing = raftLog.getEntry(newEntry.getIndex());
                if (existing.isPresent()) {
                    if (existing.get().getTerm() != newEntry.getTerm()) {
                        raftLog.truncateSuffix(newEntry.getIndex());
                        raftLog.appendEntry(newEntry);
                    }
                } else {
                    raftLog.appendEntry(newEntry);
                }
            }

            // Advance follower commit index
            if (req.getLeaderCommit() > commitIndex) {
                commitIndex = Math.min(req.getLeaderCommit(), raftLog.getLastLogIndex());
                applyEntries();
            }

            future.complete(AppendEntriesReply.newBuilder()
                    .setTerm(currentTerm)
                    .setSuccess(true)
                    .setMatchIndex(raftLog.getLastLogIndex())
                    .build());
        });
        return future;
    }

    // Client write proposal
    public CompletableFuture<byte[]> propose(byte[] serializedCommand) {
        CompletableFuture<byte[]> future = new CompletableFuture<>();
        raftExecutor.execute(() -> {
            if (role != Role.LEADER) {
                future.completeExceptionally(new NotLeaderException(leaderId));
                return;
            }

            LogEntry entry = raftLog.append(currentTerm, serializedCommand);
            pendingFutures.put(entry.getIndex(), future);

            if (clusterSize == 1) {
                commitIndex = entry.getIndex();
                applyEntries();
            } else {
                sendAppendEntriesToAll();
            }
        });
        return future;
    }

    // Periodic scanner on leader for queue leases
    private void scanQueueLeasesIfLeader() {
        if (role != Role.LEADER) return;

        long now = System.currentTimeMillis();
        List<Job> allJobs = stateMachine.getAllJobs();

        for (Job job : allJobs) {
            if (job.status() == JobStatus.PROCESSING && job.leaseUntilEpochMs() > 0 && job.leaseUntilEpochMs() <= now) {
                log.info("Queue lease expired for job {}, attempts={}/{}", job.id(), job.attempts(), job.maxAttempts());
                int nextAttempt = job.attempts() + 1;
                if (nextAttempt >= job.maxAttempts()) {
                    Command dlqCmd = new Command(UUID.randomUUID(), CommandType.MOVE_TO_DLQ, now,
                            PayloadHelper.encodeMoveToDlq(job.id().toString(), "Lease expired and max attempts reached"));
                    propose(CommandCodec.encode(dlqCmd));
                } else {
                    long nextAvailableAt = now;
                    Command requeueCmd = new Command(UUID.randomUUID(), CommandType.REQUEUE_JOB, now,
                            PayloadHelper.encodeRequeue(job.id().toString(), nextAvailableAt, "Lease expired, requeued for claim"));
                    propose(CommandCodec.encode(requeueCmd));
                }
            }
        }
    }

    private void persistMeta() {
        storage.put(KEY_CURRENT_TERM.getBytes(StandardCharsets.UTF_8),
                String.valueOf(currentTerm).getBytes(StandardCharsets.UTF_8));
        if (votedFor != null) {
            storage.put(KEY_VOTED_FOR.getBytes(StandardCharsets.UTF_8),
                    votedFor.getBytes(StandardCharsets.UTF_8));
        } else {
            storage.delete(KEY_VOTED_FOR.getBytes(StandardCharsets.UTF_8));
        }
    }

    private int majority() {
        return (clusterSize / 2) + 1;
    }

    public String getNodeId() { return nodeId; }
    public Role getRole() { return role; }
    public long getCurrentTerm() { return currentTerm; }
    public String getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public long getLastLogIndex() { return raftLog.getLastLogIndex(); }
    public long getLeaderChanges() { return leaderChanges.get(); }
    public ForgeKVStateMachine getStateMachine() { return stateMachine; }
    public RaftLog getRaftLog() { return raftLog; }
    public ScheduledExecutorService getExecutor() { return raftExecutor; }

    @Override
    public synchronized void close() {
        log.info("Closing RaftNode {}", nodeId);
        if (electionCheckTask != null) electionCheckTask.cancel(true);
        if (heartbeatTask != null) heartbeatTask.cancel(true);
        if (leaseScannerTask != null) leaseScannerTask.cancel(true);

        for (PeerClient peer : peers.values()) {
            peer.close();
        }

        raftExecutor.shutdownNow();
    }
}
