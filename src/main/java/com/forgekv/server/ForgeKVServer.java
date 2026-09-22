package com.forgekv.server;

import com.forgekv.kv.KeyValueServiceImpl;
import com.forgekv.metrics.MetricsManager;
import com.forgekv.queue.QueueServiceImpl;
import com.forgekv.raft.RaftNode;
import com.forgekv.raft.RaftServiceImpl;
import com.forgekv.statemachine.ForgeKVStateMachine;
import com.forgekv.storage.RocksDBStorageEngine;
import com.forgekv.storage.StorageEngine;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Main server daemon for a ForgeKV node.
 * Launches the gRPC server, initializes the storage engine and Raft consensus,
 * and manages Prometheus observability.
 */
public class ForgeKVServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(ForgeKVServer.class);

    private final ServerConfig config;
    private final StorageEngine storage;
    private final ForgeKVStateMachine stateMachine;
    private final RaftNode raftNode;
    private final Server grpcServer;
    private final MetricsManager metricsManager;

    public ForgeKVServer(ServerConfig config) {
        this.config = config;
        this.storage = new RocksDBStorageEngine(config.dataDir());
        this.stateMachine = new ForgeKVStateMachine(storage);
        this.raftNode = new RaftNode(config.nodeId(), storage, stateMachine, config.peerTargets());
        this.metricsManager = new MetricsManager(raftNode, config.metricsPort());

        this.grpcServer = ServerBuilder.forPort(config.port())
                .addService(new KeyValueServiceImpl(raftNode))
                .addService(new RaftServiceImpl(raftNode))
                .addService(new QueueServiceImpl(raftNode))
                .build();
    }

    public synchronized void start() throws IOException {
        log.info("Starting ForgeKV Server [nodeId={}, gRPC port={}, dataDir={}]",
                config.nodeId(), config.port(), config.dataDir().getAbsolutePath());

        raftNode.start();
        metricsManager.start();
        grpcServer.start();

        log.info("ForgeKV Node {} listening on port {}", config.nodeId(), config.port());
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (grpcServer != null) {
            grpcServer.awaitTermination();
        }
    }

    @Override
    public synchronized void close() {
        log.info("Shutting down ForgeKV Server [nodeId={}]", config.nodeId());
        try {
            if (grpcServer != null) {
                grpcServer.shutdown().awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            grpcServer.shutdownNow();
        }

        if (metricsManager != null) {
            metricsManager.close();
        }

        if (raftNode != null) {
            raftNode.close();
        }

        if (storage != null) {
            storage.close();
        }
        log.info("ForgeKV Server [nodeId={}] stopped cleanly", config.nodeId());
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public StorageEngine getStorage() {
        return storage;
    }

    public static void main(String[] args) throws Exception {
        ServerConfig cfg = ServerConfig.fromEnv();
        ForgeKVServer server = new ForgeKVServer(cfg);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Received shutdown signal from runtime");
            server.close();
        }));

        server.start();
        server.blockUntilShutdown();
    }
}
