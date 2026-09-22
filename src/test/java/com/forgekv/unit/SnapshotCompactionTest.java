package com.forgekv.unit;

import com.forgekv.raft.RaftLog;
import com.forgekv.raft.RaftNode;
import com.forgekv.raft.Role;
import com.forgekv.raft.SnapshotMeta;
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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class SnapshotCompactionTest {

    @Test
    public void testSnapshotMetadataSerialization() {
        SnapshotMeta meta = new SnapshotMeta(100L, 5L, "sha256-test", 1700000000000L);
        byte[] bytes = meta.toBytes();
        SnapshotMeta deserialized = SnapshotMeta.fromBytes(bytes);

        assertThat(deserialized.lastIncludedIndex()).isEqualTo(100L);
        assertThat(deserialized.lastIncludedTerm()).isEqualTo(5L);
        assertThat(deserialized.checksum()).isEqualTo("sha256-test");
        assertThat(deserialized.createdAt()).isEqualTo(1700000000000L);
    }

    @Test
    public void testLogCompactionPrefixDiscard(@TempDir File tempDir) {
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            RaftLog log = new RaftLog(storage);

            // Append 10 entries in term 1
            for (int i = 1; i <= 10; i++) {
                log.append(1, ("cmd-" + i).getBytes(StandardCharsets.UTF_8));
            }
            assertThat(log.size()).isEqualTo(10);
            assertThat(log.getLastLogIndex()).isEqualTo(10);

            // Compact log up to index 6
            log.discardPrefix(6, 1);

            // Entries 1 through 6 must be removed from log
            assertThat(log.size()).isEqualTo(4);
            for (long idx = 1; idx <= 6; idx++) {
                assertThat(log.getEntry(idx)).isEmpty();
            }

            // Entries 7 through 10 must remain intact
            for (long idx = 7; idx <= 10; idx++) {
                assertThat(log.getEntry(idx)).isPresent();
            }

            // Historical term lookup for compacted boundary must still return the snapshot term
            assertThat(log.getTerm(6)).isEqualTo(1);
        }
    }

    @Test
    public void testRaftNodeSnapshotPersistenceAcrossRestart(@TempDir File tempDir) throws Exception {
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            ForgeKVStateMachine fsm = new ForgeKVStateMachine(storage);
            RaftNode node = new RaftNode("node1", storage, fsm, Map.of());
            node.start();

            // Wait for single node to elect itself leader
            long end = System.currentTimeMillis() + 3000;
            while (node.getRole() != Role.LEADER && System.currentTimeMillis() < end) {
                Thread.sleep(50);
            }
            assertThat(node.getRole()).isEqualTo(Role.LEADER);

            // Single node commits entries
            Command c1 = new Command(java.util.UUID.randomUUID(), com.forgekv.statemachine.CommandType.PUT, System.currentTimeMillis(),
                    com.forgekv.statemachine.PayloadHelper.encodePut("k1".getBytes(StandardCharsets.UTF_8), "v1".getBytes(StandardCharsets.UTF_8)));
            Command c2 = new Command(java.util.UUID.randomUUID(), com.forgekv.statemachine.CommandType.PUT, System.currentTimeMillis(),
                    com.forgekv.statemachine.PayloadHelper.encodePut("k2".getBytes(StandardCharsets.UTF_8), "v2".getBytes(StandardCharsets.UTF_8)));
            Command c3 = new Command(java.util.UUID.randomUUID(), com.forgekv.statemachine.CommandType.PUT, System.currentTimeMillis(),
                    com.forgekv.statemachine.PayloadHelper.encodePut("k3".getBytes(StandardCharsets.UTF_8), "v3".getBytes(StandardCharsets.UTF_8)));

            node.propose(com.forgekv.statemachine.CommandCodec.encode(c1)).get();
            node.propose(com.forgekv.statemachine.CommandCodec.encode(c2)).get();
            node.propose(com.forgekv.statemachine.CommandCodec.encode(c3)).get();

            assertThat(node.getCommitIndex()).isGreaterThanOrEqualTo(3);

            // Take snapshot through index 2
            boolean snapSuccess = node.takeSnapshot(2).get();
            assertThat(snapSuccess).isTrue();

            node.close();
        }

        // Restart node: must load snapshot metadata and resume from lastIncludedIndex
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            ForgeKVStateMachine fsm = new ForgeKVStateMachine(storage);
            RaftNode restartedNode = new RaftNode("node1", storage, fsm, Map.of());

            assertThat(restartedNode.getCommitIndex()).isEqualTo(2);
            assertThat(restartedNode.getLastApplied()).isEqualTo(2);
            assertThat(restartedNode.getRaftLog().getSnapshotLastIncludedIndex()).isEqualTo(2);
            restartedNode.close();
        }
    }
}
