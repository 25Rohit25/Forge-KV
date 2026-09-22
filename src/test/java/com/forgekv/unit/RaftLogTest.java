package com.forgekv.unit;

import com.forgekv.raft.RaftLog;
import com.forgekv.raft.proto.LogEntry;
import com.forgekv.storage.RocksDBStorageEngine;
import com.forgekv.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RaftLogTest {

    @Test
    public void testAppendAndTruncate(@TempDir File tempDir) {
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            RaftLog log = new RaftLog(storage);
            assertThat(log.getLastLogIndex()).isEqualTo(0);
            assertThat(log.getLastLogTerm()).isEqualTo(0);

            // Append 3 entries in term 1
            log.append(1, "cmd1".getBytes(StandardCharsets.UTF_8));
            log.append(1, "cmd2".getBytes(StandardCharsets.UTF_8));
            log.append(1, "cmd3".getBytes(StandardCharsets.UTF_8));

            assertThat(log.getLastLogIndex()).isEqualTo(3);
            assertThat(log.getLastLogTerm()).isEqualTo(1);
            assertThat(log.getTerm(2)).isEqualTo(1);

            // Truncate from index 2
            log.truncateSuffix(2);
            assertThat(log.getLastLogIndex()).isEqualTo(1);
            assertThat(log.getEntry(2)).isEmpty();
            assertThat(log.getEntry(1)).isPresent();

            // Append in term 2
            log.append(2, "cmd2-term2".getBytes(StandardCharsets.UTF_8));
            assertThat(log.getLastLogIndex()).isEqualTo(2);
            assertThat(log.getLastLogTerm()).isEqualTo(2);
        }

        // Test reload from storage across restart
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            RaftLog log = new RaftLog(storage);
            assertThat(log.getLastLogIndex()).isEqualTo(2);
            assertThat(log.getLastLogTerm()).isEqualTo(2);
            List<LogEntry> entries = log.getEntriesFrom(1, 10);
            assertThat(entries).hasSize(2);
            assertThat(entries.get(0).getCommand().toStringUtf8()).isEqualTo("cmd1");
            assertThat(entries.get(1).getCommand().toStringUtf8()).isEqualTo("cmd2-term2");
        }
    }
}
