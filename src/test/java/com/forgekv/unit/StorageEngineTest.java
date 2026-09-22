package com.forgekv.unit;

import com.forgekv.storage.RocksDBStorageEngine;
import com.forgekv.storage.StorageEngine;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class StorageEngineTest {

    @Test
    public void testFirstAcceptanceCriteria_PersistenceAndOperations(@TempDir File tempDir) {
        byte[] key = "user:1".getBytes(StandardCharsets.UTF_8);
        byte[] value = "Rohit".getBytes(StandardCharsets.UTF_8);

        // 1. Start server / open RocksDB
        try (StorageEngine engine = new RocksDBStorageEngine(tempDir)) {
            // 2. PUT user:1 = Rohit
            engine.put(key, value);

            // 3. GET user:1 -> Rohit
            Optional<byte[]> retrieved = engine.get(key);
            assertThat(retrieved).isPresent();
            assertThat(new String(retrieved.get(), StandardCharsets.UTF_8)).isEqualTo("Rohit");
            // 4. Stop server / close RocksDB
        }

        // 5. Restart using the same data directory
        try (StorageEngine engine = new RocksDBStorageEngine(tempDir)) {
            // 6. GET user:1 -> Rohit (verifies persistence across restart)
            Optional<byte[]> retrieved = engine.get(key);
            assertThat(retrieved).isPresent();
            assertThat(new String(retrieved.get(), StandardCharsets.UTF_8)).isEqualTo("Rohit");

            // 7. DELETE user:1
            engine.delete(key);

            // 8. GET user:1 -> NOT_FOUND
            Optional<byte[]> deletedRetrieved = engine.get(key);
            assertThat(deletedRetrieved).isEmpty();
        }
    }

    @Test
    public void testPrefixScan(@TempDir File tempDir) {
        try (StorageEngine engine = new RocksDBStorageEngine(tempDir)) {
            engine.put("raft/log/00000000000000000001".getBytes(StandardCharsets.UTF_8), "entry1".getBytes(StandardCharsets.UTF_8));
            engine.put("raft/log/00000000000000000002".getBytes(StandardCharsets.UTF_8), "entry2".getBytes(StandardCharsets.UTF_8));
            engine.put("kv/user1".getBytes(StandardCharsets.UTF_8), "data1".getBytes(StandardCharsets.UTF_8));

            List<Map.Entry<byte[], byte[]>> logEntries = engine.scanPrefix("raft/log/".getBytes(StandardCharsets.UTF_8));
            assertThat(logEntries).hasSize(2);
            assertThat(new String(logEntries.get(0).getValue(), StandardCharsets.UTF_8)).isEqualTo("entry1");
            assertThat(new String(logEntries.get(1).getValue(), StandardCharsets.UTF_8)).isEqualTo("entry2");
        }
    }
}
