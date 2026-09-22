package com.forgekv.unit;

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

public class StateMachineTest {

    @Test
    public void testKvOperationsAndDeduplication(@TempDir File tempDir) {
        try (StorageEngine storage = new RocksDBStorageEngine(tempDir)) {
            ForgeKVStateMachine fsm = new ForgeKVStateMachine(storage);

            UUID req1 = UUID.randomUUID();
            byte[] key = "counter".getBytes(StandardCharsets.UTF_8);
            byte[] value = "42".getBytes(StandardCharsets.UTF_8);

            Command putCmd = new Command(req1, CommandType.PUT, System.currentTimeMillis(),
                    PayloadHelper.encodePut(key, value));
            byte[] putEncoded = CommandCodec.encode(putCmd);

            // Apply PUT
            byte[] res1 = fsm.apply(putEncoded);
            assertThat(res1).containsExactly(1);

            // Verify KV in state machine
            Optional<byte[]> val = fsm.getKv(key);
            assertThat(val).isPresent();
            assertThat(new String(val.get(), StandardCharsets.UTF_8)).isEqualTo("42");

            // Apply duplicate request with same requestId
            byte[] resDup = fsm.apply(putEncoded);
            assertThat(resDup).containsExactly(1);

            // Delete command
            UUID req2 = UUID.randomUUID();
            Command delCmd = new Command(req2, CommandType.DELETE, System.currentTimeMillis(),
                    PayloadHelper.encodeDelete(key));
            fsm.apply(CommandCodec.encode(delCmd));

            assertThat(fsm.getKv(key)).isEmpty();
        }
    }
}
