package com.forgekv.statemachine;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/**
 * Deterministic command envelope replicated by Raft.
 * Includes requestId for deduplication and leader-assigned timestamp.
 */
public record Command(
        UUID requestId,
        CommandType type,
        long leaderTimestampEpochMs,
        byte[] payload
) {
    public Command {
        Objects.requireNonNull(requestId, "requestId must not be null");
        Objects.requireNonNull(type, "type must not be null");
        if (payload == null) {
            payload = new byte[0];
        }
    }

    public static Command of(UUID requestId, CommandType type, byte[] payload) {
        return new Command(requestId, type, System.currentTimeMillis(), payload);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Command other)) return false;
        return leaderTimestampEpochMs == other.leaderTimestampEpochMs &&
                requestId.equals(other.requestId) &&
                type == other.type &&
                Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(requestId, type, leaderTimestampEpochMs);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
