package com.forgekv.statemachine;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Deterministic serializer and deserializer for the Command envelope.
 */
public final class CommandCodec {

    private CommandCodec() {}

    public static byte[] encode(Command command) {
        byte[] payload = command.payload();
        ByteBuffer buffer = ByteBuffer.allocate(16 + 4 + 8 + 4 + payload.length);
        buffer.putLong(command.requestId().getMostSignificantBits());
        buffer.putLong(command.requestId().getLeastSignificantBits());
        buffer.putInt(command.type().ordinal());
        buffer.putLong(command.leaderTimestampEpochMs());
        buffer.putInt(payload.length);
        buffer.put(payload);
        return buffer.array();
    }

    public static Command decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        long mostSig = buffer.getLong();
        long leastSig = buffer.getLong();
        UUID requestId = new UUID(mostSig, leastSig);
        int typeOrdinal = buffer.getInt();
        CommandType type = CommandType.values()[typeOrdinal];
        long timestamp = buffer.getLong();
        int payloadLen = buffer.getInt();
        byte[] payload = new byte[payloadLen];
        buffer.get(payload);
        return new Command(requestId, type, timestamp, payload);
    }
}
