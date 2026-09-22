package com.forgekv.queue;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Serializer and deserializer for Job persistence in RocksDB.
 */
public final class JobCodec {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JobCodec() {}

    public static byte[] encode(Job job) {
        try {
            return MAPPER.writeValueAsBytes(job);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize Job", e);
        }
    }

    public static Job decode(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, Job.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize Job", e);
        }
    }
}
