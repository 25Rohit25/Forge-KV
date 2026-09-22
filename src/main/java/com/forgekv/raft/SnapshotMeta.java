package com.forgekv.raft;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Snapshot metadata entity matching ForgeKV specification Section 12.1.
 */
public record SnapshotMeta(
        long lastIncludedIndex,
        long lastIncludedTerm,
        String checksum,
        long createdAt
) {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public byte[] toBytes() {
        try {
            return MAPPER.writeValueAsBytes(this);
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize SnapshotMeta", e);
        }
    }

    public static SnapshotMeta fromBytes(byte[] bytes) {
        try {
            return MAPPER.readValue(bytes, SnapshotMeta.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize SnapshotMeta", e);
        }
    }
}
