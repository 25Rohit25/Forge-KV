package com.forgekv.statemachine;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public final class PayloadHelper {

    private PayloadHelper() {}

    public static byte[] encodePut(byte[] key, byte[] value) {
        ByteBuffer buf = ByteBuffer.allocate(4 + key.length + 4 + value.length);
        buf.putInt(key.length);
        buf.put(key);
        buf.putInt(value.length);
        buf.put(value);
        return buf.array();
    }

    public record PutPayload(byte[] key, byte[] value) {}

    public static PutPayload decodePut(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int keyLen = buf.getInt();
        byte[] key = new byte[keyLen];
        buf.get(key);
        int valLen = buf.getInt();
        byte[] value = new byte[valLen];
        buf.get(value);
        return new PutPayload(key, value);
    }

    public static byte[] encodeDelete(byte[] key) {
        return key;
    }

    public static byte[] decodeDelete(byte[] payload) {
        return payload;
    }

    public static byte[] encodeClaim(String queue, String workerId, long leaseUntilEpochMs) {
        byte[] qBytes = queue.getBytes(StandardCharsets.UTF_8);
        byte[] wBytes = workerId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + qBytes.length + 4 + wBytes.length + 8);
        buf.putInt(qBytes.length);
        buf.put(qBytes);
        buf.putInt(wBytes.length);
        buf.put(wBytes);
        buf.putLong(leaseUntilEpochMs);
        return buf.array();
    }

    public record ClaimPayload(String queue, String workerId, long leaseUntilEpochMs) {}

    public static ClaimPayload decodeClaim(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int qLen = buf.getInt();
        byte[] qBytes = new byte[qLen];
        buf.get(qBytes);
        int wLen = buf.getInt();
        byte[] wBytes = new byte[wLen];
        buf.get(wBytes);
        long leaseUntil = buf.getLong();
        return new ClaimPayload(new String(qBytes, StandardCharsets.UTF_8), new String(wBytes, StandardCharsets.UTF_8), leaseUntil);
    }

    public static byte[] encodeAck(String jobId, String workerId) {
        byte[] jBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] wBytes = workerId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + jBytes.length + 4 + wBytes.length);
        buf.putInt(jBytes.length);
        buf.put(jBytes);
        buf.putInt(wBytes.length);
        buf.put(wBytes);
        return buf.array();
    }

    public record AckPayload(String jobId, String workerId) {}

    public static AckPayload decodeAck(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int jLen = buf.getInt();
        byte[] jBytes = new byte[jLen];
        buf.get(jBytes);
        int wLen = buf.getInt();
        byte[] wBytes = new byte[wLen];
        buf.get(wBytes);
        return new AckPayload(new String(jBytes, StandardCharsets.UTF_8), new String(wBytes, StandardCharsets.UTF_8));
    }

    public static byte[] encodeNack(String jobId, String workerId, long nextAvailableAtEpochMs, String error) {
        byte[] jBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] wBytes = workerId.getBytes(StandardCharsets.UTF_8);
        byte[] eBytes = (error != null ? error : "").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + jBytes.length + 4 + wBytes.length + 8 + 4 + eBytes.length);
        buf.putInt(jBytes.length);
        buf.put(jBytes);
        buf.putInt(wBytes.length);
        buf.put(wBytes);
        buf.putLong(nextAvailableAtEpochMs);
        buf.putInt(eBytes.length);
        buf.put(eBytes);
        return buf.array();
    }

    public record NackPayload(String jobId, String workerId, long nextAvailableAtEpochMs, String error) {}

    public static NackPayload decodeNack(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int jLen = buf.getInt();
        byte[] jBytes = new byte[jLen];
        buf.get(jBytes);
        int wLen = buf.getInt();
        byte[] wBytes = new byte[wLen];
        buf.get(wBytes);
        long nextAt = buf.getLong();
        int eLen = buf.getInt();
        byte[] eBytes = new byte[eLen];
        buf.get(eBytes);
        return new NackPayload(new String(jBytes, StandardCharsets.UTF_8), new String(wBytes, StandardCharsets.UTF_8), nextAt, new String(eBytes, StandardCharsets.UTF_8));
    }

    public static byte[] encodeExtendLease(String jobId, String workerId, long newLeaseUntilEpochMs) {
        byte[] jBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] wBytes = workerId.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + jBytes.length + 4 + wBytes.length + 8);
        buf.putInt(jBytes.length);
        buf.put(jBytes);
        buf.putInt(wBytes.length);
        buf.put(wBytes);
        buf.putLong(newLeaseUntilEpochMs);
        return buf.array();
    }

    public record ExtendLeasePayload(String jobId, String workerId, long newLeaseUntilEpochMs) {}

    public static ExtendLeasePayload decodeExtendLease(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int jLen = buf.getInt();
        byte[] jBytes = new byte[jLen];
        buf.get(jBytes);
        int wLen = buf.getInt();
        byte[] wBytes = new byte[wLen];
        buf.get(wBytes);
        long newLease = buf.getLong();
        return new ExtendLeasePayload(new String(jBytes, StandardCharsets.UTF_8), new String(wBytes, StandardCharsets.UTF_8), newLease);
    }

    public static byte[] encodeRequeue(String jobId, long nextAvailableAtEpochMs, String reason) {
        byte[] jBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] rBytes = (reason != null ? reason : "").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + jBytes.length + 8 + 4 + rBytes.length);
        buf.putInt(jBytes.length);
        buf.put(jBytes);
        buf.putLong(nextAvailableAtEpochMs);
        buf.putInt(rBytes.length);
        buf.put(rBytes);
        return buf.array();
    }

    public record RequeuePayload(String jobId, long nextAvailableAtEpochMs, String reason) {}

    public static RequeuePayload decodeRequeue(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int jLen = buf.getInt();
        byte[] jBytes = new byte[jLen];
        buf.get(jBytes);
        long nextAt = buf.getLong();
        int rLen = buf.getInt();
        byte[] rBytes = new byte[rLen];
        buf.get(rBytes);
        return new RequeuePayload(new String(jBytes, StandardCharsets.UTF_8), nextAt, new String(rBytes, StandardCharsets.UTF_8));
    }

    public static byte[] encodeMoveToDlq(String jobId, String reason) {
        byte[] jBytes = jobId.getBytes(StandardCharsets.UTF_8);
        byte[] rBytes = (reason != null ? reason : "").getBytes(StandardCharsets.UTF_8);
        ByteBuffer buf = ByteBuffer.allocate(4 + jBytes.length + 4 + rBytes.length);
        buf.putInt(jBytes.length);
        buf.put(jBytes);
        buf.putInt(rBytes.length);
        buf.put(rBytes);
        return buf.array();
    }

    public record MoveToDlqPayload(String jobId, String reason) {}

    public static MoveToDlqPayload decodeMoveToDlq(byte[] payload) {
        ByteBuffer buf = ByteBuffer.wrap(payload);
        int jLen = buf.getInt();
        byte[] jBytes = new byte[jLen];
        buf.get(jBytes);
        int rLen = buf.getInt();
        byte[] rBytes = new byte[rLen];
        buf.get(rBytes);
        return new MoveToDlqPayload(new String(jBytes, StandardCharsets.UTF_8), new String(rBytes, StandardCharsets.UTF_8));
    }
}
