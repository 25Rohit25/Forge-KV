package com.forgekv.raft;

import com.forgekv.raft.proto.LogEntry;
import com.forgekv.storage.StorageEngine;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * RocksDB-backed persistent log for Raft consensus.
 * Formats log keys with fixed-width 20-digit zero-padded index to ensure
 * exact lexicographical sort order in RocksDB.
 */
public class RaftLog {

    private static final Logger log = LoggerFactory.getLogger(RaftLog.class);
    public static final String LOG_PREFIX = "raft/log/";

    private final StorageEngine storage;
    private final NavigableMap<Long, LogEntry> entries = new ConcurrentSkipListMap<>();
    private volatile long lastLogIndex = 0;
    private volatile long lastLogTerm = 0;

    public RaftLog(StorageEngine storage) {
        this.storage = Objects.requireNonNull(storage, "storage must not be null");
        loadFromStorage();
    }

    private void loadFromStorage() {
        List<Map.Entry<byte[], byte[]>> scanned = storage.scanPrefix(LOG_PREFIX.getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<byte[], byte[]> e : scanned) {
            try {
                LogEntry entry = LogEntry.parseFrom(e.getValue());
                entries.put(entry.getIndex(), entry);
                if (entry.getIndex() > lastLogIndex) {
                    lastLogIndex = entry.getIndex();
                    lastLogTerm = entry.getTerm();
                }
            } catch (Exception ex) {
                log.error("Failed to parse log entry from RocksDB", ex);
            }
        }
        log.info("Loaded {} Raft log entries from storage. lastLogIndex={}, lastLogTerm={}",
                entries.size(), lastLogIndex, lastLogTerm);
    }

    public synchronized LogEntry append(long term, byte[] command) {
        long newIndex = lastLogIndex + 1;
        LogEntry entry = LogEntry.newBuilder()
                .setIndex(newIndex)
                .setTerm(term)
                .setCommand(ByteString.copyFrom(command))
                .build();

        byte[] key = makeKey(newIndex);
        storage.put(key, entry.toByteArray());
        entries.put(newIndex, entry);

        lastLogIndex = newIndex;
        lastLogTerm = term;
        return entry;
    }

    public synchronized void appendEntry(LogEntry entry) {
        byte[] key = makeKey(entry.getIndex());
        storage.put(key, entry.toByteArray());
        entries.put(entry.getIndex(), entry);

        if (entry.getIndex() > lastLogIndex) {
            lastLogIndex = entry.getIndex();
            lastLogTerm = entry.getTerm();
        }
    }

    public synchronized void truncateSuffix(long fromIndex) {
        if (fromIndex > lastLogIndex) {
            return;
        }
        log.warn("Truncating conflicting log entries starting from index {}", fromIndex);
        NavigableMap<Long, LogEntry> toRemove = entries.tailMap(fromIndex, true);
        List<Long> keysToRemove = new ArrayList<>(toRemove.keySet());

        for (Long idx : keysToRemove) {
            byte[] key = makeKey(idx);
            storage.delete(key);
            entries.remove(idx);
        }

        if (entries.isEmpty()) {
            lastLogIndex = 0;
            lastLogTerm = 0;
        } else {
            lastLogIndex = entries.lastKey();
            lastLogTerm = entries.get(lastLogIndex).getTerm();
        }
    }

    public Optional<LogEntry> getEntry(long index) {
        return Optional.ofNullable(entries.get(index));
    }

    public long getTerm(long index) {
        if (index == 0) {
            return 0;
        }
        LogEntry entry = entries.get(index);
        return entry != null ? entry.getTerm() : 0;
    }

    public List<LogEntry> getEntriesFrom(long startIndex, int maxCount) {
        List<LogEntry> result = new ArrayList<>();
        NavigableMap<Long, LogEntry> tail = entries.tailMap(startIndex, true);
        int count = 0;
        for (LogEntry e : tail.values()) {
            result.add(e);
            count++;
            if (count >= maxCount) {
                break;
            }
        }
        return result;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public int size() {
        return entries.size();
    }

    public static byte[] makeKey(long index) {
        String keyStr = String.format("%s%020d", LOG_PREFIX, index);
        return keyStr.getBytes(StandardCharsets.UTF_8);
    }
}
