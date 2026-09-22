package com.forgekv.storage;

import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * RocksDB implementation of StorageEngine adhering to ForgeKV specifications:
 * - Data directory is created prior to opening
 * - Opens with createIfMissing = true
 * - Wraps RocksDB checked exceptions into StorageException
 * - Closes Options and RocksDB instances cleanly
 * - Raw byte arrays used throughout
 */
public class RocksDBStorageEngine implements StorageEngine {

    private static final Logger log = LoggerFactory.getLogger(RocksDBStorageEngine.class);

    static {
        RocksDB.loadLibrary();
    }

    private final File dataDir;
    private final Options options;
    private final RocksDB db;
    private volatile boolean closed = false;

    public RocksDBStorageEngine(File dataDir) {
        this.dataDir = Objects.requireNonNull(dataDir, "dataDir must not be null");
        if (!dataDir.exists()) {
            boolean created = dataDir.mkdirs();
            if (!created && !dataDir.exists()) {
                throw new StorageException("Failed to create RocksDB data directory: " + dataDir.getAbsolutePath());
            }
        }

        this.options = new Options();
        this.options.setCreateIfMissing(true);

        try {
            this.db = RocksDB.open(this.options, this.dataDir.getAbsolutePath());
            log.info("RocksDB opened successfully at {}", this.dataDir.getAbsolutePath());
        } catch (RocksDBException e) {
            this.options.close();
            throw new StorageException("Failed to open RocksDB at " + dataDir.getAbsolutePath(), e);
        }
    }

    public RocksDBStorageEngine(String path) {
        this(new File(path));
    }

    @Override
    public void put(byte[] key, byte[] value) {
        ensureOpen();
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(value, "value must not be null");
        try {
            db.put(key, value);
        } catch (RocksDBException e) {
            throw new StorageException("Error putting key", e);
        }
    }

    @Override
    public Optional<byte[]> get(byte[] key) {
        ensureOpen();
        Objects.requireNonNull(key, "key must not be null");
        try {
            byte[] value = db.get(key);
            return Optional.ofNullable(value);
        } catch (RocksDBException e) {
            throw new StorageException("Error getting key", e);
        }
    }

    @Override
    public void delete(byte[] key) {
        ensureOpen();
        Objects.requireNonNull(key, "key must not be null");
        try {
            db.delete(key);
        } catch (RocksDBException e) {
            throw new StorageException("Error deleting key", e);
        }
    }

    @Override
    public List<Map.Entry<byte[], byte[]>> scanPrefix(byte[] prefix) {
        ensureOpen();
        Objects.requireNonNull(prefix, "prefix must not be null");
        List<Map.Entry<byte[], byte[]>> results = new ArrayList<>();

        try (RocksIterator iterator = db.newIterator()) {
            iterator.seek(prefix);
            while (iterator.isValid()) {
                byte[] key = iterator.key();
                if (!startsWith(key, prefix)) {
                    break;
                }
                byte[] value = iterator.value();
                results.add(new AbstractMap.SimpleImmutableEntry<>(key, value));
                iterator.next();
            }
        }
        return results;
    }

    @Override
    public void flush() {
        ensureOpen();
        try (FlushOptions flushOptions = new FlushOptions()) {
            flushOptions.setWaitForFlush(true);
            db.flush(flushOptions);
        } catch (RocksDBException e) {
            throw new StorageException("Error flushing RocksDB", e);
        }
    }

    @Override
    public synchronized void close() {
        if (!closed) {
            closed = true;
            try {
                db.close();
            } catch (Exception e) {
                log.warn("Error closing RocksDB database", e);
            }
            try {
                options.close();
            } catch (Exception e) {
                log.warn("Error closing RocksDB options", e);
            }
            log.info("RocksDB closed at {}", dataDir.getAbsolutePath());
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new StorageException("StorageEngine is closed");
        }
    }

    private static boolean startsWith(byte[] array, byte[] prefix) {
        if (array.length < prefix.length) {
            return false;
        }
        for (int i = 0; i < prefix.length; i++) {
            if (array[i] != prefix[i]) {
                return false;
            }
        }
        return true;
    }

    public File getDataDir() {
        return dataDir;
    }
}
