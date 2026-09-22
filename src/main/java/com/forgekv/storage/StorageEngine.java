package com.forgekv.storage;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Storage abstraction for local persistent key-value storage.
 * Keeps RocksDB behind this interface to facilitate unit testing and decoupled design.
 */
public interface StorageEngine extends Closeable {

    void put(byte[] key, byte[] value);

    Optional<byte[]> get(byte[] key);

    void delete(byte[] key);

    List<Map.Entry<byte[], byte[]>> scanPrefix(byte[] prefix);

    void flush();

    @Override
    void close();
}
