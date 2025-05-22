package com.Bitcask.Interface;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

public interface Bitcask {
    
    /**
     * Puts a key-value pair into the store.
     * @param key   the key
     * @param value the value
     * @throws IOException if write fails
     */
    void put(String key, String value) throws IOException;

    /**
     * Retrieves a value by key.
     * @param key the key
     * @return the value, or null if not found
     * @throws IOException if read fails
     */
    String get(String key) throws IOException;

    /**
     * Returns all keys in the store.
     * @return a set of keys
     */
    Set<String> listKeys();

    /**
     * Returns all key-value pairs in the store.
     * @return a map of key-value pairs
     */
    Map<String, String> getAll();

    /**
     * Closes the store and releases resources.
     */
    void close();

    /**
     * Forces flushing pending writes to disk (for durability).
     * @throws IOException if sync fails
     */
    void sync() throws IOException;

    /**
     * Triggers merge/compaction to remove stale data.
     * @throws IOException if merge fails
     */
    void merge() throws IOException;
}