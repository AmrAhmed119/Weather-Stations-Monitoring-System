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
    void put(int key, KeyDirValuePointer value) throws IOException;

    /**
     * Bulk loads a map of key-value pairs into the store.
     * @param newKeyDir the map of key-value pairs
     */
    void bulkLoad(Map<Integer, KeyDirValuePointer> newKeyDir) throws IOException;

    /**
     * Retrieves a value by key.
     * @param key the key
     * @return the value, or null if not found
     * @throws IOException if read fails
     */
    KeyDirValuePointer get(Integer key) throws IOException;

    /**
     * Returns all keys in the store.
     * @return a set of keys
     */
    Set<Integer> listKeys();

    /**
     * Returns all key-value pairs in the store.
     * @return a map of key-value pairs
     */
    Map<Integer, KeyDirValuePointer> getAll();

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