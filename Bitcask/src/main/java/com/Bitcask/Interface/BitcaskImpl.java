package com.Bitcask.Interface;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * it's simply a map of key-value pairs operating on it by multiple users.
 */


class BitcaskImpl implements Bitcask {
    public static int FileSize = 100;
    private static BitcaskImpl bitcaskObj = null;
    private final Map<Integer, KeyDirValuePointer> keydir = new ConcurrentHashMap<>();
    
    private BitcaskImpl(boolean isWriter) throws IOException {
    }

    public static BitcaskImpl getInstance(boolean isWriter) throws IOException {
        if (bitcaskObj == null) {
            bitcaskObj = new BitcaskImpl(isWriter);
        } else if (isWriter) {
            throw new IllegalStateException("Bitcask already opened; cannot open again for writing.");
        }
        return bitcaskObj;
    }

    public static BitcaskImpl getMergerInstance() throws IOException {
        return bitcaskObj;
    }

    @Override
    public void put(int key, KeyDirValuePointer value) throws IOException {
        keydir.put(key, value);
    }

    @Override
    public KeyDirValuePointer get(Integer key) throws IOException {
        return keydir.get(key);
    }

    @Override
    public Set<Integer> listKeys() {
        return keydir.keySet();
    }

    @Override
    public Map<Integer, KeyDirValuePointer> getAll() {
        return new ConcurrentHashMap<>(keydir);
    }

    @Override
    public void close() {
    }

    @Override
    public void sync() throws IOException {}
    

    @Override
    public void merge() throws IOException {
        // Implement file compaction logic - simplified version
        System.out.println("Merge operation not yet implemented");
    }

    public static Integer getNumOfSegments(){
        return 5;
    }
}


