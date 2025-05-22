package com.Bitcask.Interface;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class BitcaskImpl implements Bitcask {
    private static BitcaskImpl instance = null;
    private final Map<String, String> keydir = new ConcurrentHashMap<>();
    private final Path directoryPath;
    private final FileChannel activeFile;
    private FileLock lock;
    
    private BitcaskImpl(Path path, boolean isWriter) throws IOException {
        this.directoryPath = path;
        Files.createDirectories(path);
        
        // Open active file for writing
        Path activePath = path.resolve("active.data");
        if (isWriter) {
            this.activeFile = FileChannel.open(activePath, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.WRITE, 
                StandardOpenOption.READ);
            this.lock = this.activeFile.tryLock();
            if (this.lock == null) {
                throw new IOException("Cannot acquire lock - another writer exists");
            }
        } else {
            this.activeFile = FileChannel.open(activePath, 
                StandardOpenOption.CREATE, 
                StandardOpenOption.READ);
        }
        
        // Load existing data
        loadKeydir(path);
    }

    private void loadKeydir(Path path) throws IOException {
        Path activePath = path.resolve("active.data");
        if (Files.exists(activePath)) {
            try (BufferedReader reader = Files.newBufferedReader(activePath)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split(":", 2);
                    if (parts.length == 2) {
                        keydir.put(parts[0], parts[1]);
                    }
                }
            }
        }
    }

    public static synchronized BitcaskImpl getInstance(Path path, boolean isWriter) throws IOException {
        if (instance == null) {
            instance = new BitcaskImpl(path, isWriter);
        } else if (isWriter) {
            throw new IllegalStateException("Bitcask already opened; cannot open again for writing.");
        }
        return instance;
    }

    @Override
    public void put(String key, String value) throws IOException {
        // Write to active file
        String entry = key + ":" + value + "\n";
        activeFile.write(ByteBuffer.wrap(entry.getBytes()));
        activeFile.force(true);
        
        // Update keydir
        keydir.put(key, value);
    }

    @Override
    public String get(String key) throws IOException {
        return keydir.get(key);
    }

    @Override
    public Set<String> listKeys() {
        return keydir.keySet();
    }

    @Override
    public Map<String, String> getAll() {
        return new ConcurrentHashMap<>(keydir);
    }

    @Override
    public void close() {
        try {
            if (lock != null) {
                lock.release();
            }
            activeFile.close();
        } catch (IOException e) {
            // Log error
            System.err.println("Error closing Bitcask: " + e.getMessage());
        }
    }

    @Override
    public void sync() throws IOException {
        activeFile.force(true);
    }

    @Override
    public void merge() throws IOException {
        // Implement file compaction logic - simplified version
        System.out.println("Merge operation not yet implemented");
    }
}
