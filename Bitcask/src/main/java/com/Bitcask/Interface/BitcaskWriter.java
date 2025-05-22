package com.Bitcask.Interface;

import java.io.IOException;
import java.nio.file.Path;

public class BitcaskWriter {
    private static BitcaskWriter instance = null;
    private final BitcaskImpl bitcask;

    private BitcaskWriter(Path path) throws IOException {
        this.bitcask = BitcaskImpl.getInstance(path, true);
    }

    public static synchronized BitcaskWriter getInstance(Path path) throws IOException {
        if (instance == null) {
            instance = new BitcaskWriter(path);
        }
        return instance;
    }

    public void put(String key, String value) throws IOException {
        bitcask.put(key, value);
    }
}