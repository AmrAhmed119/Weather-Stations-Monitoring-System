package com.centralstation;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import com.Bitcask.Interface.BitcaskReader;

public class PerfWorker implements Runnable {
    private final int threadId;
    private final BitcaskReader reader;
    private final long timestamp;

    public PerfWorker(int threadId, BitcaskReader reader, long timestamp) {
        this.threadId = threadId;
        this.reader = reader;
        this.timestamp = timestamp;
    }

    @Override
    public void run() {
        String filename = timestamp + "_thread_" + threadId + ".csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            writer.write("key,value\n");
            Map<Integer, String> allEntries = reader.getAll();
            for (var entry : allEntries.entrySet()) {
                writer.write(entry.getKey() + "," + entry.getValue() + "\n");
            }
            System.out.println("Thread " + threadId + " wrote " + filename);
        } catch (IOException e) {
            System.err.println("Thread " + threadId + " failed: " + e.getMessage());
        }
    }
}
