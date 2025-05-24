package com.centralstation.BitcaskClient;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import com.Bitcask.Interface.BitcaskReader;

public class Client {

    private static final Path STORAGE_PATH = Paths.get("bitcask_storage");

    public static void main(String[] args) throws Exception {
        System.out.println("Hello from Client");

        if (args.length == 0) {
            printHelp();
            return;
        }

        String command = args[0];

        switch (command) {
            case "--view-all" -> viewAll();
            case "--view" -> {
                if (args.length < 2 || !args[1].startsWith("--key=")) {
                    System.out.println("Missing key argument. Use --key=SOME_KEY");
                    return;
                }
                int key = Integer.parseInt(args[1].substring("--key=".length()));
                viewKey(key);
            }
            case "--perf" -> {
                if (args.length < 2 || !args[1].startsWith("--clients=")) {
                    System.out.println("Missing client count. Use --clients=NUM");
                    return;
                }
                int clientCount = Integer.parseInt(args[1].substring("--clients=".length()));
                runPerf(clientCount);
            }
            default -> printHelp();
        }
    }

    private static void viewAll() {
        new Thread(() -> {
            try {
                BitcaskReader reader = new BitcaskReader(STORAGE_PATH);
                long timestamp = Instant.now().getEpochSecond();
                String filename = timestamp + ".csv";

                try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                    writer.write("key,value\n");
                    Map<Integer, String> allEntries = reader.getAll();
                    for (Map.Entry<Integer, String> entry : allEntries.entrySet()) {
                        writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                    }
                }

                System.out.println("Saved all keys to " + filename);
            } catch (IOException e) {
                System.err.println("Error in viewAll thread: " + e.getMessage());
                e.printStackTrace();
            }
        }).start(); // start the thread
    }

    private static void viewKey(int key) throws IOException {
        new Thread(() -> {
            try {
                BitcaskReader reader = new BitcaskReader(STORAGE_PATH);
                String value = reader.get(key);
                if (value != null) {
                    System.out.println("Value for key " + key + ": " + value);
                } else {
                    System.out.println("No value found for key " + key);
                }
            } catch (IOException e) {
                System.err.println("Error in viewAll thread: " + e.getMessage());
                e.printStackTrace();
            }
        }).start(); 
    }

    private static void runPerf(int clients) {
        for (int i = 1; i <= clients; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    BitcaskReader threadReader = new BitcaskReader(STORAGE_PATH);
                    long timestamp = Instant.now().getEpochSecond();
                    String filename = timestamp + "_thread_" + threadId + ".csv";

                    try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
                        writer.write("key,value\n");
                        Map<Integer, String> allEntries = threadReader.getAll();
                        for (Map.Entry<Integer, String> entry : allEntries.entrySet()) {
                            writer.write(entry.getKey() + "," + entry.getValue() + "\n");
                        }
                    }

                    System.out.println("Thread " + threadId + " saved to " + filename);
                } catch (Exception e) {
                    System.err.println("Thread " + threadId + " error: " + e.getMessage());
                }
            }).start();
        }
    }

    private static void printHelp() {
        System.out.println("""
            Usage:
              java Client --view-all
              java Client --view --key=SOME_KEY
              java Client --perf --clients=100
            """);
    }
}
