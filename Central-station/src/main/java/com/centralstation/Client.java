package com.centralstation;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

import com.Bitcask.Interface.BitcaskReader;
import com.sun.net.httpserver.HttpExchange;

public class Client {
    public static final BitcaskReader reader;
    public static final Path STORAGE = Paths.get(Utils.BITCASK_STORAGE_FOLDER_PATH);

    static {
        try {
            if (!Paths.get(Utils.BITCASK_STORAGE_FOLDER_PATH).toFile().exists()) {
                Files.createDirectories(Paths.get(Utils.BITCASK_STORAGE_FOLDER_PATH));
            }
            reader = new BitcaskReader(STORAGE);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create storage directory", e);
        }
    }

    public static void handlePerformanceOperation(HttpExchange ex) throws IOException {
        long start = System.nanoTime();
        long timestamp = Instant.now().getEpochSecond();
        String q = ex.getRequestURI().getQuery();
        int clientCount = Integer.parseInt(parse(q).get("key"));

        List<Thread> threads = new ArrayList<>();
        for (int i = 1; i <= clientCount; i++) {
            Thread t = new Thread(new PerfWorker(i, reader, timestamp));
            threads.add(t);
            t.start();
        }
        for (Thread t : threads) {
            try { t.join(); } catch (InterruptedException ignored) {}
        }
        long durationMs = (System.nanoTime() - start) / 1_000_000;
        String resp = "Performance operation completed with " + clientCount + " clients in " + durationMs + " ms.";
        ex.sendResponseHeaders(200, resp.getBytes().length);
        ex.getResponseBody().write(resp.getBytes());
        ex.close();
    }

    public static void handleGet(HttpExchange ex) throws IOException {
        String q = ex.getRequestURI().getQuery();
        int key = Integer.parseInt(parse(q).get("key"));
        String val = reader.get(key);
        byte[] resp = (val == null ? "" : val).getBytes();
        ex.sendResponseHeaders(200, resp.length);
        ex.getResponseBody().write(resp);
        ex.close();
    }

    public static void handleAll(HttpExchange ex) throws IOException {
        List<String> lines = new ArrayList<>();
        lines.add("key,value");
        for (Map.Entry<Integer, String> entry : reader.getAll().entrySet()) {
            int key = entry.getKey();
            String value = entry.getValue();
            if (value == null) value = "";
            lines.add(key + "," + value);
        }
        byte[] resp = String.join("\n", lines).getBytes();
        ex.getResponseHeaders().add("Content-Type", "text/csv");
        ex.sendResponseHeaders(200, resp.length);
        ex.getResponseBody().write(resp);
        ex.close();
    }

    private static Map<String,String> parse(String q) {
        Map<String,String> m = new HashMap<>();
        if (q == null) return m;
        for (String part : q.split("&")) {
            String[] kv = part.split("=",2);
            if (kv.length==2) m.put(kv[0], kv[1]);
        }
        return m;
    }
}
