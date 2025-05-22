package com.Bitcask;

import com.Bitcask.Interface.BitcaskWriter;
import com.Bitcask.Interface.BitcaskReader;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Demonstration of Bitcask functionality
 */
public class App {
    public static void main(String[] args) {
        try {
            // Create a path for the Bitcask storage
            Path storagePath = Paths.get("bitcask-storage");
            
            // Initialize writer and write some data
            BitcaskWriter writer = BitcaskWriter.getInstance(storagePath);
            writer.put("test-key", "test-value");
            System.out.println("Written key-value: test-key -> test-value");
            
            // Initialize reader and read data
            BitcaskReader reader = new BitcaskReader(storagePath);
            String value = reader.get("test-key");
            System.out.println("Read value for test-key: " + value);
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
/**
 * 1. Writing data using BitcaskWriter (appending, threshold -> old file -> new active file, update keydir) handling sync-on-put
 * 2. Threads: one thread for writing, multple for reading (client is shell script running end-points)
 * 3. background workers: compaction and merging files. 
 * 4. loading key-dir
 */