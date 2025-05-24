package com.Bitcask;

import com.Bitcask.Interface.BitcaskWriter;
import com.Bitcask.Interface.Merger;
import com.Bitcask.Model.KeyDirValuePointer;
import com.Bitcask.Interface.BitcaskReader;

import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Demonstration of Bitcask functionality
 */
public class App {
    public static void main(String[] args) {
        try {
            // // Create a path for the Bitcask storage
            Path storagePath = Paths.get(Utils.BITCASK_STORAGE_FOLDER.toString());
            // Ensure the storage directory exists
            if (!storagePath.toFile().exists()) {
                storagePath.toFile().mkdirs();
            }
            
            // Initialize writer and write some data
            BitcaskWriter writer = BitcaskWriter.getInstance(storagePath);
            // Use only 10 unique keys and perform multiple put operations
            for (int i = 0; i < 5; i++) {
                for (int j = 0; j < 5; j++) { // 5 updates per key
                    writer.put(i, "test-value-" + i + "-" + j);
                }
            }
            
            // Initialize reader and read data
            BitcaskReader reader = new BitcaskReader(storagePath);
            String value = reader.get(1);
            System.out.println("Read value: " + value);

            // writer.close();
            writer.printCurrentKeyDir();
            // Perform a merge operation
            Merger merger = new Merger(storagePath.toString());
            merger.mergeProcess();
            writer.printCurrentKeyDir();
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }

    }
}
/** Assumptions:
 * 1. entries pointed to active file data by file-id as sequence number but in actual folder by active.data.
 */
/**
 * 1. Writing data using BitcaskWriter (appending, threshold -> old file -> new active file, update keydir) handling sync-on-put
 * 2. Threads: one thread for writing, multple for reading (client is shell script running end-points)
 * 3. background workers: compaction and merging files. 
 * 4. loading key-dir
 */

// we have 4 types of files (critical sections): 
/**
 * active (only writer access it), 
 * old, merged (may be accessed by writer and reader)
 * keydir (reader and writer access it)

// older files are immutable but could be deleted -> readers from clients and background workers
// keydir 
/**
 * active file -> append then update keydir
 * reader -> 
 */
// what is baseDir
// what happens when we rewrite on concurrent hashmap using merger by writer object
// locks on keydir and old reader file to be atomic operation. same when renaming the active file.
// getMergeinstance in bitcaskimpl
// old and merged files may be smaller.
// raf for active file is not the best.
// delete getAll operation from BitcaskWriter, it is not needed -> use get(key) instead in central station client view all.

// threads: 
// 1. writer thread

