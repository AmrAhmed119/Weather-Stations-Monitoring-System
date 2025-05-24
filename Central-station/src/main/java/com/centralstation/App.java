package com.centralstation;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.Bitcask.Utils;
import com.Bitcask.Interface.Bitcask;
import com.Bitcask.Interface.BitcaskReader;

/**
 * Hello world!
 *
 */
public class App {
    public static void main( String[] args ) throws IOException {
        // Path to the storage directory
        Path storagePath = Paths.get(Utils.BITCASK_STORAGE_FOLDER.toString());
        BitcaskReader bitcaskReader = new BitcaskReader(storagePath);
        // Read the keys from the storage
    }
}
