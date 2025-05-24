package com.example;

import junit.framework.TestCase;
import java.nio.file.Path;
import java.nio.file.Paths;
import com.Bitcask.Utils;
import com.Bitcask.Interface.BitcaskWriter;
import com.Bitcask.Interface.Merger;
import com.Bitcask.Interface.BitcaskReader;

public class AppTest extends TestCase {

    private Path prepareStorage() {
        Path storagePath = Paths.get(Utils.BITCASK_STORAGE_FOLDER.toString());
        if (!storagePath.toFile().exists()) {
            storagePath.toFile().mkdirs();
        }
        return storagePath;
    }

    private void writeTestData(BitcaskWriter writer, int numKeys, int numUpdatesPerKey) throws Exception {
        for (int i = 0; i < numKeys; i++) {
            for (int j = 0; j < numUpdatesPerKey; j++) {
                writer.put(i, "test-value-" + i + "-" + j);
            }
        }
    }

    private void assertLatestValues(BitcaskReader reader, Path storagePath, int numKeys, int lastUpdateIndex) throws Exception {
        for (int i = 0; i < numKeys; i++) {
            String expected = "test-value-" + i + "-" + lastUpdateIndex;
            String actual = reader.get(i);
            assertEquals(expected, actual);
        }
    }

    public void testBitcaskWriteAndRead() throws Exception {
        Path storagePath = prepareStorage();
        BitcaskWriter writer = BitcaskWriter.getInstance(storagePath);
        writeTestData(writer, 5, 5);

        BitcaskReader reader = new BitcaskReader(storagePath);
        assertEquals("test-value-1-4", reader.get(1));
        assertLatestValues(reader, storagePath, 5, 4);
    }

    public void testBitcaskMerge() throws Exception {
        Path storagePath = prepareStorage();
        BitcaskWriter writer = BitcaskWriter.getInstance(storagePath);
        writeTestData(writer, 5, 5);

        Merger merger = new Merger(storagePath.toString());
        merger.mergeProcess();

        BitcaskReader reader = new BitcaskReader(storagePath);
        assertLatestValues(reader, storagePath, 5, 4);
    }

    /**
     * Test to ensure that the key directory is loaded correctly after a restart.
     * IMPORTANT: This Test should started by files in directory whatever it was merged or not.
     * @throws Exception
     */
    public void testKeyDirLoading() throws Exception {
        Path storagePath = prepareStorage();
        BitcaskWriter writer = BitcaskWriter.getInstance(storagePath);
        
        // Simulate a restart by creating a new reader
        BitcaskReader reader = new BitcaskReader(storagePath);
        assertLatestValues(reader, storagePath, 5, 4);
        
        // Check if the keydir is loaded correctly
        writer.printCurrentKeyDir();
    }
    
}
