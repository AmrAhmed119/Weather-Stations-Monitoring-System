package com.example;

import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.nio.file.Paths;
import static org.junit.jupiter.api.Assertions.*;

import com.Bitcask.Utils;
import com.Bitcask.Interface.BitcaskWriter;
import com.Bitcask.Interface.Merger;
import com.Bitcask.Interface.BitcaskReader;

public class AppTest {

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

    private void assertLatestValues(BitcaskReader reader, int numKeys, int lastUpdateIndex) throws Exception {
        for (int i = 0; i < numKeys; i++) {
            String expected = "test-value-" + i + "-" + lastUpdateIndex;
            String actual = reader.get(i);
            assertEquals(expected, actual);
        }
    }

    @Test
    public void testBitcaskEndToEndFlow() throws Exception {
        Path storagePath = prepareStorage();

        // Step 1: Write data
        BitcaskWriter writer = BitcaskWriter.getInstance(storagePath);
        writeTestData(writer, 5, 5);

        // Step 2: Verify read before merge
        BitcaskReader readerBeforeMerge = new BitcaskReader(storagePath);
        assertEquals("test-value-1-4", readerBeforeMerge.get(1));
        assertLatestValues(readerBeforeMerge, 5, 4);

        // Step 3: Perform merge
        Merger merger = new Merger(storagePath.toString());
        merger.mergeProcess();

        // Step 4: Verify read after merge
        BitcaskReader readerAfterMerge = new BitcaskReader(storagePath);
        assertLatestValues(readerAfterMerge, 5, 4);

        // Step 5: Simulate restart and verify keydir loading
        BitcaskReader readerAfterRestart = new BitcaskReader(storagePath);
        assertLatestValues(readerAfterRestart, 5, 4);
        writer.printCurrentKeyDir();
    }
}
