package com.Bitcask.Interface.MapBuilders;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.Bitcask.FileSystem.OlderFileHandler;
import com.Bitcask.Interface.BitcaskWriter;
import com.Bitcask.Model.FileRecord;
import com.Bitcask.Model.HintRecord;
import com.Bitcask.Model.KeyDirValuePointer;

public class PointerMapBuilder {
    private final OlderFileHandler olderFileHandler;
    private final String baseDir;
    private final BitcaskWriter bitcaskWriter;

    public PointerMapBuilder(OlderFileHandler olderFileHandler, String baseDir) throws Exception {
        this.baseDir = baseDir;
        this.olderFileHandler = olderFileHandler;
        this.bitcaskWriter = BitcaskWriter.getInstance(Paths.get(baseDir));
    }

    public Map<Integer, KeyDirValuePointer> build() {
        String[] olderFiles = olderFileHandler.getOlderFiles();
        String[] oldMergedFiles = olderFileHandler.getMergedFiles();
        return this.getMergedDir(oldMergedFiles, olderFiles);
    }

    private Map<Integer, KeyDirValuePointer> getMergedDir(String[] mergedFiles, String[] olderFiles) {
        Map<Integer, KeyDirValuePointer> latestRecords = new HashMap<>();
        try {
            processMergedFiles(mergedFiles, latestRecords);
            processUnmergedFiles(olderFiles, latestRecords);
            return latestRecords;
        } catch (IOException e) {
            throw new RuntimeException("Failed to merge directory", e);
        }
    }
    
    private void processMergedFiles(String[] mergedFiles, Map<Integer, KeyDirValuePointer> latestRecords) throws IOException {
        for (String mergedFile : mergedFiles) {
            String hintFileName = mergedFile.replace(".data", ".hint");
            Path hintFilePath = Paths.get(baseDir, hintFileName);
            if (Files.exists(hintFilePath)) {
                byte[] hintData = Files.readAllBytes(hintFilePath);
                HintRecord[] hintRecords = HintRecord.deserializeHintRecords(hintData);
                for (HintRecord hint : hintRecords) {
                    int key = hint.getKey();
                    KeyDirValuePointer pointer = KeyDirValuePointer.createFromHintRecord(hint, mergedFile);
                    latestRecords.put(key, pointer);
                }
            }
        }
    }

    private void processUnmergedFiles(String[] olderFiles, Map<Integer, KeyDirValuePointer> latestRecords) throws IOException {
        for (String olderFile : olderFiles) {
            processOneUnmergedFile(latestRecords, olderFile);
        }

        // Process the active file separately
        String activeFile = "active.data";
        processOneUnmergedFile(latestRecords, activeFile);
    }

    private void processOneUnmergedFile(Map<Integer, KeyDirValuePointer> latestRecords, String unmergedFile) throws IOException {
        Path filePath = Paths.get(baseDir, unmergedFile);
        if (Files.exists(filePath)) {
            byte[] fileData = Files.readAllBytes(filePath);
            FileRecord[] records = FileRecord.deserializeFileRecords(fileData);
            long[] valuePositions = FileRecord.getValuePositions(fileData);
            for (int i = 0; i < records.length; i++) {
                FileRecord record = records[i]; 
                int key = record.getKey();
                long valuePosition = valuePositions[i];
                
                if (!latestRecords.containsKey(key) ||
                    record.getTimestamp() > latestRecords.get(key).getTimestamp()) {
                    KeyDirValuePointer pointer = new KeyDirValuePointer(
                        unmergedFile,
                        record.getValueSize(),
                        valuePosition,
                        record.getTimestamp()
                    );
                    latestRecords.put(key, pointer);
                }
            }
        }
    }

    
}