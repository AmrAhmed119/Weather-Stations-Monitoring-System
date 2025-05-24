package com.Bitcask.Interface.MapBuilders;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import com.Bitcask.FileSystem.OlderFileHandler;
import com.Bitcask.Model.FileRecord;
import com.Bitcask.Model.HintRecord;

public class ValueMapBuilder {
    private final OlderFileHandler olderFileHandler;
    private final String baseDir;

    public ValueMapBuilder(OlderFileHandler olderFileHandler, String baseDir) {
        this.baseDir = baseDir;
        this.olderFileHandler = olderFileHandler;
    }

    public Map<Integer, FileRecord> build() {
        String[] olderFiles = olderFileHandler.getOlderFiles();
        String[] oldMergedFiles = olderFileHandler.getMergedFiles();
        return this.getMergedDir(oldMergedFiles, olderFiles);
    }

    private Map<Integer, FileRecord> getMergedDir(String[] mergedFiles, String[] olderFiles) {
        Map<Integer, FileRecord> latestRecords = new HashMap<>();
        try {
            processMergedFiles(mergedFiles, latestRecords);
            processOlderFiles(olderFiles, latestRecords);
            return latestRecords;
        } catch (IOException e) {
            throw new RuntimeException("Failed to merge directory", e);
        }
    }

    

    private void processOlderFiles(String[] olderFiles, Map<Integer, FileRecord> latestRecords) throws IOException {
        for (String olderFile : olderFiles) {
            Path filePath = Paths.get(baseDir, olderFile);
            if (Files.exists(filePath)) {
                byte[] fileData = Files.readAllBytes(filePath);
                FileRecord[] records = FileRecord.deserializeFileRecords(fileData);
                for (FileRecord record : records) {
                    int key = record.getKey();
                    if (!latestRecords.containsKey(key) || 
                        record.getTimestamp() > latestRecords.get(key).getTimestamp()) {
                        latestRecords.put(key, record);
                    }
                }
            }
        }
    }

    private void processMergedFiles(String[] mergedFiles, Map<Integer, FileRecord> latestRecords) throws IOException {
        for (String mergedFile : mergedFiles) {
            String hintFileName = mergedFile.replace(".data", ".hint");
            Path hintFilePath = Paths.get(baseDir, hintFileName);
            if (Files.exists(hintFilePath)) {
                byte[] hintData = Files.readAllBytes(hintFilePath);
                HintRecord[] hintRecords = HintRecord.deserializeHintRecords(hintData);
                for (HintRecord hint : hintRecords) {
                    int key = hint.getKey();
                    String value = olderFileHandler.readValueFromPosition(mergedFile, 
                                                     hint.getValuePosition(), 
                                                     hint.getValueSize());
                    FileRecord record = new FileRecord(hint.getTimestamp(), 
                                                       hint.getValueSize(), 
                                                       key, value);
                    latestRecords.put(key, record);
                    
                }
            }
        }
    }

    
}