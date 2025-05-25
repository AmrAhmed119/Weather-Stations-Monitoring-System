package com.Bitcask.Interface;

import com.Bitcask.Model.FileRecord;
import com.Bitcask.Model.HintRecord;
import com.Bitcask.Model.KeyDirValuePointer;
import com.Bitcask.FileSystem.OlderFileHandler;
import com.Bitcask.Interface.MapBuilders.ValueMapBuilder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class Merger {
    private final OlderFileHandler olderFileHandler;
    private final String baseDir;
    
    public Merger(String baseDir) {
        this.baseDir = baseDir;
        this.olderFileHandler = new OlderFileHandler(baseDir);
    }
    
    public void mergeProcess() {
        if (BitcaskImpl.getNumOfSegments() > 1) {
            String[] olderfiles = olderFileHandler.getOlderFiles();
            String[] oldMergedfiles = olderFileHandler.getMergedFiles();
            Map<Integer, FileRecord> mergedDir = new ValueMapBuilder(olderFileHandler, baseDir).build();
            Set<String> newkeyDir = this.writeMergedFiles(mergedDir);
            this.UpdateKeyDirAndCleanOlderfiles(newkeyDir, olderfiles, oldMergedfiles);
        }
    }

    private Set<String> writeMergedFiles(Map<Integer, FileRecord> mergedDir) {
        Set<String> hintFileNames = new HashSet<>();
        try {
            List<FileRecord> currentBatch = new ArrayList<>();
            int currentBatchSize = 0;
            String currentFileName = null;
            
            // Convert map to list and sort by key for consistent ordering
            List<Map.Entry<Integer, FileRecord>> sortedEntries = new ArrayList<>(mergedDir.entrySet());
            sortedEntries.sort(Map.Entry.comparingByKey());
            
            for (Map.Entry<Integer, FileRecord> entry : sortedEntries) {
                FileRecord record = entry.getValue();
                int recordSize = record.getTotalSize();
                int key = record.getKey();
                String value = record.getValue();
                
                // If adding this record would exceed the limit, write current batch and start new one
                if (currentBatchSize + recordSize > BitcaskImpl.FileSize) {
                    if (!currentBatch.isEmpty()) {
                        FileRecord[] batchArray = currentBatch.toArray(new FileRecord[0]);
                        currentFileName = olderFileHandler.createOlderFile(batchArray, true);
                        // Map all keys in the current batch to this file
                        hintFileNames.add(currentFileName);
                        currentBatch.clear();
                        currentBatchSize = 0;
                    }
                }
                
                // Add record to current batch
                currentBatch.add(record);
                currentBatchSize += recordSize;
            }
            
            // Write any remaining records
            if (!currentBatch.isEmpty()) {
                FileRecord[] batchArray = currentBatch.toArray(new FileRecord[0]);
                currentFileName = olderFileHandler.createOlderFile(batchArray, true);
                // Map all keys in the final batch to this file
                hintFileNames.add(currentFileName);
            }
            
            return hintFileNames;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write merged files", e);
        }
    }

    private void UpdateKeyDirAndCleanOlderfiles(Set<String> hintFileNames, String[] olderFiles, String[] oldMergedFiles) {
        try {
            BitcaskImpl bitcaskImpl = BitcaskImpl.getMergerInstance();
            // Update keydir in BitcaskImpl
            for (String fileName : hintFileNames) {
                
                // First read the hint file to get the value size
                String hintFileName = fileName.replace(".data", ".hint");
                Path hintFilePath = Paths.get(baseDir, hintFileName);
                if (!Files.exists(hintFilePath)) continue;
                byte[] hintData = Files.readAllBytes(hintFilePath);
                HintRecord[] hintRecords = HintRecord.deserializeHintRecords(hintData);
                
                // Find the hint record for this key
                for (HintRecord hint : hintRecords) {
                    Long TimeStampInKeyDir = bitcaskImpl.get(hint.getKey()).getTimestamp();
                    if (hint.getTimestamp() != TimeStampInKeyDir) continue;
                    // Now read the actual record using the position and size from hint
                    // FileRecord record = olderFileHandler.readRecordFromPosition(fileName, hint.getValuePosition(), hint.getValueSize());
                    KeyDirValuePointer pointer = KeyDirValuePointer.createFromHintRecord(hint, fileName);

                    // Update keydir with the new value
                    bitcaskImpl.put(hint.getKey(), pointer);
                }
            
            }

            // Clean up old files
            BitcaskLocks.acquireWriteLock();
            olderFileHandler.cleanupOldFiles(Arrays.asList(olderFiles));
            olderFileHandler.cleanupMergedFiles(Arrays.asList(oldMergedFiles));
            BitcaskLocks.releaseWriteLock();

        } catch (IOException e) {
            throw new RuntimeException("Failed to update keydir and clean files", e);
        }
    }

}


