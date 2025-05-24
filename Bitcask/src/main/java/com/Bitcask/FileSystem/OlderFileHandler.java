package com.Bitcask.FileSystem;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import com.Bitcask.Model.FileRecord;
import com.Bitcask.Model.HintRecord;

/**
 * Utility class for handling older files in the Bitcask file system.
 * Provides methods to list, archive, and delete old files.
 */
public class OlderFileHandler {
    private final String baseDir;
    private static final int BUFFER_SIZE = 8192;

    public OlderFileHandler(String baseDir) {
        this.baseDir = baseDir;
    }

    public String createOlderFile(FileRecord[] records, boolean isMerged) throws IOException {
        byte[] fileData = FileRecord.serializeFileRecords(records);
        String fileName = isMerged ? "merged_" + System.currentTimeMillis() + ".data" : "older_" + System.currentTimeMillis() + ".data";
        Path filePath = Paths.get(baseDir, fileName);
        Files.write(filePath, fileData);
        if(isMerged){
            String hintFileName = fileName.replace(".data", ".hint");
            Path hintFilePath = Paths.get(baseDir, hintFileName);
            HintRecord[] hintRecords = HintRecord.getHintRecords(records);
            byte[] hintData = HintRecord.serializeHintRecords(hintRecords);
            Files.write(hintFilePath, hintData);
        }
        return fileName;
    }

    public FileRecord readRecordFromPosition(String fileName, long position, int valueSize) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(new File(baseDir, fileName), "r")) {
            raf.seek(position);
            long timestamp = raf.readLong();
            int key = raf.readInt();
            byte[] valueBytes = new byte[valueSize];
            raf.read(valueBytes);
            return new FileRecord(timestamp, valueSize, key, new String(valueBytes));
        }
    }

    
    public void cleanupOldFiles(List<String> olderFiles) {
        for (String file : olderFiles) {
            try {
                Files.deleteIfExists(Paths.get(baseDir, file));
            } catch (IOException e) {
                throw new RuntimeException("Failed to delete old file: " + file, e);
            }
        }
    }

    public void cleanupMergedFiles(List<String> mergedFiles) {
    for (String file : mergedFiles) {
        try {
            Files.deleteIfExists(Paths.get(baseDir, file));
            Files.deleteIfExists(Paths.get(baseDir, file.replace(".data", ".hint")));
        } catch (IOException e) {
            throw new RuntimeException("Failed to delete old file: " + file, e);
        }
    }
}


    public String[] getOlderFiles() {
        try {
            return Files.list(Paths.get(baseDir))
                .filter(path -> path.toString().endsWith(".data"))
                .filter(path -> path.getFileName().toString().startsWith("older_"))
                .map(path -> path.getFileName().toString())
                .toArray(String[]::new);
        } catch (IOException e) {
            throw new RuntimeException("Failed to list older files", e);
        }
    }

    public String[] getMergedFiles() {
        try {
            return Files.list(Paths.get(baseDir))
                .filter(path -> path.toString().endsWith(".data"))
                .filter(path -> path.getFileName().toString().startsWith("merged_"))
                .map(path -> path.getFileName().toString())
                .toArray(String[]::new);
        } catch (IOException e) {
            throw new RuntimeException("Failed to list merged files", e);
        }
    }

    
}