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

    private void processFileWithHints(String olderFile, Map<String, Long> keyPositions, 
                                    File mergedFile, Map<String, Long> newKeyPositions, 
                                    long currentPosition) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(new File(baseDir, olderFile), "r");
             FileOutputStream fos = new FileOutputStream(mergedFile, true)) {
            
            byte[] buffer = new byte[BUFFER_SIZE];
            for (Map.Entry<String, Long> entry : keyPositions.entrySet()) {
                raf.seek(entry.getValue());
                int bytesRead;
                while ((bytesRead = raf.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                    currentPosition += bytesRead;
                }
                newKeyPositions.put(entry.getKey(), currentPosition);
            }
        }
    }

    private void processFileWithoutHints(String olderFile, File mergedFile, 
                                       Map<String, Long> newKeyPositions, 
                                       long currentPosition) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(baseDir, olderFile)));
             FileOutputStream fos = new FileOutputStream(mergedFile, true)) {
            
            String line;
            while ((line = reader.readLine()) != null) {
                byte[] data = line.getBytes();
                fos.write(data);
                fos.write('\n');
                currentPosition += data.length + 1;
                // Assuming the first part of the line is the key
                String key = line.split(",")[0];
                newKeyPositions.put(key, currentPosition);
            }
        }
    }

    private void cleanupOldFiles(List<String> olderFiles) {
        for (String file : olderFiles) {
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