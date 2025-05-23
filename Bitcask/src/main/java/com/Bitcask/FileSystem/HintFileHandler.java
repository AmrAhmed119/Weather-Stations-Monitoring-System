package com.Bitcask.FileSystem;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 * Handler class for managing hint files in the Bitcask file system.
 * Responsible for reading and writing hint files which contain key locations.
 * Hint files are stored in the same directory as data files.
 */
public class HintFileHandler {
    private final Path baseDirPath;

    public HintFileHandler(String baseDir) {
        this.baseDirPath = Paths.get(baseDir);
    }

    public Map<String, Long> readHintFile(String hintFileName) {
        Map<String, Long> keyPositions = new HashMap<>();
        Path hintFilePath = baseDirPath.resolve(hintFileName);
        
        try (BufferedReader reader = new BufferedReader(new FileReader(hintFilePath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 2) {
                    keyPositions.put(parts[0], Long.parseLong(parts[1]));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read hint file: " + hintFileName, e);
        }
        
        return keyPositions;
    }

    public void writeHintFile(String hintFileName, byte[] fileData) throws IOException {
        Path hintFilePath = baseDirPath.resolve(hintFileName);
        Files.write(hintFilePath, fileData);
    }
} 