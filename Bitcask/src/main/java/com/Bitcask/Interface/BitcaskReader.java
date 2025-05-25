package com.Bitcask.Interface;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Collectors;

import com.Bitcask.Model.FileRecord;
import com.Bitcask.Model.KeyDirValuePointer;

public class BitcaskReader {
    private final BitcaskImpl sharedBitcask;
    private final Path storagePath;

    public BitcaskReader(Path path) throws IOException {
        this.sharedBitcask = BitcaskImpl.getInstance(false);
        this.storagePath = path;
    }

    public String get(Integer key) throws IOException {
        KeyDirValuePointer value = sharedBitcask.get(key);
        
        // if value.getFileId() file doesnt exist, search in active.data instead
        Path filePath = storagePath.resolve(value.getFileId());
        // if (!filePath.toFile().exists()) {
        //     filePath = storagePath.resolve("active.data");
        // }
        BitcaskLocks.acquireReadLock();
        if (!filePath.toFile().exists()) {
            value = sharedBitcask.get(key);
            filePath = storagePath.resolve(value.getFileId());
        }
        RandomAccessFile randomAccessFile = new RandomAccessFile(filePath.toFile(), "r");
        randomAccessFile.seek(value.getValuePosition());
        byte[] data = new byte[value.getValueSize()];
        randomAccessFile.read(data);
        BitcaskLocks.releaseReadLock();
        String valueString = new String(data);
        randomAccessFile.close();            
        return valueString; 
    }   

    public Map<Integer, String> getAll() throws IOException {
        return sharedBitcask.listKeys().stream()
            .collect(Collectors.toMap(
                key -> key,
                key -> {
                    try {
                        return get(key);
                    } catch (IOException e) {
                        throw new RuntimeException("Error retrieving value for key: " + key, e);
                    }
                }
            ));
    }
}