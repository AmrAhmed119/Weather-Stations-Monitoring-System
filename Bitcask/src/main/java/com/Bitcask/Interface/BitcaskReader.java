package com.Bitcask.Interface;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;

public class BitcaskReader {
    private final BitcaskImpl sharedBitcask;

    public BitcaskReader(Path path) throws IOException {
        this.sharedBitcask = BitcaskImpl.getInstance(false);
    }

    public String get(Integer key, Path storagePath) throws IOException {
        KeyDirValuePointer value = sharedBitcask.get(key);
        
        // if value.getFileId() file doesnt exist, search in active.data instead
        Path filePath = storagePath.resolve(value.getFileId());
        if (!filePath.toFile().exists()) {
            filePath = storagePath.resolve("active.data");
        }

        RandomAccessFile randomAccessFile = new RandomAccessFile(filePath.toFile(), "r");
        randomAccessFile.seek(value.getValuePosition());
        byte[] data = new byte[value.getValueSize()];
        randomAccessFile.read(data);
        String valueString = new String(data);
        randomAccessFile.close();            
        return valueString; 
    }
}