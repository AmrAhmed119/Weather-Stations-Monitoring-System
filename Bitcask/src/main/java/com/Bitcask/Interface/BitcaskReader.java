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

    public String get(Integer key) {
        try {
            KeyDirValuePointer value = sharedBitcask.get(key);

            Path filePath = storagePath.resolve(value.getFileId());
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

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }   

    public Map<Integer, String> getAll() throws IOException {
        return sharedBitcask.listKeys().stream()
            .collect(Collectors.toMap(
                key -> key,
                key -> {
                    return get(key);
                }
            ));
    }
}