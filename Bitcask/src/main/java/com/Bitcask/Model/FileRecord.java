package com.Bitcask.Model;

import java.io.*;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class FileRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final long timestamp;
    private final int valueSize;
    private final int key;
    private final String value;

    public FileRecord(int key, String value) {
        this.timestamp = Instant.now().toEpochMilli();
        this.key = key;
        this.value = value;
        this.valueSize = value.getBytes().length;
    }

    public FileRecord(long timestamp, int valueSize, int key, String value) {
        this.timestamp = timestamp;
        this.valueSize = valueSize;
        this.key = key;
        this.value = value;
    }

    // Getters
    public long getTimestamp() { return timestamp; }
    public int getValueSize() { return valueSize; }
    public int getKey() { return key; }
    public String getValue() { return value; }

    // Serialize record to bytes
    public byte[] serialize() {
        byte[] valueBytes = value.getBytes();
        
        // Calculate total size: timestamp(8) + valueSize(4) + key(4) + value
        int totalSize = 8 + 4 + 4 + valueBytes.length;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putLong(timestamp);
        buffer.putInt(valueSize);
        buffer.putInt(key);
        buffer.put(valueBytes);
        
        return buffer.array();
    }

    

    public static FileRecord[] deserializeFileRecords(byte[] data) {
        List<FileRecord> records = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        while (buffer.remaining() >= 8 + 4 + 4) {
            int startPos = buffer.position();
            long timestamp = buffer.getLong();
            int valueSize = buffer.getInt();
            int key = buffer.getInt();
            if (buffer.remaining() < valueSize) {
                buffer.position(startPos);
                break;
            }
            byte[] valueBytes = new byte[valueSize];
            buffer.get(valueBytes);
            records.add(new FileRecord(timestamp, valueSize, key, new String(valueBytes)));
        }
        
        return records.toArray(new FileRecord[0]);
    }

    public static byte[] serializeFileRecords(FileRecord[] records) {
        int totalSize = 0;
        for (FileRecord record : records) {
            totalSize += record.getTotalSize();
        }
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        for (FileRecord record : records) {
            buffer.put(record.serialize());
        }
        return buffer.array();
    }

    // Deserialize bytes to record
    public static FileRecord deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        long timestamp = buffer.getLong();
        int valueSize = buffer.getInt();
        int key = buffer.getInt();
        
        byte[] valueBytes = new byte[valueSize];
        buffer.get(valueBytes);
        String value = new String(valueBytes);
        
        return new FileRecord(timestamp, valueSize, key, value);
    }

    // Get total size of serialized record
    public int getTotalSize() {
        return 8 + 4 + 4 + value.getBytes().length;
    }

    public static long[] getValuePositions(byte[] fileData) {
        List<Long> positions = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(fileData);

        final int HEADER_SIZE = 8 + 4 + 4; // timestamp + valueSize + key

        while (buffer.remaining() >= HEADER_SIZE) {
            long position = buffer.position();
            buffer.getLong(); // Skip timestamp
            int valueSize = buffer.getInt(); // Read value size
            buffer.getInt(); // Skip key

            // Check if enough bytes remain for the value
            if (buffer.remaining() < valueSize) {
                break; // Incomplete record, stop processing

            }
            positions.add((long) buffer.position());
            buffer.position(buffer.position() + valueSize);
        }

        return positions.stream().mapToLong(Long::longValue).toArray();
    }
} 