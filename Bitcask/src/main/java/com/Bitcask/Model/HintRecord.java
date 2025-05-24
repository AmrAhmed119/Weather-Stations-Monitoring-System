package com.Bitcask.Model;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HintRecord implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final long timestamp;
    private final int valueSize;
    private final long valuePosition;
    private final int key;

    public HintRecord(long timestamp, int valueSize, long valuePosition, int key) {
        this.timestamp = timestamp;
        this.valueSize = valueSize;
        this.valuePosition = valuePosition;
        this.key = key;
    }

    // Getters
    public long getTimestamp() { return timestamp; }
    public int getValueSize() { return valueSize; }
    public long getValuePosition() { return valuePosition; }
    public int getKey() { return key; }

    // Serialize hint record to bytes
    public byte[] serialize() {
        // Calculate total size: timestamp(8) + valueSize(4) + valuePosition(8) + key(4)
        int totalSize = 8 + 4 + 8 + 4;
        
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putLong(timestamp);
        buffer.putInt(valueSize);
        buffer.putLong(valuePosition);
        buffer.putInt(key);
        
        return buffer.array();
    }

    // Deserialize bytes to hint record
    public static HintRecord deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        
        long timestamp = buffer.getLong();
        int valueSize = buffer.getInt();
        long valuePosition = buffer.getLong();
        int key = buffer.getInt();
        
        return new HintRecord(timestamp, valueSize, valuePosition, key);
    }

    public static HintRecord[] deserializeHintRecords(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        int recordSize = 8 + 4 + 8 + 4;  // timestamp + valueSize + valuePosition + key
        int numRecords = data.length / recordSize;
        HintRecord[] records = new HintRecord[numRecords];
        
        for (int i = 0; i < numRecords; i++) {
            byte[] recordData = new byte[recordSize];
            buffer.get(recordData);
            records[i] = deserialize(recordData);
        }
        
        return records;
    }

    public static byte[] serializeHintRecords(HintRecord[] records) {
        int recordSize = records[0].getTotalSize();
        int totalSize = recordSize * records.length;
        ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        
        for (HintRecord record : records) {
            buffer.put(record.serialize());
        }
        
        return buffer.array();
    }

    // Get total size of serialized hint record
    public int getTotalSize() {
        return 8 + 4 + 8 + 4;  // timestamp + valueSize + valuePosition + key
    }

    public static HintRecord[] getHintRecords(FileRecord[] records) {
        List<HintRecord> hintRecords = new ArrayList<>();
        long currentPosition = 0;
        
        for (FileRecord record : records) {
            // Create hint record with current position
            HintRecord hintRecord = new HintRecord(
                record.getTimestamp(),
                record.getValueSize(),
                currentPosition + 8 + 4 + 4,
                record.getKey()
            );
            hintRecords.add(hintRecord);
            
            // Update position for next record
            // Add size of timestamp(8) + valueSize(4) + key(4) + value
            currentPosition += 8 + 4 + 4 + record.getValueSize();
        }
        
        return hintRecords.toArray(new HintRecord[0]);
    }
} 