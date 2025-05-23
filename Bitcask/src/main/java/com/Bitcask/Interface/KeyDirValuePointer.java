package com.Bitcask.Interface;

public class KeyDirValuePointer {
    private String fileId;
    private int valueSize;
    private long valuePosition;
    private long timestamp;

    public KeyDirValuePointer(String fileId, int valueSize, long valuePosition, long timestamp) {
        this.fileId = fileId;
        this.valueSize = valueSize;
        this.valuePosition = valuePosition;
        this.timestamp = timestamp;
    }

    public String getFileId() {
        return fileId;
    }

    public int getValueSize() {
        return valueSize;
    }

    public long getValuePosition() {
        return valuePosition;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
