package com.centralstation.parquet;


import com.centralstation.messaging.WeatherStatusMessage;

import java.util.ArrayList;
import java.util.List;

public class ParquetManager {
    private final int batchSize;
    private final List<WeatherStatusMessage> buffer = new ArrayList<>();
    private final FileWriter parquetWriter;

    public ParquetManager(FileWriter parquetWriter, int batchSize) {
        this.parquetWriter = parquetWriter;
        this.batchSize = batchSize;
    }

    public void addMessage(WeatherStatusMessage message) {
        buffer.add(message);
        if (buffer.size() >= batchSize) {
            flush();
        }
    }

    public void flush() {
        parquetWriter.writeBatch(buffer);
        buffer.clear();
    }
}
