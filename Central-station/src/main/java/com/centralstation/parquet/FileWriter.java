package com.centralstation.parquet;

import com.centralstation.messaging.WeatherStatusMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class FileWriter {
    private final String outputDirectory;

    public FileWriter(String outputDirectory) {
        this.outputDirectory = outputDirectory;
    }

    public void writeBatch(List<WeatherStatusMessage> messages) {
        for (WeatherStatusMessage message : messages) {
            writeMessageAsParquet(message, getPartitionFolder(message));
        }
    }

    public String getPartitionFolder(WeatherStatusMessage message) {
        long stationId = message.stationId();
        long timestamp = message.timestamp();
        long timestampMod10 = timestamp % 10;
        String folder = outputDirectory + stationId + "_" + timestampMod10 + "/";
        // Ensure the directory exists
        try {
            Files.createDirectories(Paths.get(folder));
        } catch (IOException e) {
            throw new RuntimeException("Failed to create partition directory: " + folder, e);
        }
        return folder;
    }

    public void writeMessageAsParquet(WeatherStatusMessage message, String path) {
        // Define Avro schema for WeatherStatusMessage
        String schemaJson = """
        {
          "type": "record",
          "name": "WeatherStatusMessage",
          "fields": [
            {"name": "stationId", "type": "long"},
            {"name": "serialNumber", "type": "long"},
            {"name": "batteryStatus", "type": "string"},
            {"name": "timestamp", "type": "long"},
            {"name": "humidity", "type": "int"},
            {"name": "temperature", "type": "int"},
            {"name": "windSpeed", "type": "int"}
          ]
        }
        """;
        Schema schema = new Schema.Parser().parse(schemaJson);

        // Create Avro record
        GenericRecord record = new GenericData.Record(schema);
        record.put("stationId", message.stationId());
        record.put("serialNumber", message.serialNumber());
        record.put("batteryStatus", message.batteryStatus());
        record.put("timestamp", message.timestamp());
        record.put("humidity", message.humidity());
        record.put("temperature", message.temperature());
        record.put("windSpeed", message.windSpeed());

        // Write to Parquet file (one file per message, can be optimized for batch)
        String filePath = path + "weather_status_" + System.currentTimeMillis() + ".parquet";
        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(filePath))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE) // typically 128MB
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)     // typically 1MB
                .withConf(new org.apache.hadoop.conf.Configuration())
                .build()) {
            writer.write(record);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write Parquet file: " + filePath, e);
        }
    }
}
