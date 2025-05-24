package com.centralstation;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

// import org.jcp.xml.dsig.internal.dom.Utils;

import com.Bitcask.Interface.BitcaskWriter;
import com.centralstation.messaging.KafkaConfig;
import com.centralstation.messaging.KafkaWeatherConsumer;
import com.centralstation.messaging.WeatherStatusMessage;

public class KafkaReader {
    public static final Path STORAGE = Paths.get(Utils.BITCASK_STORAGE_FOLDER_PATH);
    private static final BitcaskWriter writer;

    static {
        try {
            if (!STORAGE.toFile().exists()) {
                Files.createDirectories(STORAGE);
            }
            writer = BitcaskWriter.getInstance(STORAGE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void readFromKafka() {
        // each 5 seconds, increment a value and update key = 1 using writer
        // int value = 0;
        // while (true) {
        //     value++;
        //     String valStr = Integer.toString(value);
        //     try {
        //         writer.put(1, valStr);
        //         System.out.println(writer.get(1));
        //         System.out.println("Updated key=1 with value: " + valStr);
        //     } catch (Exception e) {
        //         System.err.println("Failed to write to Bitcask: " + e.getMessage());
        //     }
        //     try {
        //         Thread.sleep(5000);
        //     } catch (InterruptedException e) {
        //         Thread.currentThread().interrupt();
        //     break;
        //     }
        // }

        try (KafkaWeatherConsumer consumer = new KafkaWeatherConsumer(
            KafkaConfig.BOOTSTRAP_SERVERS_PROD.getValue(),
            KafkaConfig.CONSUMER_GROUP_ID.getValue(),
            KafkaConfig.WEATHER_TOPIC.getValue())
        ) {
            while (true) {
                List<WeatherStatusMessage> messages = consumer.pollMessages();
                for (WeatherStatusMessage message : messages) {
                    // Process each message
                    writer.put((int) message.stationId(), message.toString());
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.err.println("Error in KafkaReader: " + e.getMessage());
            e.printStackTrace();
        }

    }

}