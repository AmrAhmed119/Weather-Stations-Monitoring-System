package org.example;

import org.example.messaging.KafkaConfig;
import org.example.messaging.KafkaWeatherConsumer;
import org.example.messaging.WeatherStatusMessage;
import org.example.parquet.ParquetConfig;
import org.example.parquet.ParquetManager;
import org.example.parquet.FileWriter;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        FileWriter parquetWriter = new FileWriter(ParquetConfig.OUTPUT_DIRECTORY.getString());
        ParquetManager parquetManager = new ParquetManager(parquetWriter, ParquetConfig.BATCH_SIZE.getInt());

        try (KafkaWeatherConsumer consumer = new KafkaWeatherConsumer(
                KafkaConfig.BOOTSTRAP_SERVERS_PROD.getValue(),
                KafkaConfig.CONSUMER_GROUP_ID.getValue(),
                KafkaConfig.WEATHER_TOPIC.getValue())
        ) {
            while (true) {
                List<WeatherStatusMessage> messages = consumer.pollMessages();
                for (WeatherStatusMessage message : messages) {
                    parquetManager.addMessage(message);
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ERROR] Kafka Consumer failed: " + e.getMessage(), e);
        }
    }
}