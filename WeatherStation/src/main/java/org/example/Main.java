package org.example;

import org.example.messaging.KafkaWeatherProducer;
import org.example.station.WeatherStationMock;
import org.example.station.WeatherStatusMessage;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        WeatherStationMock weatherStation = new WeatherStationMock();
        final String bootstrapServers = "localhost:9092";
        final String topic = "weather_status";

        try (KafkaWeatherProducer producer = new KafkaWeatherProducer(bootstrapServers, topic)) {
            while (true) {
                Optional<WeatherStatusMessage> message = weatherStation.generateMessage();
                if (message.isEmpty()) {
                    logger.info("[DROPPED] Message dropped by simulation.");
                } else {
                    producer.sendMessage(message.get());
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    logger.info("[INTERRUPTED] Exiting gracefully...");
                    break;
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ERROR] Kafka producer failed: " + e.getMessage(), e);
        }
    }
}