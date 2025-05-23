package org.example;

import org.example.messaging.KafkaConfig;
import org.example.messaging.KafkaWeatherConsumer;
import org.example.messaging.WeatherStatusMessage;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {

        try (KafkaWeatherConsumer consumer = new KafkaWeatherConsumer(
                KafkaConfig.BOOTSTRAP_SERVERS_PROD.getValue(),
                KafkaConfig.CONSUMER_GROUP_ID.getValue(),
                KafkaConfig.WEATHER_TOPIC.getValue())
        ) {
            while (true) {
                List<WeatherStatusMessage> messages = consumer.pollMessages();
                for (WeatherStatusMessage message : messages) {
                    logger.info(message.toString());
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