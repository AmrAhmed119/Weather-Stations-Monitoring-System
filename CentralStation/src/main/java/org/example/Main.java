package org.example;

import org.example.messaging.KafkaWeatherConsumer;

public class Main {
    public static void main(String[] args) {
        final String bootstrapServers = "kafka-service:9092";
        final String topic = "weather-status";
        final String groupId = "weather-status-group";

        try (KafkaWeatherConsumer consumer = new KafkaWeatherConsumer(bootstrapServers, groupId, topic)) {
            while (true) {
                consumer.pollMessages();

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    break;
                }
            }
        }
    }
}