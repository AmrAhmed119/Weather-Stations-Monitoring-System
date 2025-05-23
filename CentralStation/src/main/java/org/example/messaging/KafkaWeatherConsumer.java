package org.example.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaWeatherConsumer implements AutoCloseable {
    private static final Logger logger = Logger.getLogger(KafkaWeatherConsumer.class.getName());
    private final KafkaConsumer<String, String> consumer;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String topic;

    public KafkaWeatherConsumer(String bootstrapServers, String groupId, String topic) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(properties);
        this.topic = topic;
        this.consumer.subscribe(Collections.singletonList(topic));
    }

    public List<WeatherStatusMessage> pollMessages() {
        List<WeatherStatusMessage> messages = new ArrayList<>();
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                try {
                    WeatherStatusMessage message = objectMapper.readValue(record.value(), WeatherStatusMessage.class);
                    messages.add(message);
                    logger.info("[RECEIVED] Message received from Kafka: " + message);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "[ERROR] Failed to parse message: " + record.value(), e);
                }
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ERROR] Error while polling messages: " + e.getMessage(), e);
        }
        return messages;
    }


    @Override
    public void close() {
        consumer.close();
    }
}
