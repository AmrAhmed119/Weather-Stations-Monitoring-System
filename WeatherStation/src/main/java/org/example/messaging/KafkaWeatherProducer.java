package org.example.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.station.WeatherStatusMessage;

import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaWeatherProducer implements AutoCloseable{
    private static final Logger logger = Logger.getLogger(KafkaWeatherProducer.class.getName());
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public KafkaWeatherProducer(String bootstrapServers, String topic) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(properties);
        this.topic = topic;
    }

    public void sendMessage(WeatherStatusMessage message) {
        try {
            String json = objectMapper.writeValueAsString(message);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, json);
            producer.send(record);
            logger.info("[SENT] Message sent to Kafka: " + json);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "[ERROR] Failed to send message: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
