package org.example;

public enum KafkaConfig {
    BOOTSTRAP_SERVERS_LOCAL("localhost:9092"),
    BOOTSTRAP_SERVERS_PROD("kafka-service:9092"),
    WEATHER_TOPIC_CONSUME_FROM("weather-status"),
    WEATHER_TOPIC_PRODUCE_TO("rain-trigger"),
    APPLICATION_ID_CONSUMER("weather-rain-detector-dsl");

    private final String value;

    KafkaConfig(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
