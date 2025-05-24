package com.centralstation.messaging;

public enum KafkaConfig {
    BOOTSTRAP_SERVERS_LOCAL("localhost:9092"),
    BOOTSTRAP_SERVERS_PROD("kafka-service:9092"),
    CONSUMER_GROUP_ID("weather-status-group"),
    WEATHER_TOPIC("weather-status");

    private final String value;

    KafkaConfig(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}