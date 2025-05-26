package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.logging.Logger;
import java.util.Properties;

public class Main {
    private static final Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, KafkaConfig.APPLICATION_ID_CONSUMER.getValue());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.BOOTSTRAP_SERVERS_PROD.getValue());
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        ObjectMapper mapper = new ObjectMapper();
        KStream<String, String> stream = builder.stream(KafkaConfig.WEATHER_TOPIC_CONSUME_FROM.getValue());

        stream
            .peek((key, value) -> logger.info("Received message: key=" + key + "value="+ value))
            .filter((key, value) -> {
                try {
                    WeatherStatusMessage status = mapper.readValue(value, WeatherStatusMessage.class);
                    return status.humidity() > 70;
                } catch (Exception e) {
                    logger.info("Failed to parse message: " + value + e);
                    return false;
                }
            })
            .mapValues(value -> {
                try {
                    WeatherStatusMessage status = mapper.readValue(value, WeatherStatusMessage.class);
                    String result = String.format("Rain detected at station %d with humidity %d%%", 
                        status.stationId(), status.humidity());
                    logger.info("Sending message: " + result);
                    return result;
                } catch (Exception e) {
                    logger.info("Failed to parse message for sending: " +  value + e);
                    return "Invalid data";
                }
            })
            .to(KafkaConfig.WEATHER_TOPIC_PRODUCE_TO.getValue(), Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        logger.info("Kafka Streams started.");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Kafka Streams.");
            streams.close();
        }));
    }
}