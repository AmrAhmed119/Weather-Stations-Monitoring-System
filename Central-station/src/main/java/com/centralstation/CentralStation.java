package com.centralstation;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class CentralStation {
    private static final int PORT = 8080;
    
    public static void main(String[] args) throws Exception {
        // ——— Kafka Consumer Thread ———
        Thread kafkaThread = new Thread(KafkaReader::readFromKafka);
        kafkaThread.setName("kafka-consumer-thread");
        kafkaThread.start();

        // ——— HTTP API ———
        HttpServer http = HttpServer.create(new InetSocketAddress(PORT), 0);
        http.createContext("/get", Client::handleGet);
        http.createContext("/all", Client::handleAll);
        http.createContext("/perf", Client::handlePerformanceOperation);
        http.setExecutor(Executors.newCachedThreadPool());
        http.start();
        System.out.println("CentralStation HTTP up on port " + PORT);

    }
}


