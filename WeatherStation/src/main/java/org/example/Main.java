package org.example;

import org.example.station.WeatherStationMock;
import org.example.station.WeatherStatusMessage;

import java.util.Optional;

public class Main {
    public static void main(String[] args) {
        WeatherStationMock weatherStation = new WeatherStationMock();

        while (true) {
            Optional<WeatherStatusMessage> message = weatherStation.generateMessage();
            if (message.isEmpty()) {
                System.out.println("[DROPPED] Message dropped by simulation.");
            } else {
                System.out.println("[SENT] " + message.get().toJson());
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }
}