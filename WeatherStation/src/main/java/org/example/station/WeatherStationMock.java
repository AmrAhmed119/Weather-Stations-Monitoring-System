package org.example.station;

import java.util.Optional;
import java.util.Random;

public class WeatherStationMock {
    private final long stationId;
    private final Random random = new Random();
    private Long serialNumber = 1L;

    public WeatherStationMock() {
        this.stationId = 1L + (Math.abs(random.nextLong()) % Long.MAX_VALUE); 
    }

    private String getRandomBatteryStatus() {
        int val = random.nextInt(100);
        if (val < 30) return "low";
        else if (val < 70) return "medium";
        else return "high";
    }

    private int getRandomInt(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }

    public Optional<WeatherStatusMessage> generateMessage() {
        if (random.nextInt(100) < 10) {
            return Optional.empty();
        }

        long sNo = serialNumber++;
        String batteryStatus = getRandomBatteryStatus();
        long timestamp = System.currentTimeMillis() / 1000L;
        int humidity = getRandomInt(20, 90);
        int temperature = getRandomInt(60, 110);
        int windSpeed = getRandomInt(0, 30);

        return Optional.of(new WeatherStatusMessage(
                stationId, sNo, batteryStatus, timestamp, humidity, temperature, windSpeed
        ));
    }
}