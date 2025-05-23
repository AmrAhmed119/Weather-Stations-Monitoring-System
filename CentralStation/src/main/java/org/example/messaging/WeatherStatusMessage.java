package org.example.messaging;

public record WeatherStatusMessage(long stationId, long serialNumber, String batteryStatus, long timestamp,
                                   int humidity, int temperature, int windSpeed) {
}