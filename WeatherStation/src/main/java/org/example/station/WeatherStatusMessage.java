package org.example.station;

public class WeatherStatusMessage {
    private final long stationId;
    private final long serialNumber;
    private final String batteryStatus;
    private final long timestamp;
    private final int humidity;
    private final int temperature;
    private final int windSpeed;

    public WeatherStatusMessage(long stationId, long serialNumber, String batteryStatus, long timestamp, int humidity, int temperature, int windSpeed) {
        this.stationId = stationId;
        this.serialNumber = serialNumber;
        this.batteryStatus = batteryStatus;
        this.timestamp = timestamp;
        this.humidity = humidity;
        this.temperature = temperature;
        this.windSpeed = windSpeed;
    }

    public String toJson() {
        return String.format(
            "{\n  \"station_id\": %d,\n  \"s_no\": %d,\n  \"battery_status\": \"%s\",\n  \"status_timestamp\": %d,\n  \"weather\": {\n    \"humidity\": %d,\n    \"temperature\": %d,\n    \"wind_speed\": %d\n  }\n}",
            stationId, serialNumber, batteryStatus, timestamp, humidity, temperature, windSpeed
        );
    }
}
