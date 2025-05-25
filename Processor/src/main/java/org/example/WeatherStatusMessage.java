package org.example;

public class WeatherStatusMessage {
    public Long station_id;
    public Long s_no;
    public String battery_status;
    public Long status_timestamp;
    public Weather weather;

    public static class Weather {
        public int humidity;
        public int temperature;
        public int wind_speed;
    }
}