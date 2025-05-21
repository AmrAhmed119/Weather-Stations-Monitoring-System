# WeatherStream Monitoring System

## 🌦 Overview

**WeatherStream** is a distributed, real-time weather monitoring system designed to simulate and process high-frequency data streams from multiple IoT-based weather stations. This project demonstrates an end-to-end architecture for data ingestion, processing, and querying of weather sensor data using modern big data and search technologies.

## 🔧 System Architecture

1. **Data Acquisition**
   - Simulated weather stations stream real-time weather data.
   - Data is sent to a Kafka cluster for reliable queueing and transport.

2. **Data Processing & Archiving**
   - A central base station (consumer) ingests data from Kafka.
   - All readings are stored efficiently in **Parquet** format for historical analysis.

3. **Indexing and Querying**
   - A **Bitcask** key-value store provides fast access to the latest reading of each station.
   - **Elasticsearch + Kibana** index the archived Parquet files for powerful search and visualization.

## 📦 Technologies Used
- Apache Kafka
- Apache Parquet
- Bitcask (custom or embedded)
- Elasticsearch & Kibana
- Java 

## 🚀 Goals
- Handle high-throughput weather data streams.
- Enable real-time and historical weather data analysis.
- Demonstrate efficient indexing strategies for both latest and archival data.

