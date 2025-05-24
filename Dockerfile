# ======== Stage 1: Build ========
FROM maven:3.9.6-eclipse-temurin-21 AS builder

WORKDIR /app

# Copy only what's needed
COPY pom.xml .
COPY Bitcask ./Bitcask
COPY Central-station ./Central-station

# Build centralStation and its dependency (Bitcask)
RUN mvn -pl Central-station -am clean package -DskipTests

# ======== Stage 2: Run ========
FROM eclipse-temurin:21-jdk

WORKDIR /app

# Copy the built fat jar
COPY --from=builder /app/Central-station/target/Central-station-1.0-SNAPSHOT-jar-with-dependencies.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
