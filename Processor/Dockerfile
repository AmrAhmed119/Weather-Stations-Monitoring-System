# Stage 1: Build the application using Maven and Java 21
FROM maven:3.9.6-eclipse-temurin-21 AS build

WORKDIR /app

COPY pom.xml .
COPY src ./src

RUN mvn clean package -DskipTests

# Stage 2: Run the application using Java 21
FROM eclipse-temurin:21-jdk

WORKDIR /app

COPY --from=build /app/target/*-jar-with-dependencies.jar app.jar

# Run the jar
ENTRYPOINT ["java", "-jar", "app.jar"]
