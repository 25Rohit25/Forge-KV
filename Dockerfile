# Multi-stage Dockerfile for ForgeKV
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /app

# Copy pom.xml and source code
COPY pom.xml .
COPY src ./src

# Build jar package
RUN mvn -B clean package -DskipTests

# Runtime stage
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Copy shaded fat-jar
COPY --from=builder /app/target/forgekv-1.0.0-SNAPSHOT.jar app.jar

# Expose gRPC port and Prometheus metrics port
EXPOSE 7001 8001 7002 8002 7003 8003

# Run the ForgeKV server daemon
ENTRYPOINT ["java", "-jar", "app.jar"]
