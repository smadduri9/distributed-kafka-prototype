#!/bin/bash

# Demo script for Kafka-like Distributed System
# Demonstrates: Replication, Leader Election, Heartbeat Algorithm

echo "=========================================="
echo "Kafka-like Distributed System - Feature Demo"
echo "=========================================="
echo ""

# Check if Java is available
if ! command -v java &> /dev/null; then
    echo "Error: Java is not installed or not in PATH"
    exit 1
fi

# Check if Gradle wrapper exists
if [ ! -f "./gradlew" ]; then
    echo "Error: Gradle wrapper not found. Please run from project root."
    exit 1
fi

echo "Building the project..."
./gradlew build --no-daemon -q

if [ $? -ne 0 ]; then
    echo "Error: Build failed"
    exit 1
fi

echo ""
echo "Running feature demonstrations..."
echo ""

# Run the demo
./gradlew run --args="demo" --no-daemon 2>&1 | grep -v "SLF4J\|logback\|Deprecated\|BUILD"

# Alternative: Run directly with java
# java -cp "build/classes/java/main:build/libs/*" com.kafkads.demo.FeatureDemo

echo ""
echo "Demo completed!"

