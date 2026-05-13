#!/bin/bash

# Simple script to run the feature demonstrations

echo "Running Feature Demonstrations..."
echo ""

# Build first
./gradlew build --no-daemon -q

# Use Gradle to run with proper classpath
./gradlew runDemo --no-daemon

