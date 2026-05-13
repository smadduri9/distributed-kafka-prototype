# Frontend Demo

A simple web interface to demonstrate the Kafka-like distributed system.

## Features

- **Broker Status**: Real-time broker health monitoring
- **Topic Management**: Create and list topics
- **Produce Messages**: Send messages to topics
- **Fetch Messages**: Retrieve messages from topics by offset
- **Auto-refresh**: Automatic status and topic list updates

## Running the Demo

### Option 1: Using Gradle

```bash
./gradlew run
```

This will start:
- Broker on port 9092
- REST API server on port 8080
- Frontend available at http://localhost:8080

### Option 2: Using the JAR

```bash
./gradlew build
java -jar build/libs/kafka_ds-1.0.0.jar
```

## Accessing the Frontend

Open your browser and navigate to:
```
http://localhost:8080
```

## API Endpoints

The REST API provides the following endpoints:

- `GET /api/broker/status` - Get broker status
- `GET /api/topics` - List all topics
- `POST /api/topics/create` - Create a new topic
- `POST /api/produce` - Produce a message
- `GET /api/fetch` - Fetch messages

## Usage Example

1. **Check Broker Status**: The status indicator shows if the broker is online
2. **Create a Topic**: 
   - Enter topic name (e.g., "test-topic")
   - Set partitions (e.g., 3)
   - Set replication factor (e.g., 1)
   - Click "Create Topic"
3. **Produce a Message**:
   - Enter topic name
   - Set partition ID (e.g., 0)
   - Enter message content
   - Click "Produce Message"
4. **Fetch Messages**:
   - Enter topic name
   - Set partition ID
   - Set start offset (e.g., 0)
   - Set max messages to fetch
   - Click "Fetch Messages"

## Architecture

- **Frontend**: HTML/CSS/JavaScript (vanilla, no frameworks)
- **REST API**: Java HTTP Server (built-in)
- **Broker**: Custom TCP protocol broker
- **Communication**: REST API wraps broker operations

## Browser Compatibility

Works with all modern browsers:
- Chrome/Edge
- Firefox
- Safari
- Opera


