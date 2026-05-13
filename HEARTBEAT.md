# Broker Heartbeat Mechanism

## Overview

The broker heartbeat mechanism allows the system to monitor broker health and detect when brokers become unavailable. This is essential for cluster management and partition reassignment.

## Components

### 1. BrokerHeartbeat Class
Located at: `com.kafkads.broker.BrokerHeartbeat`

- Sends periodic heartbeats to the controller
- Default interval: 3 seconds
- Automatically updates broker status in the controller's metadata

### 2. Controller Heartbeat Tracking
The Controller tracks broker heartbeats and can detect when brokers go offline:
- Monitors `lastHeartbeat` timestamp for each broker
- Default timeout: 10 seconds
- Automatically marks brokers as dead if no heartbeat received

### 3. Protocol Support
- **Request Type**: `HEARTBEAT` (code: 7)
- **Response Type**: `HEARTBEAT_RESPONSE` (code: 7)
- Handled by `RequestHandler.handleHeartbeat()`

## Usage

### Starting Heartbeat

```java
// Create broker and controller
Broker broker = new Broker(config);
Controller controller = new Controller();
controller.start();
controller.registerBroker(brokerId, host, port);

// Create and start heartbeat
BrokerHeartbeat heartbeat = new BrokerHeartbeat(broker, controller);
heartbeat.start();
```

### Stopping Heartbeat

```java
heartbeat.stop();
```

### Checking Broker Status

```java
// In the controller
MetadataManager.BrokerMetadata metadata = 
    controller.getMetadataManager().getBrokerMetadata(brokerId);

if (metadata != null) {
    boolean isAlive = metadata.isAlive();
    long lastHeartbeat = metadata.getLastHeartbeat();
    long timeSinceHeartbeat = System.currentTimeMillis() - lastHeartbeat;
    
    System.out.println("Broker " + brokerId + " is " + 
        (isAlive ? "alive" : "dead"));
    System.out.println("Last heartbeat: " + timeSinceHeartbeat + "ms ago");
}
```

## Configuration

### Heartbeat Interval
Default: 3 seconds (3000ms)
- Can be adjusted in `BrokerHeartbeat.HEARTBEAT_INTERVAL_MS`

### Broker Timeout
Default: 10 seconds (10000ms)
- Configured in `Controller.BROKER_TIMEOUT_MS`
- Broker is marked dead if no heartbeat received within this time

## Test Results

All heartbeat tests pass:
- ✅ `testHeartbeatCreation()` - Creates heartbeat instance
- ✅ `testHeartbeatStart()` - Starts heartbeat mechanism
- ✅ `testHeartbeatUpdatesController()` - Updates controller metadata
- ✅ `testHeartbeatStops()` - Stops heartbeat correctly
- ✅ `testMultipleHeartbeats()` - Sends multiple heartbeats
- ✅ `testBrokerAliveStatus()` - Tracks broker alive status
- ✅ `testHeartbeatWithoutController()` - Handles null controller gracefully

## Integration with Broker

To integrate heartbeat with a running broker:

```java
Broker broker = new Broker(config);
Controller controller = new Controller();
controller.start();

// Register broker
controller.registerBroker(
    config.getBrokerId(),
    config.getBrokerHost(),
    config.getBrokerPort()
);

// Start broker
broker.start();

// Start heartbeat
BrokerHeartbeat heartbeat = new BrokerHeartbeat(broker, controller);
heartbeat.start();

// ... broker operations ...

// On shutdown
heartbeat.stop();
broker.shutdown();
controller.stop();
```

## Monitoring

The Controller automatically monitors broker health:

```java
// Controller checks broker health every 5 seconds
// If a broker hasn't sent a heartbeat in 10 seconds, it's marked as dead
// Dead brokers trigger partition reassignment
```

## Benefits

1. **Health Monitoring**: Real-time visibility into broker status
2. **Automatic Failure Detection**: Detects broker failures automatically
3. **Partition Reassignment**: Triggers partition reassignment when brokers fail
4. **Cluster Management**: Enables dynamic cluster management

