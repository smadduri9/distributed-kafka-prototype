# Feature Demonstration Script

This script demonstrates three key features of the Kafka-like distributed system:

1. **Leader Election** - Raft consensus algorithm
2. **Heartbeat Algorithm** - Broker health monitoring
3. **Replication** - Message replication across brokers

## Running the Demo

### Option 1: Using Gradle (Recommended)

```bash
./gradlew runDemo
```

### Option 2: Using the shell script

```bash
./run-demo.sh
```

Both methods will:
1. Build the project
2. Run all three demonstrations
3. Show detailed output for each feature

## What the Demo Shows

### 1. Leader Election (Raft Consensus)

- Creates 3 Raft nodes in a cluster
- Shows initial state (all followers)
- Demonstrates election timeout triggering
- Shows term increment during election
- Displays election results and leader selection
- Demonstrates voting mechanism

**Output includes:**
- Node states (FOLLOWER, CANDIDATE, LEADER)
- Current term for each node
- Which node each node voted for
- Final leader election result

### 2. Heartbeat Algorithm

- Creates controller and broker
- Registers broker with controller
- Starts heartbeat mechanism (3-second interval)
- Monitors heartbeat for 10 seconds
- Shows real-time heartbeat timestamps
- Demonstrates heartbeat timeout detection
- Shows broker status (ALIVE/DEAD) based on heartbeats

**Output includes:**
- Heartbeat timestamps
- Time since last heartbeat
- Broker alive/dead status
- Timeout detection demonstration

### 3. Replication

- Creates controller with 3 brokers
- Creates topic with replication factor 3
- Shows partition assignments (leader and replicas)
- Demonstrates replication manager
- Simulates message replication
- Shows high water mark tracking
- Explains replication modes (SYNC/ASYNC)

**Output includes:**
- Broker registration
- Topic creation with replication
- Partition assignments showing leader and replicas
- Message replication simulation
- High water mark values
- Replication mode explanation

## Log Files

Each node, broker, and the controller writes logs to **separate log files** in the `logs/` directory:

### Raft Nodes (Leader Election)
- `logs/demo-node-1.log` - All logs from Raft node 1
- `logs/demo-node-2.log` - All logs from Raft node 2
- `logs/demo-node-3.log` - All logs from Raft node 3

### Brokers (Heartbeat & Replication)
- `logs/demo-broker-1.log` - All logs from Broker 1
- `logs/demo-broker-2.log` - All logs from Broker 2
- `logs/demo-broker-3.log` - All logs from Broker 3

### Controller
- `logs/demo-controller.log` - All logs from the Controller

### Watching Logs in Real-Time

Open multiple terminal windows to watch different logs simultaneously:

```bash
# Terminal 1: Watch all node logs
tail -f logs/demo-node-*.log

# Terminal 2: Watch a specific node
tail -f logs/demo-node-1.log

# Terminal 3: Watch broker logs
tail -f logs/demo-broker-*.log

# Terminal 4: Watch controller
tail -f logs/demo-controller.log
```

### Analyzing Logs After Demo

```bash
# View all logs
cat logs/demo-node-*.log logs/demo-broker-*.log logs/demo-controller.log

# Search for specific events
grep "election" logs/demo-node-*.log
grep "heartbeat" logs/demo-broker-*.log
grep "replication" logs/demo-controller.log

# Compare node states
grep "State=" logs/demo-node-*.log
```

See `LOGS.md` and `README-LOGS.md` for detailed log file documentation.

## Expected Output

The demo will show:

```
==========================================
Kafka-like Distributed System - Feature Demo
==========================================

[1] LEADER ELECTION DEMONSTRATION
=====================================
Creating 3 Raft nodes for cluster...
Starting all nodes...
Waiting for election timeout...
Checking election results...
✓ Leader elected: Node X (Term: Y)

[2] HEARTBEAT ALGORITHM DEMONSTRATION
=======================================
Controller started
Broker registered
Heartbeat started
Monitoring heartbeat...
✓ Heartbeat demonstration complete

[3] REPLICATION DEMONSTRATION
===============================
Controller started
Registering brokers...
Creating topic with replication...
Partition assignments...
✓ Replication demonstration complete
```

## Understanding the Output

### Leader Election
- **Term**: Increments each time an election occurs
- **VotedFor**: Shows which candidate a node voted for
- **State**: Current role (FOLLOWER, CANDIDATE, LEADER)

### Heartbeat
- **Time since heartbeat**: Milliseconds since last heartbeat received
- **Status**: ALIVE if heartbeat received within timeout, DEAD otherwise
- **Timeout**: Default 10 seconds

### Replication
- **Leader**: Broker responsible for handling writes
- **Replicas**: List of brokers that maintain copies
- **High Water Mark**: Highest offset that has been replicated to all replicas
- **Replication Mode**: SYNC (waits) or ASYNC (fire-and-forget)

## Notes

- The demo uses simulated network communication
- In a real distributed system, these operations would involve network RPCs
- The demonstrations show the core algorithms and mechanisms
- All components are properly initialized and cleaned up

