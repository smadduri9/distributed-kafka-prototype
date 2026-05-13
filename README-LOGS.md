# Log Files for Demo

The feature demonstration script creates separate log files for each node, broker, and the controller to make it easy to track what each component is doing.

## Log File Structure

After running the demo (`./gradlew runDemo`), you'll find log files in the `logs/` directory:

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

## Viewing Logs

### During Demo (Real-time)
Open multiple terminal windows and watch different logs:

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

### After Demo
```bash
# View all logs
cat logs/demo-node-*.log logs/demo-broker-*.log logs/demo-controller.log

# View specific node
cat logs/demo-node-1.log

# Search for specific events
grep "election" logs/demo-node-*.log
grep "heartbeat" logs/demo-broker-*.log
grep "replication" logs/demo-controller.log
```

## What Each Log Contains

### Node Logs (demo-node-{id}.log)
- Node startup and initialization
- State transitions (FOLLOWER → CANDIDATE → LEADER)
- Election events and term increments
- Voting decisions
- Heartbeat sending (if leader)
- Log replication events

### Broker Logs (demo-broker-{id}.log)
- Broker initialization
- Topic creation
- Message production operations
- Message fetching operations
- Heartbeat sending to controller
- Storage operations

### Controller Log (demo-controller.log)
- Controller startup
- Broker registration events
- Broker heartbeat updates
- Broker timeout detection
- Topic creation and partition assignment
- Partition reassignment on broker failure

## Example Analysis

### Analyzing Leader Election
```bash
# See which nodes became candidates
grep "Became candidate" logs/demo-node-*.log

# See election terms
grep "term=" logs/demo-node-*.log | sort

# Compare node states
grep "State=" logs/demo-node-*.log
```

### Analyzing Heartbeat
```bash
# See all heartbeats
grep "heartbeat" logs/demo-broker-*.log

# See heartbeat timeouts
grep "timeout" logs/demo-controller.log

# Check broker status
grep "ALIVE\|DEAD" logs/demo-controller.log
```

### Analyzing Replication
```bash
# See replication events
grep "replication\|replicate" logs/demo-controller.log
grep "replication\|replicate" logs/demo-broker-*.log

# See partition assignments
grep "Partition\|partition" logs/demo-controller.log
```

## Log Format

Each log entry follows this format:
```
YYYY-MM-DD HH:mm:ss.SSS [thread-name] LEVEL logger-name - message
```

Example:
```
2025-12-04 19:18:10.007 [main] INFO  com.kafkads.consensus.RaftNode - Starting Raft node: nodeId=1
2025-12-04 19:18:10.211 [pool-1-thread-1] INFO  com.kafkads.consensus.RaftNode - Became candidate: nodeId=1, term=0
```

## Tips

1. **Run demo in background, watch logs in foreground** - Best way to see real-time activity
2. **Use grep to filter** - Find specific events across all logs
3. **Compare timestamps** - Understand the sequence of events across nodes
4. **Watch multiple logs simultaneously** - See how nodes interact

