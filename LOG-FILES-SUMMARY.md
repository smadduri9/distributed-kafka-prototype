# Log Files Summary

The demo script creates separate log files for each node, broker, and the controller to demonstrate the features clearly.

## Log File Structure

After running `./gradlew runDemo`, you'll find these log files in the `logs/` directory:

### Leader Election (Raft Nodes)
- `demo-node-1.log` - Logs from Raft node 1
- `demo-node-2.log` - Logs from Raft node 2
- `demo-node-3.log` - Logs from Raft node 3

Each file contains:
- Node startup and initialization
- State transitions (FOLLOWER → CANDIDATE → LEADER)
- Election events and term increments
- Voting decisions

### Heartbeat Algorithm
- `demo-broker-1.log` - Logs from Broker 1 (heartbeat sender)
- `demo-controller.log` - Logs from Controller (heartbeat receiver)

Broker log contains:
- Heartbeat sending events
- Heartbeat interval information

Controller log contains:
- Broker registration
- Heartbeat updates
- Broker timeout detection
- Broker status changes (ALIVE/DEAD)

### Replication
- `demo-broker-1.log` - Leader broker logs
- `demo-broker-2.log` - Follower broker logs
- `demo-broker-3.log` - Follower broker logs
- `demo-controller.log` - Partition assignment logs

## Quick Commands

### View All Logs
```bash
ls -lh logs/demo-*.log
```

### Watch Logs in Real-Time
```bash
# All nodes
tail -f logs/demo-node-*.log

# Specific node
tail -f logs/demo-node-1.log

# All brokers
tail -f logs/demo-broker-*.log

# Controller
tail -f logs/demo-controller.log
```

### Search for Events
```bash
# Find all election events
grep "election" logs/demo-node-*.log

# Find all heartbeat events
grep "heartbeat" logs/demo-broker-*.log logs/demo-controller.log

# Find replication events
grep "replication" logs/demo-broker-*.log logs/demo-controller.log
```

### Compare Node States
```bash
# See state transitions for all nodes
grep "Became" logs/demo-node-*.log

# See terms for all nodes
grep "term=" logs/demo-node-*.log
```

## Example Analysis

### Analyzing Leader Election
1. Open `logs/demo-node-1.log`, `logs/demo-node-2.log`, `logs/demo-node-3.log`
2. Compare timestamps to see election sequence
3. Look for "Became candidate" and "Became leader" events
4. Check term increments across nodes

### Analyzing Heartbeat
1. Open `logs/demo-broker-1.log` to see heartbeat sends
2. Open `logs/demo-controller.log` to see heartbeat receives
3. Compare timestamps to verify 3-second interval
4. Look for timeout detection after heartbeat stops

### Analyzing Replication
1. Open `logs/demo-controller.log` to see partition assignments
2. Open broker logs to see replication operations
3. Check for leader and follower activities

