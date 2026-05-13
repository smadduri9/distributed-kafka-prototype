# Log Files Documentation

During the feature demonstrations, each node, broker, and the controller writes logs to separate files for easy tracking and debugging.

## Log File Structure

All log files are written to the `logs/` directory:

### Raft Nodes
- `demo-node-1.log` - Logs from Raft node 1
- `demo-node-2.log` - Logs from Raft node 2
- `demo-node-3.log` - Logs from Raft node 3

### Brokers
- `demo-broker-1.log` - Logs from Broker 1
- `demo-broker-2.log` - Logs from Broker 2
- `demo-broker-3.log` - Logs from Broker 3

### Controller
- `demo-controller.log` - Logs from the Controller

## What's Logged

### Raft Node Logs
- Node state transitions (FOLLOWER → CANDIDATE → LEADER)
- Election events and term increments
- Voting decisions
- Heartbeat sending (if leader)
- Log replication events

### Broker Logs
- Broker startup and shutdown
- Topic creation
- Message production
- Message fetching
- Heartbeat sending to controller
- Storage operations

### Controller Logs
- Controller startup
- Broker registration
- Broker heartbeat updates
- Broker timeout detection
- Topic creation and partition assignment
- Partition reassignment on broker failure

## Viewing Logs

### During Demo
Logs are written in real-time as the demo runs. You can watch them with:

```bash
# Watch all node logs
tail -f logs/demo-node-*.log

# Watch a specific node
tail -f logs/demo-node-1.log

# Watch broker logs
tail -f logs/demo-broker-*.log

# Watch controller logs
tail -f logs/demo-controller.log
```

### After Demo
View complete logs:

```bash
# View all logs
cat logs/demo-node-*.log logs/demo-broker-*.log logs/demo-controller.log

# View specific node's complete log
cat logs/demo-node-1.log

# Search for specific events
grep "election" logs/demo-node-*.log
grep "heartbeat" logs/demo-broker-*.log
grep "replication" logs/demo-controller.log
```

## Log Format

Each log entry follows this format:
```
YYYY-MM-DD HH:mm:ss.SSS [thread] LEVEL logger - message
```

Example:
```
2025-12-04 19:08:17.514 [main] INFO  com.kafkads.consensus.RaftNode - Starting Raft node: nodeId=1
2025-12-04 19:08:17.719 [pool-1-thread-1] INFO  com.kafkads.consensus.RaftNode - Became candidate: nodeId=1, term=0
```

## Log Levels

- **INFO**: General operational messages
- **DEBUG**: Detailed debugging information
- **WARN**: Warning messages
- **ERROR**: Error messages

## Log Rotation

Log files are rotated daily. Old logs are kept for 7 days before being automatically deleted.

## Example Analysis

### Leader Election Analysis
```bash
# See which node became leader
grep "Became leader" logs/demo-node-*.log

# See election terms
grep "term=" logs/demo-node-*.log

# See voting behavior
grep "voted" logs/demo-node-*.log
```

### Heartbeat Analysis
```bash
# See all heartbeats
grep "heartbeat" logs/demo-broker-*.log

# See heartbeat timeouts
grep "timeout" logs/demo-controller.log
```

### Replication Analysis
```bash
# See replication events
grep "replication" logs/demo-controller.log
grep "replicate" logs/demo-broker-*.log
```

## Tips

1. **Run demo in one terminal, watch logs in another** - This gives you real-time visibility
2. **Use grep to filter** - Find specific events across all logs
3. **Compare node logs** - See how different nodes react to the same events
4. **Check timestamps** - Understand the sequence of events across nodes

