# KafkaDS: Distributed Message Broker

KafkaDS is a distributed message broker in Java inspired by Apache Kafka.
It focuses on core distributed-systems behaviors including leader election,
replication, partition leadership, and secure high-throughput transport.

## Highlights

- Raft-based leader election and heartbeat-based failure detection
- Partition leadership management across brokers
- Synchronous and asynchronous replication modes
- Consumer offset tracking for reliable consumption
- Netty-based binary protocol with SSL and compression

## Tech Stack

- Java
- Netty
- Maven

## Project Structure

- `src/main/java/com/kafkads/broker` broker lifecycle and cluster membership
- `src/main/java/com/kafkads/raft` leader election and consensus logic
- `src/main/java/com/kafkads/replication` sync and async replication flows
- `src/main/java/com/kafkads/storage` log and segment abstractions
- `src/main/java/com/kafkads/network` protocol, SSL, compression, transport

## Status

This repository is being organized and documented as a standalone project.
Code modules and tests will continue to be added.
