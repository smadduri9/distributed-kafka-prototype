package com.kafkads.bench;

import com.kafkads.broker.Broker;
import com.kafkads.client.Producer;
import com.kafkads.config.BrokerConfig;
import com.kafkads.protocol.Acks;

import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * A small single-broker load test that reports produce throughput and latency through
 * the public {@link Producer}. Broker and client share one JVM and talk over loopback,
 * so the numbers describe this code path on this machine and nothing more.
 * Run with: java -cp target/kafkads.jar com.kafkads.bench.Benchmark [records] [recordBytes] [batch]
 */
public final class Benchmark {

    private Benchmark() {
    }

    public static void main(String[] args) throws Exception {
        int totalRecords = args.length > 0 ? Integer.parseInt(args[0]) : 500_000;
        int recordBytes = args.length > 1 ? Integer.parseInt(args[1]) : 128;
        int batchSize = args.length > 2 ? Integer.parseInt(args[2]) : 500;
        int latencySamples = 5_000;

        Path dataDir = Files.createTempDirectory("kafkads-bench");
        Properties p = new Properties();
        p.setProperty("node.id", "1");
        p.setProperty("port", "29099");
        p.setProperty("log.dir", dataDir.toString());
        p.setProperty("raft.election.timeout.min.ms", "100");
        p.setProperty("raft.election.timeout.max.ms", "200");
        p.setProperty("raft.heartbeat.interval.ms", "30");
        Broker broker = new Broker(BrokerConfig.fromProperties(p));
        broker.start();
        broker.startRaft();

        try (Producer producer = new Producer(
                List.of(new InetSocketAddress("127.0.0.1", 29099)), 30_000, 60_000)) {
            byte[] payload = new byte[recordBytes];
            Arrays.fill(payload, (byte) 'x');

            List<byte[]> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                batch.add(payload);
            }
            long produced = 0;
            long start = System.nanoTime();
            while (produced < totalRecords) {
                producer.send(0, batch, Acks.QUORUM);
                produced += batchSize;
            }
            double seconds = (System.nanoTime() - start) / 1e9;

            long[] latencies = new long[latencySamples];
            List<byte[]> single = List.of(payload);
            for (int i = 0; i < latencySamples; i++) {
                long t0 = System.nanoTime();
                producer.send(0, single, Acks.QUORUM);
                latencies[i] = System.nanoTime() - t0;
            }
            Arrays.sort(latencies);

            System.out.println("=== KafkaDS benchmark (single broker, acks=QUORUM, fsync per request) ===");
            System.out.printf("records=%d recordBytes=%d batch=%d%n", produced, recordBytes, batchSize);
            System.out.printf("throughput: %.0f records/sec, %.1f MB/sec%n",
                    produced / seconds, (produced * (double) recordBytes) / (1024 * 1024) / seconds);
            System.out.printf("produce latency (single record): p50=%.3f ms, p99=%.3f ms, max=%.3f ms%n",
                    latencies[latencySamples / 2] / 1e6,
                    latencies[(int) (latencySamples * 0.99)] / 1e6,
                    latencies[latencySamples - 1] / 1e6);
            System.out.println(broker.metrics());
        } finally {
            broker.close();
        }
    }
}
