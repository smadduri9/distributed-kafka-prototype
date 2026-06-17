package com.kafkads.bench;

import com.kafkads.broker.Broker;
import com.kafkads.config.BrokerConfig;
import com.kafkads.network.Client;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.messages.ProduceRequest;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * A small single-broker load test that reports produce throughput and latency.
 * Run with: java -cp target/kafkads.jar com.kafkads.bench.Benchmark [records] [recordBytes] [batch]
 */
public final class Benchmark {

    private Benchmark() {
    }

    public static void main(String[] args) throws Exception {
        int totalRecords = args.length > 0 ? Integer.parseInt(args[0]) : 500_000;
        int recordBytes = args.length > 1 ? Integer.parseInt(args[1]) : 128;
        int batchSize = args.length > 2 ? Integer.parseInt(args[2]) : 500;
        int latencySamples = 20_000;

        Path dataDir = Files.createTempDirectory("kafkads-bench");
        Properties p = new Properties();
        p.setProperty("node.id", "1");
        p.setProperty("port", "29099");
        p.setProperty("log.dir", dataDir.toString());
        Broker broker = new Broker(BrokerConfig.fromProperties(p));
        broker.start();
        broker.startReplication();

        try (Client client = new Client()) {
            client.connect("127.0.0.1", 29099);

            byte[] payload = new byte[recordBytes];
            Arrays.fill(payload, (byte) 'x');

            // Throughput phase: batched produce.
            List<byte[]> batch = new ArrayList<>(batchSize);
            for (int i = 0; i < batchSize; i++) {
                batch.add(payload);
            }
            long produced = 0;
            long start = System.nanoTime();
            while (produced < totalRecords) {
                client.sendAndWait(new ProduceRequest(0, Acks.LEADER, batch), 30, TimeUnit.SECONDS);
                produced += batchSize;
            }
            long elapsedNs = System.nanoTime() - start;
            double seconds = elapsedNs / 1e9;
            double recsPerSec = produced / seconds;
            double mbPerSec = (produced * (double) recordBytes) / (1024 * 1024) / seconds;

            // Latency phase: single-record produce.
            long[] latencies = new long[latencySamples];
            List<byte[]> single = List.of(payload);
            for (int i = 0; i < latencySamples; i++) {
                long t0 = System.nanoTime();
                client.sendAndWait(new ProduceRequest(0, Acks.LEADER, single), 30, TimeUnit.SECONDS);
                latencies[i] = System.nanoTime() - t0;
            }
            Arrays.sort(latencies);

            System.out.println("=== KafkaDS benchmark ===");
            System.out.printf("records=%d recordBytes=%d batch=%d%n", produced, recordBytes, batchSize);
            System.out.printf("throughput: %.0f records/sec, %.1f MB/sec%n", recsPerSec, mbPerSec);
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
