package com.kafkads.cli;

import com.kafkads.client.Consumer;
import com.kafkads.client.ConsumerRecord;
import com.kafkads.client.Producer;
import com.kafkads.client.RecordMetadata;
import com.kafkads.protocol.Acks;
import com.kafkads.protocol.messages.MetadataResponse;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Minimal command-line client built on the public {@link Producer} and {@link Consumer}.
 *
 * <pre>
 * kafkads-cli metadata &lt;bootstrap&gt;            cluster view from the first broker that answers
 * kafkads-cli status   &lt;host:port&gt;            one broker's own view
 * kafkads-cli produce  &lt;bootstrap&gt; &lt;partition&gt; &lt;acks: leader|quorum|all&gt; &lt;value&gt;...
 * kafkads-cli consume  &lt;bootstrap&gt; &lt;partition&gt; [fromOffset]
 * </pre>
 */
public final class Cli {

    private Cli() {
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            usage();
            return;
        }
        try {
            switch (args[0]) {
                case "metadata" -> {
                    try (Consumer c = new Consumer(args[1])) {
                        print(c.metadata());
                    }
                }
                case "status" -> {
                    try (Consumer c = new Consumer(args[1])) {
                        print(c.metadataFrom(args[1]));
                    }
                }
                case "produce" -> {
                    int partition = Integer.parseInt(args[2]);
                    Acks acks = Acks.valueOf(args[3].toUpperCase());
                    List<byte[]> values = new ArrayList<>();
                    for (int i = 4; i < args.length; i++) {
                        values.add(args[i].getBytes(StandardCharsets.UTF_8));
                    }
                    try (Producer p = new Producer(args[1])) {
                        RecordMetadata m = p.send(partition, values, acks);
                        System.out.println("OK partition=" + m.partition() + " baseOffset=" + m.baseOffset()
                                + " count=" + m.count());
                    }
                }
                case "consume" -> {
                    int partition = Integer.parseInt(args[2]);
                    long from = args.length > 3 ? Long.parseLong(args[3]) : 0L;
                    try (Consumer c = new Consumer(args[1])) {
                        for (ConsumerRecord r : c.readAll(partition, from)) {
                            System.out.println(r.offset() + "\t" + new String(r.value(), StandardCharsets.UTF_8));
                        }
                    }
                }
                default -> usage();
            }
        } catch (RuntimeException e) {
            System.err.println("FAILED " + e.getClass().getSimpleName() + ": " + e.getMessage());
            System.exit(2);
        }
    }

    private static void print(MetadataResponse m) {
        System.out.println("broker=" + m.respondingBroker());
        for (MetadataResponse.BrokerInfo b : m.brokers()) {
            System.out.println("  broker " + b.id() + " " + b.host() + ":" + b.port());
        }
        for (MetadataResponse.PartitionInfo p : m.partitions()) {
            System.out.println("partition=" + p.partition() + " replicas=" + p.replicas()
                    + " preferred=" + p.preferredLeader() + " leader=" + p.leader() + " term=" + p.term()
                    + " logEndOffset=" + p.logEndOffset() + " highWatermark=" + p.highWatermark());
        }
    }

    private static void usage() {
        System.err.println("""
                Usage:
                  metadata <bootstrap>
                  status   <host:port>
                  produce  <bootstrap> <partition> <leader|quorum|all> <value>...
                  consume  <bootstrap> <partition> [fromOffset]""");
        System.exit(1);
    }
}
