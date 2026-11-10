package com.kafkads.broker;

import com.kafkads.config.BrokerConfig;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;

/** Entry point: starts a broker from a properties config file. */
public final class Main {

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println("Usage: kafkads <broker.properties>");
            System.exit(1);
        }
        BrokerConfig config = BrokerConfig.load(Path.of(args[0]));
        Broker broker = new Broker(config);
        broker.start();
        broker.startRaft();
        System.out.println("KafkaDS broker " + config.nodeId() + " listening on "
                + config.host() + ":" + config.port()
                + " hosting partitions " + broker.assignment().partitionsOf(config.nodeId())
                + " of assignment " + broker.assignment());

        CountDownLatch shutdown = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            broker.close();
            shutdown.countDown();
        }));
        shutdown.await();
    }
}
