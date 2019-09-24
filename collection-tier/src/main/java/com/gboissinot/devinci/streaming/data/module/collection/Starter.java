package com.gboissinot.devinci.streaming.data.module.collection;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

public class Starter {
    public static void main(String[] args) throws InterruptedException, NoSuchAlgorithmException, KeyStoreException, IOException {

        KafkaHandlerConfig config = new KafkaHandlerConfig();
        config.setActive(true);
        config.setBootstrapServers(Collections.singletonList("localhost:9092"));
        config.setTopicName("velib_stats");

        KafkaPublisher publisher = new KafkaPublisher(config);

        Collector collector = new Collector(publisher);
        ScheduledExecutorRepeat executorRepeat = new ScheduledExecutorRepeat(collector, 10);
        executorRepeat.repeat();
    }
}
