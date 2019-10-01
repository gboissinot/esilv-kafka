package com.gboissinot.devinci.streaming.data.module.analysis.table;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Gregory Boissinot
 */
public class KafkaTableStatsConsumerUtils {

    public static void main(String[] args) {

        final String topic = "velib-freedocks";

        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-test-1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<String, Long> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));

        final AtomicInteger counter = new AtomicInteger(0);

        Set<String> statiosnNames = new HashSet<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.close();
            System.out.println("Nb elements: " + counter.get());
        }));

        while (true) {
            final ConsumerRecords<String, Long> consumerRecords = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, Long> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord);
                counter.incrementAndGet();
                statiosnNames.add(consumerRecord.key());
            }
            System.out.println(String.format("Count  set %s - Count size %s", statiosnNames.size(), counter.get()));
        }
    }

}
