package com.vc.window;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Application {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final Topology topology = getTopology("streams-plaintext-input", "streams-pipe-output");

        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static Topology getTopology(String inputTopic, String outputTopic) {
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder
                .stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        sessionWindows(outputTopic, stream);
        return builder.build();
    }

    private static void sessionWindows(String outputTopic, KStream<String, String> stream) {
        Materialized<String, Long, SessionStore<Bytes, byte[]>> materialized =
                Materialized.<String, Long, SessionStore<Bytes, byte[]>>as("dedup-store")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long());
//                        .withRetention(Duration.ofMillis(499L));
        KTable<Windowed<String>, Long> count = stream
                .groupByKey()
                .windowedBy(SessionWindows.with(Duration.ofMillis(500L)).grace(Duration.ofMillis(600L)))
                .aggregate(() -> 0L,
                        (key, value, aggregate) -> aggregate + 1,
                        (aggKey, aggOne, aggTwo) -> aggOne + aggTwo, materialized);

        count
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>>() {
                    @Override
                    public KeyValue<String, Long> apply(Windowed<String> windowedKey, Long value) {
                        String key = String.format("%s-%s:%s", windowedKey.window().start(), windowedKey.window().end(), windowedKey.key());
                        if (value != null && value == 1) {
                            return new KeyValue<>(key, value);
                        } else {
                            key = "ignore-" + key;
                            return new KeyValue<>(key, value);
                        }
                    }
                })
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    }

    private static void basicGroupByWindow(String outputTopic, KStream<String, String> stream) {
        Materialized<String, Long, WindowStore<Bytes, byte[]>> materialized =
                Materialized.<String, Long, WindowStore<Bytes, byte[]>>as("dedup-store")
                .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long());
        stream
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofMillis(5000)).advanceBy(Duration.ofMillis(1000)))
                .aggregate(() -> 0L, (key, value, currentCount) -> currentCount + 1L, materialized)
                .toStream()
                .map(new KeyValueMapper<Windowed<String>, Long, KeyValue<String, Long>>() {
                    @Override
                    public KeyValue<String, Long> apply(Windowed<String> windowedKey, Long value) {
                        return new KeyValue<>(
                                String.format("%s-%s:%s", windowedKey.window().start(), windowedKey.window().end(), windowedKey.key()),
                                value);
                    }
                })
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
    }
}