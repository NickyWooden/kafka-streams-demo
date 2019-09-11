package com.ly.kafka.main;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MyWord {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount2");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.107:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("input-1.1.1");
       /* KStream<String,Long> words =*/ source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store3"))
                .toStream()
                .to("output-1.1.1", Produced.with(Serdes.String(), Serdes.Long()));

       KStream<String,Long> source2 = builder.stream("output-1.1.1",
               Consumed.with(Serdes.String(), Serdes.Long()));

        System.out.println("is running!");
        source2.foreach((k,v) -> System.out.println("("+k+","+v+")"));
//        words.foreach((k,v) -> System.out.println("("+k+","+v+")"));
//        words.to("output-topic", Produced.with(Serdes.String(), Serdes.Long()));
//        source.to("output-topic");

        final Topology topology = builder.build();
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
            System.out.println("stream starting...");
            streams.start();
//            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
//        System.exit(0);
        // ... same as Pipe.java above
    }
}
