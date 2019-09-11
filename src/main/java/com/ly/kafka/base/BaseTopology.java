package com.ly.kafka.base;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

abstract public class BaseTopology {
    private Properties initConf(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, getClass().getName());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.107:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        return props;
    }
    public void startStream(String topic){
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> kStream = builder.stream(topic);
        calcTopology(kStream);
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology,initConf());

        final CountDownLatch latch = new CountDownLatch(1);
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
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
    }
    public abstract void calcTopology(KStream<String, String> kStream);
}
