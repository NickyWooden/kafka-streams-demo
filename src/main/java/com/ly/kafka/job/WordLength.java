package com.ly.kafka.job;

import com.ly.kafka.base.BaseTopology;
import com.ly.kafka.beans.Word;
import com.ly.kafka.utils.JsonDeserializer;
import com.ly.kafka.utils.JsonSerializer;
import com.ly.kafka.utils.PriorityQueueSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;

public class WordLength extends BaseTopology {
    @Override
    public void calcTopology(KStream<String, String> kStream) {
    Serde<Word> serde = Serdes.serdeFrom(new JsonSerializer(),new JsonDeserializer());
        kStream
                .mapValues(v ->new Word(v,0L))
                .groupBy((k,v)->v.getWord(), Grouped.with( Serdes.String(),serde))
                .windowedBy(TimeWindows.of(Duration.ofSeconds(10)).advanceBy(Duration.ofSeconds(10)))
                .aggregate(()->new Word("none",0L),
                        (k,v,agg) ->{
                        agg.setSize(agg.getSize()+v.getWord().length());
                        agg.setWord(v.getWord());
                        return agg;
                        },
                        Materialized.<String,Word, WindowStore<Bytes,byte[]>> as("word-store").withValueSerde(serde)
                        )
                .toStream()
                .foreach((k,v) ->System.out.println(k+","+v));
    }
}
