package com.ly.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ly.kafka.beans.Word;
import org.apache.kafka.common.serialization.Serializer;
import org.rocksdb.Statistics;

import java.util.Map;

public class JsonSerializer implements Serializer<Word> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Word obj) {
        try {
            return jsonMapper.writeValueAsBytes(obj);
        }
        catch (Exception ex){
            System.out.println("jsonSerialize exception."+ ex);
            return null;
        }
    }

    @Override
    public void close() {

    }
}
