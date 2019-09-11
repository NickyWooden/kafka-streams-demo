package com.ly.kafka.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ly.kafka.beans.Word;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer implements Deserializer<Word> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public Word deserialize(String s, byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        else {
            try {
                return jsonMapper.readValue(bytes, Word.class);
            }
            catch (Exception ex){
                System.out.println("jsonSerialize exception: "+ ex);
                return null;
            }
        }
    }

    @Override
    public void close() {

    }
}
