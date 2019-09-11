package com.ly.kafka.beans;

import lombok.Data;

@Data
public class Word {
    private String word;

    public Word(String word, Long size) {
        this.word = word;
        this.size = size;
    }
    public Word(){

    }

    private Long size;
}
