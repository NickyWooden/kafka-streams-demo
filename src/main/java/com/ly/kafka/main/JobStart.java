package com.ly.kafka.main;

import com.ly.kafka.job.WordLength;

public class JobStart {
    public static void main(String[] args){
        WordLength wordLength = new WordLength();
        wordLength.startStream("word-01");
    }
}
