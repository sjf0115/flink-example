package com.flink.common.bean;

import java.io.Serializable;

/**
 * 带时间戳的 WordCount
 */
public class WordCountTimestamp implements Serializable{
    private String id;
    private String word;
    private int frequency;
    private Long timestamp;

    public WordCountTimestamp() {

    }

    public WordCountTimestamp(String id, String word, int frequency, Long timestamp) {
        this.id = id;
        this.word = word;
        this.frequency = frequency;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "WordCountTimestamp{" +
                "id='" + id + '\'' +
                ", word='" + word + '\'' +
                ", frequency=" + frequency +
                ", timestamp=" + timestamp +
                '}';
    }
}
