package com.flink.common.bean;

/**
 * 带时间戳的 WordCount
 */
public class WordCountTimestamp {
    private String id;
    private String word;
    private int count;
    private Long timestamp;

    public WordCountTimestamp() {

    }

    public WordCountTimestamp(String id, String word, int count, Long timestamp) {
        this.id = id;
        this.word = word;
        this.count = count;
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

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
