package com.flink.common.bean;

/**
 * 功能：简单用户行为
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/5/11 09:55
 */
public class SimpleUserBehavior {
    private Long userId;
    private Long timestamp;

    public SimpleUserBehavior() {
    }

    public SimpleUserBehavior(Long userId, Long timestamp) {
        this.userId = userId;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }
}
