package com.flink.example.stream.sink.custom;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 功能：自定义实现无输出 Sink
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/9/3 下午3:06
 */
public class DiscardSinkFunction<IN> extends RichSinkFunction<IN> {
    private static final long serialVersionUID = 1L;

    public DiscardSinkFunction() {
    }

    @Override
    public void invoke(IN record) {

    }
}
