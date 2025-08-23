package com.flink.example.stream.source.custom;

import com.flink.common.bean.WordCount;
import com.flink.common.bean.WordCountTimestamp;
import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * 功能：模拟单词流 事件时间 存在乱序
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午10:57
 */
public class WordCountTimestampMockSource extends RichParallelSourceFunction<WordCountTimestamp> {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountTimestampMockSource.class);
    // 速度 每秒多少条
    private long speed = 1;
    // 阈值 最多发送多条跳 -1表示无限制
    private long threshold = -1L;
    private volatile boolean cancel = false;
    private final Random random = new Random();
    private final List<String> words = Lists.newArrayList("flink", "spark", "storm");

    public WordCountTimestampMockSource() {
    }

    public WordCountTimestampMockSource(int threshold) {
        this.threshold = threshold;
    }

    public WordCountTimestampMockSource(int speed, int threshold) {
        this.speed = speed;
        this.threshold = threshold;
    }

    @Override
    public void run(SourceContext<WordCountTimestamp> ctx) throws Exception {
        long index = 0;
        // 每条耗时多少纳秒 1s(1000000000ns)
        long delay = 1000_000_000 / speed;
        long stat1 = System.currentTimeMillis();
        long start = System.nanoTime(); // 纳秒
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                String word = words.get((int) index % words.size());
                int frequency = random.nextInt(10)+1;
                long timestamp = System.currentTimeMillis();
                // 每5个出现一次延迟1s
                if (index % 5 == 0) {
                    timestamp -= 1000;
                }
                // 每10个出现一次延迟20s
                if (index % 10 == 0) {
                    timestamp -= 20000;
                }
                WordCountTimestamp wc = new WordCountTimestamp(String.valueOf(index+1), word, frequency, timestamp);
                LOG.info("index: {}, word: {}, frequency: {}, timestamp: {}", wc.getId(), wc.getWord(), wc.getFrequency(), wc.getTimestamp());
                ctx.collect(wc);
            }
            long end = System.nanoTime();
            long diff = end - start; // 耗时
            while (diff < delay) {
                Thread.sleep(1);
                end = System.nanoTime();
                diff = end - start;
            }
            start = end;
            if(index++ >= threshold && threshold != -1) {
                long end1 = System.currentTimeMillis();
                LOG.info("Source 输出总耗时: {}s", (end1-stat1) / 1000);
                break;
            }
        }
    }

    @Override
    public void cancel() {
        cancel = true;
    }
}
