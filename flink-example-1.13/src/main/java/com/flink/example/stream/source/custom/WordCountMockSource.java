package com.flink.example.stream.source.custom;

import com.flink.common.bean.WordCount;
import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;

/**
 * 功能：模拟单词流
 * 作者：SmartSi
 * CSDN博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/10/14 下午10:57
 */
public class WordCountMockSource extends RichParallelSourceFunction<WordCount> {
    private static final Logger LOG = LoggerFactory.getLogger(WordCountMockSource.class);
    // 速度 每秒多少条
    private long speed = 1;
    // 阈值 最多发送多条跳 -1表示无限制
    private long threshold = -1L;
    private volatile boolean cancel = false;
    private final Random random = new Random();
    private final List<String> words = Lists.newArrayList("flink", "spark", "storm");

    public WordCountMockSource() {
    }

    public WordCountMockSource(int threshold) {
        this.threshold = threshold;
    }

    public WordCountMockSource(int speed, int threshold) {
        this.speed = speed;
        this.threshold = threshold;
    }

    @Override
    public void run(SourceContext<WordCount> ctx) throws Exception {
        long index = 0;
        // 每条耗时多少纳秒 1s(1000000000ns)
        long delay = 1000_000_000 / speed;
        long stat1 = System.currentTimeMillis();
        long start = System.nanoTime(); // 纳秒
        while (!cancel) {
            synchronized (ctx.getCheckpointLock()) {
                String word = words.get((int) index % words.size());
                WordCount wc = new WordCount(word, random.nextInt(10)+1);
                LOG.info("word: {}, frequency: {}", wc.getWord(), wc.getFrequency());
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
