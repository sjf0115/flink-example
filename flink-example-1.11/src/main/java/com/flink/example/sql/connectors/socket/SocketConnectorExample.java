package com.flink.example.sql.connectors.socket;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：Socket Connector 示例
 * 作者：@SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2025/11/1 15:09
 */
public class SocketConnectorExample {
    public static void main(String[] args) throws Exception {
        // TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 1. 创建 Socket Source 表
        String sourceSql = "CREATE TABLE socket_source_table (\n" +
                "  word STRING COMMENT '单词'\n" +
                ") WITH (\n" +
                "  'connector.type' = 'socket',\n" +
                "  'host' = 'localhost',\n" +
                "  'port' = '9100',\n" +
                "  'delimiter' = '\n',\n" +
                "  'maxNumRetries' = '3',\n" +
                "  'delayBetweenRetries' = '500'\n" +
                ")";
        tEnv.executeSql(sourceSql);

        Table table = tEnv.sqlQuery(
                "SELECT word\n" +
                "FROM socket_source_table");
        DataStream dataStream = tEnv.toAppendStream(table, Row.class);
        dataStream.print();

        env.execute("SocketConnectorExample");
    }
}
