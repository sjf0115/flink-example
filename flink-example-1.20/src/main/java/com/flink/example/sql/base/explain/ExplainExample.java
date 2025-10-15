package com.flink.example.sql.base.explain;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ExplainExample {
    public static void main(String[] args) {
        // TableEnvironment
        /*EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);*/

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建表
        String sourceSql = "CREATE TABLE datagen_table (\n" +
                "    word STRING,\n" +
                "    frequency int\n" +
                ") WITH (\n" +
                "  'connector' = 'datagen',\n" +
                "  'rows-per-second' = '1',\n" +
                "  'fields.word.kind' = 'random',\n" +
                "  'fields.word.length' = '1',\n" +
                "  'fields.frequency.min' = '1',\n" +
                "  'fields.frequency.max' = '9'\n" +
                ")";
        tableEnv.executeSql(sourceSql);

        // Query 语句
        String sql = "SELECT word, SUM(frequency) AS frequency\n" +
                "FROM datagen_table\n" +
                "GROUP BY word";

        // 使用 explainSql 查看执行计划方式
        String explainResult = tableEnv.explainSql(sql);
        System.out.println(explainResult);

        System.out.println("-----------------------------------------------------");

        // 使用 executeSql 查看执行计划方式
        TableResult plainExplain = tableEnv.executeSql("EXPLAIN PLAN FOR " + sql);
        plainExplain.print();

        System.out.println("-----------------------------------------------------");

        // 使用 executeSql 查看执行计划方式
        TableResult changelogExplain = tableEnv.executeSql("EXPLAIN CHANGELOG_MODE " + sql);
        changelogExplain.print();

        System.out.println("-----------------------------------------------------");

        // 使用 executeSql 查看执行计划方式
        TableResult jsonExplain = tableEnv.executeSql("EXPLAIN JSON_EXECUTION_PLAN " + sql);
        jsonExplain.print();
    }
}
