package com.flink.cdc.sql.changelog;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * 功能：JOIN 键非主键 Changelog 乱序导致结果？？
 * 作者：SmartSi
 * 博客：https://smartsi.blog.csdn.net/
 * 公众号：大数据生态
 * 日期：2022/4/4 下午9:14
 */
public class JoinChangelogOutOfOrderExample {
    public static void main(String[] args) throws Exception{
        // 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 环境配置
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
        // 作业配置
        Configuration config = tEnv.getConfig().getConfiguration();
        config.setString("parallelism.default", "2");
        config.setString("table.exec.sink.upsert-materialize", "none");
        config.setString("pipeline.name", JoinChangelogOutOfOrderExample.class.getSimpleName());

        // 创建用户输入表
        String userDetailSql = "CREATE TEMPORARY TABLE user_detail (\n" +
                "  user_id BIGINT,\n" +
                "  level_id BIGINT,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'database-name' = 'flink',\n" +
                "  'table-name' = 'user_detail',\n" +
                "\t'scan.startup.mode' = 'latest-offset'\n" +
                ")";
        tEnv.executeSql(userDetailSql);

        // 创建等级输入表
        String levelDetailSql = "CREATE TEMPORARY TABLE level_detail (\n" +
                "  level_id BIGINT,\n" +
                "  level_name VARCHAR,\n" +
                "  PRIMARY KEY (level_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'database-name' = 'flink',\n" +
                "  'table-name' = 'level_detail',\n" +
                "  'scan.startup.mode' = 'initial',\n" +
                "  'scan.parallelism' = '1'\n" +
                ")";
        tEnv.executeSql(levelDetailSql);

        // 创建输出表
        String userLevelDetailSql = "CREATE TEMPORARY TABLE user_level_detail (\n" +
                "\tuser_id BIGINT,\n" +
                "\tlevel_id BIGINT,\n" +
                "  level_name VARCHAR,\n" +
                "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'jdbc',\n" +
                "  'url' = 'jdbc:mysql://localhost:3306/flink',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '123456',\n" +
                "  'table-name' = 'user_level_detail',\n" +
                "  'scan.parallelism' = '1'\n" +
                ")";
        tEnv.executeSql(userLevelDetailSql);

        // 执行 JOIN 并输出结果
        /*String sql = "INSERT INTO user_level_detail\n" +
                "SELECT a1.user_id, a1.level_id, a2.level_name\n" +
                "FROM user_detail AS a1\n" +
                "JOIN level_detail AS a2\n" +
                "ON a1.level_id = a2.level_id";
        tableEnv.executeSql(sql);*/

        // 执行 JOIN
        String groupSql = "SELECT a1.user_id, a1.level_id, a2.level_name\n" +
                "FROM user_detail AS a1\n" +
                "JOIN level_detail AS a2\n" +
                "ON a1.level_id = a2.level_id";
        Table resultTable = tEnv.sqlQuery(groupSql);

        // 输出结果
        DataStream<Row> resultStream4 = tEnv.toChangelogStream(resultTable);
        resultStream4.print();

        env.execute();
    }
}
