package com.github.axinger.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToDorisDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义Kafka Source表
        String createKafkaSourceDDL =
                "CREATE TABLE kafka_source ("
                        + "id BIGINT,"
                        + "name STRING,"
                        + "age INT,"
                        + "ts TIMESTAMP(3),"
                        + "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = 'your_topic_name',"
                        + "'properties.bootstrap.servers' = 'your_kafka_broker:9092',"
                        + "'format' = 'json',"
                        + "'scan.startup.mode' = 'earliest-offset'"
                        + ")";

        // 定义Doris Sink表
        String createDorisSinkDDL =
                "CREATE TABLE doris_sink ("
                        + "id BIGINT,"
                        + "name STRING,"
                        + "age INT,"
                        + "ts TIMESTAMP(3)"
                        + ") WITH ("
                        + "'connector' = 'doris',"
                        + "'fenodes' = 'your_doris_fe_host:8030',"
                        + "'table.identifier' = 'your_db.your_table',"
                        + "'username' = 'your_username',"
                        + "'password' = 'your_password',"
                        + "'sink.label-prefix' = 'your_label_prefix',"
                        + "'sink.properties.format' = 'json',"
                        + "'sink.properties.strip_outer_array' = 'true'"
                        + ")";

        // 执行创建表的SQL语句
        tableEnv.executeSql(createKafkaSourceDDL);
        tableEnv.executeSql(createDorisSinkDDL);

        // 插入数据到Doris Sink表
        String insertIntoDorisSQL =
                "INSERT INTO doris_sink SELECT id, name, age, ts FROM kafka_source";

        // 执行插入操作
        TableResult result = tableEnv.executeSql(insertIntoDorisSQL);

        // 启动Flink作业
        System.out.println("Flink job started.");
        env.execute("Flink Kafka to Doris Example");
    }
}
