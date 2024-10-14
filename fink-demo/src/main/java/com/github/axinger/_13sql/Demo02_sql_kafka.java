package com.github.axinger._13sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo02_sql_kafka {
    public static void main(String[] args) throws Exception {
        // 创建 Stream Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 定义 Kafka 连接器属性
        String kafkaBootstrapServers = "hadoop102:9092";
        String inputTopicA = "testA";
        String inputTopicB = "testB";
        String outputTopic = "testC";

        // 创建 Kafka 输入表
        tEnv.executeSql(
                "CREATE TABLE topicA (" +
                        "id INT," +
                        "name STRING," +
                        "ts TIMESTAMP(3)," + // 时间戳字段
                        "ROWTIME AS TO_TIMESTAMP(FROM_UNIXTIME(ts)) " + // 将时间戳转换为 rowtime
//                        "WATERMARK FOR ROWTIME AS ROWTIME - INTERVAL '5' SECOND" + // 定义 watermark
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "'," +
                        "'topic' = '" + inputTopicA + "'," +
                        "'scan.startup.mode' = 'latest-offset'," +
                        "'format' = 'json'" +
                        ")"
        ).print();

        tEnv.executeSql("select * from topicA").print();

        // 启动执行
        env.execute("Kafka Left Join Example");
    }
}
