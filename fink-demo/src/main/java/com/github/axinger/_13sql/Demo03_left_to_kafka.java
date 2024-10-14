package com.github.axinger._13sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Demo03_left_to_kafka {

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
                        "ts AS PROCTIME()" +
//                        "WATERMARK FOR proctime AS proctime - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "'," +
                        "'topic' = '" + inputTopicA + "'," +
                        "'scan.startup.mode' = 'latest-offset'," +
                        "'format' = 'json'" +
                        ")"
        );

        tEnv.executeSql(
                "CREATE TABLE topicB (" +
                        "id INT," +
                        "age INT," +
                        "ts AS PROCTIME()" +
//                        "WATERMARK FOR proctime AS proctime - INTERVAL '5' SECOND" +
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "'," +
                        "'topic' = '" + inputTopicB + "'," +
                        "'scan.startup.mode' = 'latest-offset'," +
                        "'format' = 'json'" +
                        ")"
        );

        // 创建 Kafka 输出表
        tEnv.executeSql(
                "CREATE TABLE outputTable (" +
                        "idA INT," +
                        "name STRING," +
                        "idB INT," +
                        "age INT, " +
                        "ts TIMESTAMP(3) METADATA FROM 'timestamp'," +
//                        "WATERMARK FOR proctime AS proctime - INTERVAL '5' SECOND" +
                        " PRIMARY KEY (`idA`) NOT ENFORCED" + // 一定要有主键
                        ") WITH (" +
                        "'connector' = 'upsert-kafka'," + // 写入kafka，可以变更
                        "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "'," +
                        "'topic' = '" + outputTopic + "'," +
                        "'key.format' = 'json'," +
                        "'value.format' = 'json'" +
                        ")"
        );


        // 执行 SQL 查询
        // proctime AS PROCTIME()
        Table table = tEnv.sqlQuery("SELECT " +
                "a.id AS idA, " +
                "a.name AS name, " +
                "b.id AS idB, " +
                "b.age AS age, " +
                "PROCTIME() AS ts " +
                "FROM " +
                "topicA AS a " +
                "LEFT JOIN " +
                "topicB AS b " +
                "ON " +
                "a.id = b.id");

        // left 查询
//        table.execute().print();

        table.executeInsert("outputTable").print();

        env.execute("写入kafka");

    }
}
