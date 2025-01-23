package com.github.axinger.doris;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaToDorisDemo {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ConfigBean configBean = ConfigLoader.loadConfig("application.yaml", ConfigBean.class);
        ConfigBean.KafkaConfigBean kafka = configBean.getFlink().getKafka();

        ConfigBean.DorisConfigBean doris = configBean.getFlink().getDoris();

        // 定义Kafka Source表
        String createKafkaSourceDDL = String.format(
                "CREATE TABLE kafka_source ("
                        + "id BIGINT,"
                        + "name STRING,"
                        + "age INT,"
                        + "ts TIMESTAMP(3),"
                        + "WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"
                        + ") WITH ("
                        + "'connector' = 'kafka',"
                        + "'topic' = '%s',"
                        + "'properties.bootstrap.servers' = '%s',"
                        + "'format' = 'json',"
                        + "'scan.startup.mode' = 'earliest-offset'"
                        + ")",
                kafka.getTopic(),
                kafka.getBootstrapServers()
        );

        // 定义Doris Sink表
        String createDorisSinkDDL = String.format(
                "CREATE TABLE doris_sink ("
                        + "id BIGINT,"
                        + "name STRING,"
                        + "age INT,"
                        + "ts TIMESTAMP(3)"
                        + ") WITH ("
                        + "'connector' = 'doris',"
                        + "'fenodes' = '%s',"
                        + "'table.identifier' = '%s',"
                        + "'username' = '%s',"
                        + "'password' = '%s',"
                        + "'sink.label-prefix' = '%s',"
                        + "'sink.properties.format' = 'json',"
                        + "'sink.properties.strip_outer_array' = 'true'"
                        + ")",
                doris.getFenodes(),
                doris.getTableIdentifier(),
                doris.getUsername(),
                doris.getPassword(),
                doris.getPassword()
        );

        // 执行创建表的SQL语句
        tableEnv.executeSql(createKafkaSourceDDL);
        tableEnv.executeSql(createDorisSinkDDL);

        // 插入数据到Doris Sink表
        String insertIntoDorisSQL =
                "INSERT INTO doris_sink SELECT id, name, age, ts FROM kafka_source";

        // 执行插入操作
        TableResult result = tableEnv.executeSql(insertIntoDorisSQL);
        result.print();
        // 启动Flink作业
        env.execute("Flink Kafka to Doris Example");
    }
}
