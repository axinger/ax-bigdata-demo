package com.github.axinger._13sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkPulsarSQLExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 创建 Pulsar 表
        String createTableSql = "CREATE TABLE pulsar_table (\n" +
                "  `id` BIGINT,\n" +
                "  `name` STRING,\n" +
                "  `age` INT\n" +
                ") WITH (\n" +
                "  'connector' = 'pulsar',\n" +
                "  'topics' = 'persistent://public/default/user_behavior',\n" +
                "  'service-url' = 'pulsar://hadoop203:6650',\n" +
                "  'source.start.message-id' = 'earliest',\n" +
                "  'format' = 'json'\n" +
                ")";

        tableEnv.executeSql(createTableSql);

        // 查询数据
        String querySql = "SELECT * FROM pulsar_table";
        tableEnv.executeSql(querySql).print();

        // 执行任务
        env.execute("Flink Pulsar SQL Example");
    }
}
