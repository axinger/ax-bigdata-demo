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

        // 创建 Kafka 输入表
        tEnv.executeSql(
                "CREATE TABLE topicA (" +
                        "id INT," +
                        "name STRING," +
                        "ts TIMESTAMP(3)," + // ts 列是 Kafka 消息中的时间戳字段，这里可以忽略，因为我们专注于处理时间。
                        " proctime AS PROCTIME() "+
//                        "ROWTIME AS TO_TIMESTAMP(FROM_UNIXTIME(ts)) " + // 将时间戳转换为 rowtime
//                        "WATERMARK FOR ROWTIME AS ROWTIME - INTERVAL '5' SECOND" + // 定义 watermark
                        ") WITH (" +
                        "'connector' = 'kafka'," +
                        "'properties.bootstrap.servers' = '" + kafkaBootstrapServers + "'," +
                        "'topic' = '" + inputTopicA + "'," +
                        "'scan.startup.mode' = 'latest-offset'," +
                        "'format' = 'json'" +
                        ")"
        ).print();

        // # 普通查询
//        tEnv.executeSql("select * from topicA").print();

        // 窗口统计个数
        // 使用处理时间进行窗口操作
        // 在使用处理时间进行窗口操作时，Flink SQL 会根据处理时间动态生成窗口。下面是一个基于处理时间的 10 秒滚动窗口的示例，计算每个用户的操作次数。
//        tEnv.executeSql("SELECT\n" +
//                "  id,\n" +
//                "  COUNT(name) AS name_count,\n" + // 按照id分组，计算name个数
//                "  TUMBLE_START(proctime, INTERVAL '10' SECOND) AS window_start,\n" +
//                "  TUMBLE_END(proctime, INTERVAL '10' SECOND) AS window_end\n" +
//                "FROM topicA\n" +
//                "GROUP BY\n" +
//                "  id,\n" +
//                "  TUMBLE(proctime, INTERVAL '10' SECOND);\n").print();

        // 全局统计个数， -U，删除旧统计，+U，新增新统计
        tEnv.executeSql("SELECT\n" +
                "  id,\n" +
                "  COUNT(*) AS total_count,\n" +
                "  MAX(proctime) AS last_updated\n" +
                "FROM topicA\n" +
                "GROUP BY id;\n").print();

        // 启动执行
        env.execute("Kafka Left Join Example");
    }
}
