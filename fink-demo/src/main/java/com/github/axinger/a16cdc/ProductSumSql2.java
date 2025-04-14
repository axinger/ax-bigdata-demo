package com.github.axinger.a16cdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class ProductSumSql2 {
    public static void main(String[] args) throws Exception {
        // 1. 初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境建议根据实际情况调整并行度
        // 生产环境建议添加检查点配置（可选）
        env.enableCheckpointing(60_000); // 每60秒做一次检查点

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(30_000);
        checkpointConfig.setCheckpointStorage("file:///D:\\flink_point\\product_sum2");
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(1); // 同时只存在一个
        checkpointConfig.setMinPauseBetweenCheckpoints(2000); //两个检测点之间最小间隔

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 创建 MySQL CDC 源表（捕获 yield_records 表变更）
        String sourceDDL = " CREATE TABLE yield_records (\n" +
                "                            id BIGINT,\n" +
                "                            product_id INT,\n" +
                "                            num INT,\n" +
                "                            production_date TIMESTAMP(3),\n" +
                "                            PRIMARY KEY (id) NOT ENFORCED\n" +
                "                        ) WITH (\n" +
                "                            'connector' = 'mysql-cdc',\n" +
                "                            'hostname' = 'hadoop102',\n" +
                "                            'port' = '3306',\n" +
                "                            'username' = 'root',\n" +
                "                            'password' = '123456',\n" +
                "                            'database-name' = 'ax_test10_cdc',\n" +
                "                            'table-name' = 'yield_records'\n" +
                "                        );";

        // 执行 DDL 创建表
        tableEnv.executeSql(sourceDDL).print();


        // 创建 JDBC Sink 表（写入 yield_sum 结果）
        String sinkDDL = "CREATE TABLE yield_sum (\n" +
                "                    product_id STRING,\n" +
                "                    sum_num INT,\n" +
                "                    sum_count INT,\n" +
                "                    records_date DATE,\n" +
                "                    update_time TIMESTAMP(3),\n" +
                "                    PRIMARY KEY (product_id, records_date) NOT ENFORCED\n" +
                "                ) WITH (\n" +
                "                    'connector' = 'jdbc',\n" +
                "                    'url' = 'jdbc:mysql://hadoop102:3306/ax_test10_cdc',\n" +
                "                    'table-name' = 'yield_sum',\n" +
                "                    'username' = 'root',\n" +
                "                    'password' = '123456'\n" +
                "                );";
        tableEnv.executeSql(sinkDDL);


        // 新增 Watermark 策略（将 production_date 作为事件时间）
        String watermarkDDL = "CREATE VIEW yield_records_with_watermark AS \n" +
                "SELECT \n" +
                "   *, \n" +
                "   production_date AS event_time,  \n" + // 将生产日期作为事件时间
                "   CAST(production_date AS TIMESTAMP(3)) AS ts, \n" +
                "   WATERMARK FOR ts AS ts - INTERVAL '5' SECOND \n" + // 声明 Watermark（可处理乱序）
                "FROM yield_records;";

        tableEnv.executeSql(watermarkDDL);

        // 5. 定义聚合逻辑（按产品ID和日期分组统计）
        // 其他代码保持不变，仅修改聚合逻辑部分
        // 使用 CUMULATE 窗口重写聚合逻辑
        String statisticsSQL =
                "INSERT INTO yield_sum\n" +
                        "SELECT\n" +
                        "    CAST(product_id AS STRING) AS product_id,\n" +
                        "    SUM(num) AS sum_num,\n" +
                        "    CAST(COUNT(*) AS INT) AS sum_count,\n" +  // 类型转换
                        "    CAST(WINDOW_END AS DATE) AS records_date,  -- 窗口结束时间转日期\n" +
                        "    CURRENT_TIMESTAMP AS update_time\n" +
                        "FROM TABLE(\n" +
                        "    CUMULATE(\n" +
                        "        TABLE yield_records_with_watermark,\n" +
                        "        DESCRIPTOR(ts),  -- 时间字段\n" +
                        "        INTERVAL '5' SECOND,  -- 累积间隔（1小时触发一次）\n" +
                        "        INTERVAL '1' DAY  -- 窗口总大小（1天）\n" +
                        "    )\n" +
                        ")\n" +
                        "WHERE\n" +
                        "    product_id IS NOT NULL\n" +
                        "    AND ts IS NOT NULL\n" +
                        "GROUP BY\n" +
                        "    product_id,\n" +
                        "    WINDOW_START,\n" +
                        "    WINDOW_END;";

        // 6. 执行统计任务
        tableEnv.executeSql(statisticsSQL).print();

        // 7. 启动任务
        env.execute("Flink CDC Daily Product Statistics");
    }
}
