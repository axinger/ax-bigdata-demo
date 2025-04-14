package com.github.axinger.a16cdc;

import com.github.axinger.doris.ConfigLoader;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 基于 Flink CDC 实时统计每日产品入库量和入库次数
 * 数据流向：MySQL (yield_records) → Flink 实时统计 → MySQL (yield_sum)
 */
public class ProductSumSql {
    public static void main(String[] args) throws Exception {
        // 1. 初始化流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); // 生产环境建议根据实际情况调整并行度
        // 生产环境建议添加检查点配置（可选）
        env.enableCheckpointing(60_000); // 每60秒做一次检查点

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(30_000);
        checkpointConfig.setCheckpointStorage("file:///D:\\flink_point\\product_sum");
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(1); // 同时只存在一个
        checkpointConfig.setMinPauseBetweenCheckpoints(2000); //两个检测点之间最小间隔

        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 创建 MySQL CDC 源表（捕获 yield_records 表变更）
//        String sourceDDL = "CREATE TABLE yield_records (\n" +
//                "                            id BIGINT,\n" +
//                "                            product_id INT,\n" +
//                "                            num INT,\n" +
//                "                            production_date TIMESTAMP(3),\n" +
//                "                            PRIMARY KEY (id) NOT ENFORCED\n" +
//                "                        ) WITH (\n" +
//                "                            'connector' = 'mysql-cdc',\n" +
//                "                            'hostname' = 'hadoop102',\n" +
//                "                            'port' = '3306',\n" +
//                "                            'username' = 'root',\n" +
//                "                            'password' = '123456',\n" +
//                "                            'database-name' = 'ax_test10_cdc',\n" +
//                "                            'table-name' = 'yield_records'\n" +
//                "                        );";
        String sourceDDL = ConfigLoader.loadConfigSql("a16_sql1/create_table_yield_records.sql");
        // 执行 DDL 创建表
        tableEnv.executeSql(sourceDDL).print();

        // 创建 JDBC Sink 表（写入 yield_sum 结果）
//        String sinkDDL = "CREATE TABLE yield_sum (\n" +
//                "                    product_id STRING,\n" +
//                "                    sum_num INT,\n" +
//                "                    sum_count INT,\n" +
//                "                    records_date DATE,\n" +
//                "                    update_time TIMESTAMP(3),\n" +
//                "                    PRIMARY KEY (product_id, records_date) NOT ENFORCED\n" +
//                "                ) WITH (\n" +
//                "                    'connector' = 'jdbc',\n" +
//                "                    'url' = 'jdbc:mysql://hadoop102:3306/ax_test10_cdc',\n" +
//                "                    'table-name' = 'yield_sum',\n" +
//                "                    'username' = 'root',\n" +
//                "                    'password' = '123456'\n" +
//                "                );";

        String sinkDDL = ConfigLoader.loadConfigSql("a16_sql1/create_table_yield_sum.sql");
        tableEnv.executeSql(sinkDDL);

        // 5. 定义聚合逻辑（按产品ID和日期分组统计）
        // 其他代码保持不变，仅修改聚合逻辑部分
//        String statisticsSQL = "INSERT INTO yield_sum\n" +
//                "SELECT\n" +
//                "    CAST(product_id AS STRING) AS product_id,\n" +
//                "    SUM(num) AS sum_num,\n" +
//                "    CAST(COUNT(*) AS INT) AS sum_count,\n" +
//                "    CAST(production_date AS DATE) AS records_date,\n" +
//                "    CURRENT_TIMESTAMP AS update_time\n" +
//                "FROM yield_records\n" +
//                "WHERE\n" +
//                "    product_id IS NOT NULL\n" +
//                "    AND production_date IS NOT NULL\n" +
//                "GROUP BY\n" +
//                "    product_id,\n" +
//                "    CAST(production_date AS DATE);";
        String statisticsSQL = ConfigLoader.loadConfigSql("a16_sql1/insert_into_yield_sum.sql");
        // 6. 执行统计任务
        tableEnv.executeSql(statisticsSQL).print();

        // 7. 启动任务
        env.execute("Flink CDC Daily Product Statistics");
    }
}
