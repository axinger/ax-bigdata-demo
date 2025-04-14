package com.github.axinger.a16cdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 基于 Flink CDC 实时统计每日产品入库量和入库次数
 * 数据流向：MySQL (inventory) → Flink 实时统计 → MySQL (daily_statistics)
 */
public class ProductSum2 {
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

        // 2. 创建 Table 环境（流模式）
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 3. 定义 CDC 源表（MySQL inventory 表）
        String sourceDDL = "CREATE TABLE inventory ("
                + "  id INT,"
                + "  product_id STRING,"
                + "  quantity INT,"
                + "  inventory_date TIMESTAMP(3),"
                + "  PRIMARY KEY (id) NOT ENFORCED"
                + ") WITH ("
                + "  'connector' = 'mysql-cdc',"
                + "  'hostname' = 'hadoop202',"
                + "  'port' = '3306',"
                + "  'username' = 'root',"
                + "  'password' = '123456',"
                + "  'database-name' = 'ax_test10_cdc',"
                + "  'table-name' = 'inventory'"
                + ")";

        // 4. 定义目标表（MySQL daily_statistics 表）
        String sinkDDL =
                "CREATE TABLE daily_statistics ("
                        + "  product_id STRING,"
                        + "  total_quantity INT,"
                        + "  inventory_count INT,"
                        + "  statistics_date DATE,"
                        + "  PRIMARY KEY (product_id, statistics_date) NOT ENFORCED" // 关键修复点
                        + ") WITH ("
                        + "  'connector' = 'jdbc',"
                        + "  'url' = 'jdbc:mysql://hadoop202/ax_test10_cdc',"
                        + "  'table-name' = 'daily_statistics',"
                        + "  'username' = 'root',"
                        + "  'password' = '123456'"
                        + ")";


        // 执行 DDL 创建表
        tableEnv.executeSql(sourceDDL).print();
        tableEnv.executeSql(sinkDDL);

        // 5. 定义聚合逻辑（按产品ID和日期分组统计）
        String statisticsSQL =
                "INSERT INTO daily_statistics "
                        + "SELECT "
                        + "  product_id, "
                        + "  CAST(SUM(quantity) AS INT) AS total_quantity, "  // 累计入库量
                        + "  CAST(COUNT(*) AS INT) AS inventory_count, "      // 当日入库次数
                        + "  CAST(inventory_date AS DATE) AS statistics_date " // 转日期类型
                        + "FROM inventory "
                        + "GROUP BY product_id, CAST(inventory_date AS DATE)"; // 分组键

        // 6. 执行统计任务
        tableEnv.executeSql(statisticsSQL).print();

        // 7. 启动任务
        env.execute("Flink CDC Daily Product Statistics");
    }
}
