package com.github.axinger._16cdc;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Slf4j
public class ProductCdcSql {

    /**
     * 主函数 - Flink SQL 流处理任务入口
     * @param args 命令行参数（未使用）
     * @throws Exception 可能抛出执行环境初始化、SQL执行相关的异常
     */
    public static void main(String[] args) throws Exception {
        // 初始化流处理环境（单并行度调试配置）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        /* 创建CDC源表配置
         * 表结构说明：
         * - 使用MySQL CDC连接器实时捕获变更
         * - 定义事件时间字段use_date及其5秒延迟水印策略
         * - 主键字段id用于保证数据唯一性（NOT ENFORCED模式）
         * 连接配置：
         * - 指向hadoop102节点的MySQL数据库ax_test
         * - 监控sys_product表的变更
         */
        tableEnv.executeSql("CREATE TABLE sys_product (" +
                "id STRING NOT NULL," +
                "name STRING," +
                "quantity INT," +
                "use_date TIMESTAMP(3)," +
                "watermark for use_date as use_date - interval '5' second, " + // 定义水印
                "PRIMARY KEY ( id ) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'ax_test'," +
                "'table-name' = 'sys_product'" +
                ")");

        /* 聚合查询逻辑：
         * 1. 按商品名称(name)和日期维度(date_day)分组
         * 2. 计算每日各商品的总销量(total_quantity)
         * 3. 格式化时间字段为yyyy-MM-dd格式作为日期分区
         */
        String query = "SELECT\n" +
                "  `name`,\n" +
                "  SUM( quantity ) AS total_quantity,\n" +
                "  CAST( DATE_FORMAT( use_date, 'yyyy-MM-dd' ) AS VARCHAR ) AS date_day \n" +
                "FROM\n" +
                "  sys_product \n" +
                "GROUP BY\n" +
                "  `name`,\n" +
                "  CAST( DATE_FORMAT( use_date, 'yyyy-MM-dd' ) AS VARCHAR )";

        // 构建最终输出格式：拼接日期和名称作为唯一ID
        String sql = StrUtil.format("SELECT CONCAT(date_day, '-' ,name) AS id, name, total_quantity, date_day FROM ({})", query);
        Table result = tableEnv.sqlQuery(sql);

        /* 创建结果表配置（JDBC输出）
         * 表结构说明：
         * - 主键id由日期和名称组合生成
         * - 输出到相同MySQL实例的sys_product_sum表
         * 连接配置：
         * - 使用JDBC连接器进行结果写入
         * - 目标表用于存储每日商品销售汇总数据
         */
        tableEnv.executeSql(
                "CREATE TABLE sys_product_sum (" +
                        "id STRING NOT NULL," +
                        "name STRING," +
                        "total_quantity INT," +
                        "date_day STRING, " +
                        "PRIMARY KEY ( id ) NOT ENFORCED " +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = 'jdbc:mysql://hadoop102:3306/ax_test'," + // 替换为实际的URL
                        "'username' = 'root'," + // 用户名
                        "'password' = '123456'," + // 密码
                        "'table-name' = 'sys_product_sum'" + // 表名
                        ")"
        );

        // 执行流式写入：将聚合结果持久化到MySQL汇总表
        result.executeInsert("sys_product_sum");
    }
}
