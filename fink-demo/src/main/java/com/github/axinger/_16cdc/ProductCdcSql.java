package com.github.axinger._16cdc;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@Slf4j
public class ProductCdcSql {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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


        String query = "SELECT\n" +
                "  `name`,\n" +
                "  SUM( quantity ) AS total_quantity,\n" +
                "  CAST( DATE_FORMAT( use_date, 'yyyy-MM-dd' ) AS VARCHAR ) AS date_day \n" +
                "FROM\n" +
                "  sys_product \n" +
                "GROUP BY\n" +
                "  `name`,\n" +
                "  CAST( DATE_FORMAT( use_date, 'yyyy-MM-dd' ) AS VARCHAR )";


        String sql = StrUtil.format("SELECT CONCAT(date_day, '-' ,name) AS id, name, total_quantity, date_day FROM ({})", query);
        Table result = tableEnv.sqlQuery(sql);

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

        // 将结果写入db
        result.executeInsert("sys_product_sum");

    }


}
