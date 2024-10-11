package com.github.axinger;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

@Slf4j
public class Demo2_sql {
    public static void main(String[] args) throws Exception {

        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义源表

        tableEnv.executeSql("CREATE TABLE t1 (" +
                "id STRING NOT NULL," +
                "name STRING," +
                "age INT," +
                "PRIMARY KEY ( id ) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'ax_test'," +
                "'table-name' = 't1'" +
                ")");

        // 查询，然后收集数据
        try (CloseableIterator<Row> collect = tableEnv.sqlQuery("select * from t1").execute().collect()) {

            collect.forEachRemaining(row -> {
                int arity = row.getArity();
                String shorted = row.getKind().shortString();
                Object name = row.getField("name");
                Object age = row.getField("age");
                log.info("arity={}，操作类型={}，name={},age={}",arity,shorted,name,age);
            });
        } catch (Exception e) {
            System.out.println("e = " + e.getMessage());
        }


//        tableEnv.sqlQuery("select * from t1").printSchema();
        env.execute();

    }
}
