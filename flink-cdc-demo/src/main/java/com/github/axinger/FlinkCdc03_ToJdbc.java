package com.github.axinger;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCdc03_ToJdbc {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 定义源表 DDL
//        String sourceDDL = """
//                CREATE TABLE t1 (
//                	id STRING NOT NULL,
//                	name STRING,
//                	PRIMARY KEY ( id ) NOT ENFORCED
//                	) WITH (
//                	'connector' = 'mysql-cdc',
//                	'hostname' = 'localhost',
//                	'port' = '3306',
//                	'username' = 'root',
//                	'password' = '123456',
//                	'database-name' = 'ax_test',
//                	'table-name' = 't1'
//                )
//                """;

        String sourceDDL ="CREATE TABLE t1 (\n" +
                "                        id STRING NOT NULL,\n" +
                "                        name STRING,\n" +
                "                        operation STRING METADATA FROM 'operation' VIRTUAL,\n" +
                "                        before ROW<id STRING, name STRING> METADATA FROM 'before' VIRTUAL,\n" +
                "                        after ROW<id STRING, name STRING> METADATA FROM 'after' VIRTUAL,\n" +
                "                        PRIMARY KEY (id) NOT ENFORCED\n" +
                "                    ) WITH (\n" +
                "                        'connector' = 'mysql-cdc',\n" +
                "                        'hostname' = 'localhost',\n" +
                "                        'port' = '3306',\n" +
                "                        'username' = 'root',\n" +
                "                        'password' = '123456',\n" +
                "                        'database-name' = 'ax_test',\n" +
                "                        'table-name' = 't1'\n" +
                "                    )";

        // 定义目标表 DDL
        String targetDDL = " CREATE TABLE target_mysql ( \n" +
                "     id BIGINT NOT NULL, \n" +
                "     name STRING, \n" +
                "     PRIMARY KEY ( id ) NOT ENFORCED\n" +
                " ) WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://localhost:3306/ax_test2',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'table-name' = 't1'              \n" +
                ")";


        // 注册源表和目标表
        tableEnv.executeSql(sourceDDL);
        tableEnv.executeSql(targetDDL);


// 消费 CDC 数据并区分 INSERT, UPDATE, DELETE 操作写入目标表

        tableEnv.executeSql("INSERT INTO target_mysql\n" +
                "SELECT\n" +
                "    id,\n" +
                "    name\n" +
                "FROM\n" +
                "    t1\n" +
                "WHERE\n" +
                "    operation = 'INSERT'\n" +
                "\n" +
                "UNION ALL\n" +
                "\n" +
                "SELECT\n" +
                "    after.id,\n" +
                "    after.name\n" +
                "FROM\n" +
                "    t1\n" +
                "WHERE\n" +
                "    operation = 'UPDATE' ");

        env.execute();
    }
}
