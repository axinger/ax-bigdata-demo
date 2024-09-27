//package com.github.axinger;
//
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//
//public class FlinkCdc03_ToJdbc {
//    public static void main(String[] args) throws Exception {
//        // 创建 Flink 环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//        // 定义源表 DDL
////        String sourceDDL = """
////                CREATE TABLE t1 (
////                	id STRING NOT NULL,
////                	name STRING,
////                	PRIMARY KEY ( id ) NOT ENFORCED
////                	) WITH (
////                	'connector' = 'mysql-cdc',
////                	'hostname' = 'localhost',
////                	'port' = '3306',
////                	'username' = 'root',
////                	'password' = '123456',
////                	'database-name' = 'ax_test',
////                	'table-name' = 't1'
////                )
////                """;
//
//        String sourceDDL = """
//                    CREATE TABLE t1 (
//                        id STRING NOT NULL,
//                        name STRING,
//                        operation STRING METADATA FROM 'operation' VIRTUAL,
//                        before ROW<id STRING, name STRING> METADATA FROM 'before' VIRTUAL,
//                        after ROW<id STRING, name STRING> METADATA FROM 'after' VIRTUAL,
//                        PRIMARY KEY (id) NOT ENFORCED
//                    ) WITH (
//                        'connector' = 'mysql-cdc',
//                        'hostname' = 'localhost',
//                        'port' = '3306',
//                        'username' = 'root',
//                        'password' = '123456',
//                        'database-name' = 'ax_test',
//                        'table-name' = 't1'
//                    )
//                """;
//
//        // 定义目标表 DDL
//        String targetDDL = """
//                CREATE TABLE target_mysql ( id BIGINT NOT NULL, name STRING, PRIMARY KEY ( id ) NOT ENFORCED) WITH (
//                	'connector' = 'jdbc',
//                	'url' = 'jdbc:mysql://localhost:3306/ax_test2',
//                	'username' = 'root',
//                	'password' = '123456',
//                    'table-name' = 't1'
//                )
//                """;
//
//
//        // 注册源表和目标表
//        tableEnv.executeSql(sourceDDL);
//        tableEnv.executeSql(targetDDL);
//
//
//// 消费 CDC 数据并区分 INSERT, UPDATE, DELETE 操作写入目标表
//
//        tableEnv.executeSql(
//                """
//                        INSERT INTO target_mysql
//                        SELECT
//                            id,
//                            name
//                        FROM
//                            t1
//                        WHERE
//                            operation = 'INSERT'
//
//                        UNION ALL
//
//                        SELECT
//                            after.id,
//                            after.name
//                        FROM
//                            t1
//                        WHERE
//                            operation = 'UPDATE'
//                        """);
//
//        env.execute();
//    }
//}
