package com.github.axinger._16cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkCdc04_MysqlToMysql {

    public static void main(String[] args) {
        test();
    }

    public static void test() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.enableCheckpointing(5000L);
//        env.getCheckpointConfig().setCheckpointTimeout(1000L);
//        env.getCheckpointConfig().setCheckpointStorage("file:///D:/flink_point");
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 同时只存在一个

        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
        String sourceDDL = "CREATE TABLE t1 (\n" +
                "                id STRING NOT NULL,\n" +
                "                name STRING,\n" +
                "                PRIMARY KEY ( id ) NOT ENFORCED\n" +
                "                ) WITH (\n" +
                "                'connector' = 'mysql-cdc',\n" +
                "                'hostname' = 'localhost',\n" +
                "                'port' = '3306',\n" +
                "                'username' = 'root',\n" +
                "                'password' = '123456',\n" +
                "                'database-name' = 'ax_test',\n" +
                "                'table-name' = 't1'\n" +
                "                )";
        tableEnvironment.executeSql(sourceDDL);
        String targetDDL = "CREATE TABLE target_mysql ( \n" +
                "     id BIGINT NOT NULL, \n" +
                "     name STRING, \n" +
                "     PRIMARY KEY ( id ) NOT ENFORCED\n" +
                " ) WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://localhost:3306/ax_test2',\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'table-name' = 't1',\n" +
                "    'sink.buffer-flush.max-rows' = '1',\n" +
                "    'sink.buffer-flush.interval' = '1s',\n" +
                "    'sink.upsert-mode' = 'on-primary-key'\n" +
                ")";

//        'sink.buffer-flush.max-rows' = '1', -- 控制每次批量写入的行数，避免一次性写入大量数据导致内存溢出
//
//        'sink.buffer-flush.interval' = '1s', -- 控制缓冲区刷新间隔，防止长时间不刷新导致数据丢失
//
//        'sink.upsert-mode' = 'on-primary-key' -- 启用 UpsertSink
        tableEnvironment.executeSql(targetDDL);
        tableEnvironment.sqlQuery("select * from t1").execute().print();

        String syncSQL = "INSERT INTO t1 SELECT * FROM source_mysql";
        tableEnvironment.executeSql(syncSQL).print();


    }
}

