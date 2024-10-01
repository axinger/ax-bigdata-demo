package com.github.axinger;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DorisDemo {
    public static void main(String[] args) {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);
        StreamTableEnvironment tabEnv = StreamTableEnvironment.create(environment);


        Table table = tabEnv.sqlQuery("select * from doris");


//        executeInsert 和 executeSql 是两种不同的方式来操作数据，主要是在使用 Table API 或 SQL API 时涉及到。
        tabEnv.executeSql("");

        table.executeInsert("doris");

    }
}
