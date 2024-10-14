package com.github.axinger._13sql;

import com.github.axinger.bean.Person;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CreateTableAndQuery {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建表处理环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 假设我们已经有了一个DataStream，代表从源头来的数据
        DataStream<Person> source = env.fromElements(
           new Person("jim",10),
           new Person("tom",10)
        );

        // 将DataStream转换为Table
//        Schema schema = Schema.newBuilder()
//                .column("name",1)
//                .build();
        Table table1 = tableEnv.fromDataStream(source);
//        table1.printSchema();

        tableEnv.createTemporaryView("emp", table1);

        // 创建另一个Table，这里只是为了演示，实际上你可以使用其他数据源


        // 查询第一个表
        tableEnv.toRetractStream(tableEnv.sqlQuery("SELECT * FROM emp"), Person.class).print("第一个值");

        // 查询第二个表
        Table table2 = tableEnv.sqlQuery("SELECT * FROM emp");
        tableEnv.toRetractStream(table2, Person.class).print("第二个值");


//        tableEnv.sqlQuery("SELECT * FROM emp").execute().print();
//        tableEnv.executeSql("SELECT * FROM emp").print();


        // 触发执行
        env.execute("Create Table And Query Example");
    }
}
