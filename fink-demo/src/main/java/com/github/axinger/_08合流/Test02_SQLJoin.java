package com.github.axinger._08合流;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Test02_SQLJoin {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 指定表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 状态保留时间，生产环境中，需要设置
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));


        // 员工流
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, Emp>() {
                    @Override
                    public Emp map(String value) throws Exception {
                        String[] split = value.split(",");

                        return new Emp(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]), Long.valueOf(split[3]));
                    }
                });
        tEnv.createTemporaryView("emp", empDS);


        // 部门流
        // 使用sql，不用指定水位线
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 8888)
                .map(new MapFunction<String, Dept>() {
                    @Override
                    public Dept map(String value) throws Exception {
                        String[] split = value.split(",");

                        return new Dept(Integer.valueOf(split[0]), split[1], Long.valueOf(split[2]));
                    }
                });

        tEnv.createTemporaryView("dept", deptDS);

        // 默认内连接 inter，普通内外链接，需要设置状态保留时间
        TableResult result = tEnv.executeSql("select e.empNo,e.empName,e.deptNo,d.deptName from emp e join dept d on emp.deptNo = dept.deptNo");
        result.print();

        // 只能执行查询语句
//        tEnv.sqlQuery()

        // 左外链接
        // 左右表先到，显示结果不一样， 有+I，-D，+I动作
//        tEnv.executeSql("select e.empNo,e.empName,e.deptNo,d.deptName from emp e left join dept d on emp.deptNo = dept.deptNo")
//                .print();


        // 写入kafka 使用  upsert-kafka
        // 建表
        tEnv.executeSql("CREATE emp_dept ");

        //写入kafka  方式一
        TableResult tableResult1 = tEnv.executeSql(" insert into em_dept  select .....");


        //写入kafka  方式二，用 sqlQuery，分2步
        TableResult tableResult2 = tEnv.sqlQuery("select .....").executeInsert("emp_dept");


    }
}
