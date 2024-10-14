package com.github.axinger._08合流;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Test02_SQLJoin_kafka {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 指定表执行环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 状态保留时间，生产环境中，需要设置
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));


        // 员工流
        // 从kafka中读取，变化的员工信息
        tEnv.executeSql("CREATE table emp(" +
                "empNo string," +
                "proc_time as  PROCTIME()" + //PROCTIME(),处理时间函数
                ")" +
                " WITH ()");


        // 部门流
        // 从HBase中，读取固定的部门
        tEnv.executeSql("CREATE table dept(" +
                "deptNo string" +
                ")" +
                " WITH ()");


        // 发送到，kafka建表
        tEnv.executeSql("CREATE table emp_dept(");

        TableResult tableResult2 = tEnv.sqlQuery("select e.empNo,e.empName,e.deptNo,d.deptName from emp e left join dept d on emp.deptNo = dept.deptNo")
                .executeInsert("emp_dept");
    }
}
