package com.github.axinger._08合流;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class Test03_lookup_join {
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
        // 去重
        // kafka join多表，+I,-D,+I,


        // 部门流
        // 从HBase中，读取固定的部门，
        // 逻辑：员工流来新数据，就从HBase中查询部门信息，过多链接，可以HBase缓存时间，经常不变化的一般用缓存
        tEnv.executeSql("CREATE table dept(" +
                "deptNo string" +
                ")" +
                " WITH ()");

        // 发送到，kafka建表，需要有主键
        tEnv.executeSql("CREATE table emp_dept(");



        // lookup join
        // FOR SYSTEM TIME AS 系统处理时间
        tEnv.sqlQuery("SELECT e.empNo,e.empName,e.deptNo,d.deptName FROM emp AS e JOIN dept  FOR SYSTEM TIME AS e.proc_time AS d  on emp.deptNo = dept.deptNo")
                //写入kafka
                .executeInsert("emp_dept");

    }
}
