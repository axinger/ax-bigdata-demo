package com.github.axinger;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

@Slf4j
public class Demo3_sql_to_db {

    public static void main(String[] args) throws Exception {
        // 设置执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // left表
        tableEnv.executeSql("CREATE TABLE emp (" +
                "id BIGINT NOT NULL," +
                "emp_code INT," +
                "emp_name STRING," +
                "dept_code INT," +
                "PRIMARY KEY ( id ) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'ax_test'," +
                "'table-name' = 'sys_emp'" +
                ")");


        // right表
        tableEnv.executeSql("CREATE TABLE dept (" +
                "id BIGINT NOT NULL," +
                "dept_code INT," +
                "dept_name STRING," +
                "PRIMARY KEY ( id ) NOT ENFORCED" +
                ") WITH (" +
                "'connector' = 'mysql-cdc'," +
                "'hostname' = 'hadoop102'," +
                "'port' = '3306'," +
                "'username' = 'root'," +
                "'password' = '123456'," +
                "'database-name' = 'ax_test'," +
                "'table-name' = 'sys_dept'" +
                ")");

        // 注册自定义函数
        tableEnv.createTemporarySystemFunction("toUpper", NameToCalculation.class);

        // 执行LEFT JOIN并添加计算字段
        String joinSql = "SELECT " +
                "a.id as id, " +
                "a.emp_code as emp_code, " +
                "a.emp_name as emp_name," +
//                "b.dept_code as dept_code, " +
//                "b.dept_name as dept_name ,\n" +
                "toUpper(b.dept_code,b.dept_name) as dept " + // 使用自定义函数
                "FROM emp AS a " +
                "LEFT JOIN dept AS b " +
                "ON a.dept_code = b.dept_code";

        Table joinedTable = tableEnv.sqlQuery(joinSql);
//        joinedTable.execute().print();


        // 注册PostgreSQL接收表
        tableEnv.executeSql(
                "CREATE TABLE sys_emp_dept (" +
                        "id BIGINT NOT NULL," +
                        "emp_code INT," +
                        "emp_name STRING," +
                        "dept STRING, " +
                        " PRIMARY KEY ( id ) NOT ENFORCED \n" +
                        ") WITH (" +
                        "'connector' = 'jdbc'," +
                        "'url' = 'jdbc:mysql://hadoop102:3306/ax_test2'," + // 替换为实际的URL
                        "'username' = 'root'," + // 用户名
                        "'password' = '123456'," + // 密码
                        "'table-name' = 'sys_emp_dept'" + // 表名
//                        "'sink.buffer-flush.max-rows' = '5000'," +
//                        "'sink.buffer-flush.interval' = '2000 ms'," +
//                        "'sink.flush-on-checkpoint' = 'true'" +
                        ")"
        );

//        // 将JOIN的结果写入PostgreSQL
        joinedTable.executeInsert("postgres_table_sink");


        // 执行查询并将数据插入t2表
        // join表， CRUD, 都会自动更新， 直接插入目标表就行了，目标表，不需要删除
//        tableEnv.executeSql(
//                "INSERT INTO sys_emp_dept (id,emp_code,emp_name, dept) " +
//                        joinSql
//        );


        // 执行任务
        env.execute("MySQL to PostgreSQL with Calculation");
    }

    public static class NameToCalculation extends ScalarFunction {
        public String eval(Integer code, String name) {
            return code + "-" + name;
        }
    }
}
