package com.github.axinger._13sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkPulsarSQLExample2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createTableSql2 =   "CREATE TABLE pulsar_table2 (\n" +
                "  `data` STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'pulsar',\n" +
                "  'topics' = 'persistent://public/default/user_behavior2',\n" +
                "  'service-url' = 'pulsar://hadoop203:6650',\n" +
                "  'source.start.message-id' = 'earliest',\n" +
                "  'format' = 'json'\n" +
                ");\n";
        tableEnv.executeSql(createTableSql2);
        tableEnv.createTemporarySystemFunction("JsonArrayParser", JsonArrayParser.class);
//        TableResult result = tableEnv.executeSql(
//                "SELECT t.id, t.name, t.age\n" +
//                        "FROM pulsar_table2, LATERAL TABLE(JsonArrayParser(data)) AS t(id, name, age);"
//        );
       tableEnv.executeSql( "SELECT  * FROM pulsar_table2")
               .print();
        env.execute("Flink Pulsar SQL Example");
    }
}
