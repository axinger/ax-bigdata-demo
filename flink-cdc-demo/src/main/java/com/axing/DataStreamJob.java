/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.axing;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.Map;


public class DataStreamJob {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(5000L);
        env.getCheckpointConfig().setCheckpointTimeout(1000L);
        env.getCheckpointConfig().setCheckpointStorage("file:///D:/flink_point");
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1); // 同时只存在一个


        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("ax_test")
                .tableList("ax_test.t1")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();


        DataStreamSource<String> mysqlSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");
        mysqlSource.print("打印结果=" + LocalDateTime.now() + ":");


        // 目标 MySQL Sink 配置
//        JdbcConnectionOptions jdbcOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
//                .withUrl("jdbc:mysql://localhost:3306/ax_test2")
//                .withDriverName("com.mysql.jdbc.Driver")
//                .withUsername("root")
//                .withPassword("123456")
//                .build();
//
//		SinkFunction<String> sink = JdbcSink.sink(
//				"INSERT INTO t1 (id,name) VALUES (?, ?)",
//				(ps, t) -> {
//					JSONObject jsonObject = JSONObject.parseObject(t);
//
//					System.out.println("jsonObject = " + jsonObject);
//
//					JSONObject after = jsonObject.getJSONObject("after");
//					JSONObject before = jsonObject.getJSONObject("before");
//
//					if (after != null) { // 插入或更新操作
//						ps.setString(1, after.getString("id"));
//						ps.setString(2, after.getString("name"));
//
//						// 执行插入或更新 SQL
//						ps.executeUpdate();
//					} else if (before != null) { // 删除操作
//						ps.setString(1, before.getString("id"));
//
//						// 构建删除 SQL，这里假设主键为 id
//						String deleteSQL = "DELETE FROM t1 WHERE id = ?";
//						PreparedStatement deleteStmt = jdbcOptions.getConnection().prepareStatement(deleteSQL);
//						deleteStmt.setString(1, before.getString("id"));
//
//						// 执行删除 SQL
//						deleteStmt.executeUpdate();
//					}
//				},
//				jdbcOptions);
//
//
//        // 将处理后的数据写入目标 MySQL
//
//        mysqlSource.addSink(sink);

        env.execute();

    }
}
