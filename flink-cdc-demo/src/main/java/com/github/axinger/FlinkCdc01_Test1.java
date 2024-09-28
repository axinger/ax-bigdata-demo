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

package com.github.axinger;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class FlinkCdc01_Test1 {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(6000);
//        checkpointConfig.setCheckpointStorage("file:///D:/flink_point/ck_cdc_mysql");
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck_cdc_mysql");
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMaxConcurrentCheckpoints(1); // 同时只存在一个
        checkpointConfig.setMinPauseBetweenCheckpoints(2000); //两个检测点之间最小间隔

        // mysql8
        Properties properties =new Properties();
        properties.setProperty("useSSL", "false");
        properties.setProperty("allowPublicKeyRetrieval", "true");

        //存档到hdfs,需要导入hadoop依赖,,指定hdfs用户名
        System.setProperty("HADOOP_USER_NAME", "admin");

        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("localhost")
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("ax_test") //多个库
                .tableList("ax_test.t1") // 多个库.多个表

                /* CRUD 增加(Create)、读取(Read)、更新(Update)和删除(Delete)
                 * 初始化一个快照，宕机重启后，有数据产生了，会拉取所有历史数据，会有重复的，
                 * 新增数据，再删除，不会有历史记录
                 * 新增数据，再修改，只会有最新的r记录
                 *
                 * c: 创建（Create）操作。
                    u: 更新（Update）操作。
                    d: 删除（Delete）操作。
                    s: 结构变更（Schema change），表示模式更改。
                    r: 读取（Read），但在Debezium中，这个标记可能表示的是“快照”（Snapshot）期间的操作。
                 */
                .startupOptions(StartupOptions.initial()) // 默认值
//                .startupOptions(StartupOptions.earliest()) // 没有快照，拉取历史数据，耗时
//                .startupOptions(StartupOptions.latest()) // 宕机时，产生的数据，不会被监控
                .deserializer(new JsonDebeziumDeserializationSchema())
                .jdbcProperties(properties)
                .build();

        env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc-source")
                .print();

        env.execute("Print MySQL Snapshot + Binlog");
    }
}
