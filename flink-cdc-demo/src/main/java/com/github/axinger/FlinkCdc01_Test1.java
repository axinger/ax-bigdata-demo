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

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.fastjson2.JSONWriter;
import com.alibaba.fastjson2.TypeReference;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
        Properties properties = new Properties();
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

        DataStreamSource<String> streamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql-cdc-source");
//        streamSource.print("原始数据");

        keyByAge(streamSource);


        env.execute("Print MySQL Snapshot + Binlog");
    }

    private static void keyByAge(DataStreamSource<String> streamSource) {
        streamSource
                .map(new MapFunction<String, JSONObject>() {

                    @Override
                    public JSONObject map(String value) {
                        System.out.println("value = " + value);
                        JSONObject jsonObject = JSONObject.parseObject(value);
                        return jsonObject.getJSONObject("after");
                    }
                })

//        .process(
//                new ProcessFunction<JSONObject,String>() {
//
//                    @Override
//                    public void processElement(JSONObject s, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
//
//                        System.out.println("s = " + s.toString(JSONWriter.Feature.WriteMapNullValue));
////                        System.out.println("context.timestamp() = " + context.timestamp());
//
//                      getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("lastVcStatus",
//                                Integer::sum,
//                                Types.INT));
//
//                        collector.collect(s.getString("after"));
//                    }
//                }
//        )
                .keyBy(value -> value.getString("age"))
                .process(new KeyedProcessFunction<String, JSONObject, String>() {

                    ReducingState<String> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        state = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("lastVcStatus",
                                (value1, value2) -> StrUtil.format("{},{}", value1, value2),
                                Types.STRING));

                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                        state.add(value.getString("name"));
                        // 在后面,out
                        out.collect(StrUtil.format("年龄为{}，的姓名有{}", value.getString("age"), state.get()));
                    }

                })
                .print("处理后数据");
    }


}
