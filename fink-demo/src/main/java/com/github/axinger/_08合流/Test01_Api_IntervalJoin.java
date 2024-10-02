package com.github.axinger._08合流;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Test01_Api_IntervalJoin {
    public static void main(String[] args) {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        // 员工流
        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 7777)
                .map(new MapFunction<String, Emp>() {
                    @Override
                    public Emp map(String value) throws Exception {
                        String[] split = value.split(",");

                        return new Emp(Integer.valueOf(split[0]), split[1], Integer.valueOf(split[2]), Long.valueOf(split[3]));
                    }
                }).assignTimestampsAndWatermarks(
                        // 先不考虑乱序，单调递增
                        WatermarkStrategy.<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Emp>() {
                                    // 时间戳
                                    @Override
                                    public long extractTimestamp(Emp element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );


        // 部门流
        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 8888).map(new MapFunction<String, Dept>() {
            @Override
            public Dept map(String value) throws Exception {
                String[] split = value.split(",");

                return new Dept(Integer.valueOf(split[0]), split[1], Long.valueOf(split[2]));
            }
        }).assignTimestampsAndWatermarks(
                // 先不考虑乱序，单调递增
                WatermarkStrategy.<Dept>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Dept>() {
                            // 时间戳
                            @Override
                            public long extractTimestamp(Dept element, long recordTimestamp) {
                                return element.getTs();
                            }
                        })
        );


        // 使用 intervalJoin


        empDS.keyBy(Emp::getDeptNo)
                .intervalJoin(deptDS.keyBy(Dept::getDeptNo))
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>() {
                    @Override
                    public void processElement(Emp left, Dept right, ProcessJoinFunction<Emp, Dept, Tuple2<Emp, Dept>>.Context ctx, Collector<Tuple2<Emp, Dept>> out) throws Exception {

                        out.collect(new Tuple2<>(left, right));
                    }
                })
                .print();


    }
}
