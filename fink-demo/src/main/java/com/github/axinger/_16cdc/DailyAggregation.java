//package com.github.axinger._16cdc;
//
//import com.alibaba.fastjson2.JSONObject;
//import com.github.axinger._08合流.Emp;
//import com.github.axinger.bean.SysProduct;
//import com.github.axinger.bean.SysProductCollect;
//import com.github.axinger.bean.WaterSensor;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.state.AggregatingState;
//import org.apache.flink.api.common.state.AggregatingStateDescriptor;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//
//import java.time.Duration;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//
//public class DailyAggregation {
//
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//
//        env.fromElements(
//                        SysProduct.builder()
//                                .id(1)
//                                .name("冰箱")
//                                .quantity(10)
//                                .ts(LocalDateTime.parse("2024-10-02 08:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getSecond())
//                                .build(),
//
//                        SysProduct.builder()
//                                .id(2)
//                                .name("冰箱")
//                                .quantity(20)
//                                .ts(LocalDateTime.parse("2024-10-02 09:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getSecond())
//                                .build(),
//
//                        SysProduct.builder()
//                                .id(3)
//                                .name("空调")
//                                .quantity(30)
//                                .ts(LocalDateTime.parse("2024-10-02 08:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getSecond())
//                                .build(),
//
//                        SysProduct.builder()
//                                .id(3)
//                                .name("空调")
//                                .quantity(40)
//                                .ts(LocalDateTime.parse("2024-10-01 08:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).getSecond())
//                                .build()
//
//                )
//                .assignTimestampsAndWatermarks(
//                        WatermarkStrategy.<SysProduct>forBoundedOutOfOrderness(Duration.ofSeconds(5))
//                                .withTimestampAssigner(new SerializableTimestampAssigner<SysProduct>() {
//                                    // 时间戳
//                                    @Override
//                                    public long extractTimestamp(SysProduct element, long recordTimestamp) {
//                                        return element.getTs();
//                                    }
//                                })
//                )
//                .keyBy(SysProduct::getName) // 按照产品分类统计
//                .window(TumblingEventTimeWindows.of(Time.days(1)))
//
//                .process(
//                        new KeyedProcessFunction<String, SysProductCollect, String>() {
//
//                            AggregatingState<Integer, Double> vcAvgAggregatingState;
//
//                            @Override
//                            public void open(Configuration parameters) throws Exception {
//                                super.open(parameters);
//                                vcAvgAggregatingState = getRuntimeContext()
//                                        .getAggregatingState(
//                                                new AggregatingStateDescriptor<>(
//                                                        "vcAvgAggregatingState",
//                                                        new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
//                                                            @Override
//                                                            public Tuple2<Integer, Integer> createAccumulator() {
//                                                                return Tuple2.of(0, 0);
//                                                            }
//
//                                                            @Override
//                                                            public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
//                                                                return Tuple2.of(accumulator.f0 + value, accumulator.f1 + 1);
//                                                            }
//
//                                                            @Override
//                                                            public Double getResult(Tuple2<Integer, Integer> accumulator) {
//                                                                return accumulator.f0 * 1D / accumulator.f1;
//                                                            }
//
//                                                            @Override
//                                                            public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
////                                                                return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
//                                                                return null;
//                                                            }
//                                                        },
//                                                        Types.TUPLE(Types.INT, Types.INT))
//                                        );
//                            }
//
//                            @Override
//                            public void processElement(SysProductCollect value, Context ctx, Collector<String> out) throws Exception {
//                                // 将 水位值 添加到  聚合状态中
//                                vcAvgAggregatingState.add(value.getVc());
//                                // 从 聚合状态中 获取结果
//                                Double vcAvg = vcAvgAggregatingState.get();
//
//                                out.collect("传感器id为" + value.getId() + ",平均水位值=" + vcAvg);
//
////                                vcAvgAggregatingState.get();    // 对 本组的聚合状态 获取结果
////                                vcAvgAggregatingState.add();    // 对 本组的聚合状态 添加数据，会自动进行聚合
////                                vcAvgAggregatingState.clear();  // 对 本组的聚合状态 清空数据
//                            }
//                        }
//                )
//    );
//
//
//    }
//}
