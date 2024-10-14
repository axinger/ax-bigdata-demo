package com.github.axinger._15水位线;

import cn.hutool.core.date.LocalDateTimeUtil;
import com.github.axinger.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Demo {
    public static void main(String[] args) {
        // 处理事件时间（Event Time），才需要水位线，解决0点漂移

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<WaterSensor> map = ds.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                return null;
            }
        });

        ds.assignTimestampsAndWatermarks(
//                WatermarkStrategy.noWatermarks() // 不需要水位线
//                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))// 时间范围
                WatermarkStrategy.<String>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {

                            @Override
                            public long extractTimestamp(String element, long recordTimestamp) {
                                return 0;
                            }
                        })
        );

        // 分组后，开窗
        KeyedStream<String, String> ds2 = ds.keyBy(new KeySelector<String, String>() {

            @Override
            public String getKey(String value) throws Exception {
                return "";
            }
        });
        // 以滚动事件时间窗口为例
        // 窗口对象什么时候创建，第一个元素到来，创建窗口对象
        // 窗口起始时间，左闭右开，
        // 什么时候触发计算
        // 关闭
        // 分组后，每个组开窗

        WindowedStream<String, String, TimeWindow> windowDS = ds2.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));


        // 聚合计算
        SingleOutputStreamOperator<String> reduceDS = windowDS.reduce(new ReduceFunction<String>() {
                                                                        @Override
                                                                        public String reduce(String value1, String value2) throws Exception {
                                                                            return "";
                                                                        }

                                                                    }
                ,
                // 补充时间属性
                new WindowFunction<String, String, String, TimeWindow>() {

                    @Override
                    public void apply(String s, TimeWindow window, Iterable<String> input, Collector<String> out) throws Exception {


                        String next = input.iterator().next();

                        // 这个是为了获取，计算时间，是flink的
                        long start = window.getStart();
                        String startStr = LocalDateTimeUtil.format(LocalDateTimeUtil.of(start), "yyyy-MM-dd HH:mm:ss");
                        long end = window.getEnd();
                        String endStr = LocalDateTimeUtil.format(LocalDateTimeUtil.of(end), "yyyy-MM-dd HH:mm:ss");
                        out.collect(next);
                    }
                }
        );

        reduceDS.print();

//        windowDS.process(new ProcessWindowFunction<String, WaterSensor, String, TimeWindow>() {
//
//            @Override
//            public void process(String s, ProcessWindowFunction<String, WaterSensor, String, TimeWindow>.Context context, Iterable<String> elements, Collector<WaterSensor> out) throws Exception {
//
//            }
//        });


    }
}
