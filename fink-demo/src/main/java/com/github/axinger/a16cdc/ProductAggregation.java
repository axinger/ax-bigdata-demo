package com.github.axinger.a16cdc;

import cn.hutool.core.util.StrUtil;
import com.github.axinger.a16cdc.model.ProductAcc;
import com.github.axinger.a16cdc.model.SysProduct;
import com.github.axinger.a16cdc.model.SysProductMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class ProductAggregation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


//        DataStream<String> inputStream = env.fromElements(
//                "1501,冰箱,10,2024-10-15 08:00:00",
//                "1502,冰箱,5,2024-10-15 08:00:01",
//                "1503,冰箱,2,2024-10-15 08:00:02",
//                "1504,冰箱,2,2024-10-15 08:00:03",
//                "1505,冰箱,2,2024-10-15 08:00:04",
//                "1506,空调,2,2024-10-15 08:00:05",
//                "1507,空调,2,2024-10-15 08:00:06",
//
//                "1601,冰箱,10,2024-10-16 08:00:00",
//                "1602,冰箱,10,2024-10-16 09:00:00"
//                // 更多数据...
//        );


        env.socketTextStream("hadoop102", 7777)
                .map(new SysProductMap())
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SysProduct>(Time.seconds(10)) {
//                    @Override
//                    public long extractTimestamp(SysProduct element) {
//                        return element.date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
//                    }
//                })

                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SysProduct>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((record, timestamp) -> record.date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()))
                .keyBy(value -> StrUtil.format("{}-{}", value.getName(),value.getDate().toLocalDate()))
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))

                // https://blog.csdn.net/m0_37687896/article/details/129448206
                // 每来一条数据触发一次计算,trigger(EventTimeTrigger.create()) 可以使触发器生效。
                // 但是，这样的话，窗口将不会每来一条数据触发一次计算，而是要等到该窗口的 Watermark 推进到窗口结束时间后才会触发计算
//                .trigger(EventTimeTrigger.create())
//                .window(TumblingEventTimeWindows.of(Time.days(1)))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
//                .trigger(CountTrigger.of(1)) //每来一条数据触发一次计算
//                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(10)))
//                . trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5))) // 会一直打印，
//                .trigger(ProcessingTimeTrigger.create()) // 每 5 秒计算一次

//                .window(TumblingEventTimeWindows.of(Time.days(1), Time.seconds(5)))
//                .trigger(ProcessingTimeTrigger.create() )// 每 5 秒计算一次

                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))// 应用5秒的时间窗口
                .aggregate(new AggregateFunction<SysProduct, ProductAcc, ProductAcc>() {
                    @Override
                    public ProductAcc createAccumulator() {
                        return new ProductAcc();
                    }

                    @Override
                    public ProductAcc add(SysProduct value, ProductAcc accumulator) {
                        accumulator.sumQuantity += value.getQuantity();
                        accumulator.dataCount += 1;
                        accumulator.name = value.getName();
                        accumulator.maxId = value.getId();
                        accumulator.date = value.getDate();
                        return accumulator;
                    }

                    @Override
                    public ProductAcc getResult(ProductAcc accumulator) {
                        return accumulator;
                    }

                    @Override
                    public ProductAcc merge(ProductAcc a, ProductAcc b) {
                        a.sumQuantity += b.sumQuantity;
                        a.dataCount += b.dataCount;
                        a.maxId = Math.max(a.maxId, b.maxId);
                        return a;
                    }
                })
                .print(LocalDateTime.now()+"统计家电生产情况");
        env.execute();
    }

}
