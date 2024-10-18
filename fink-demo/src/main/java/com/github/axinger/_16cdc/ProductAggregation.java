package com.github.axinger._16cdc;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ProductAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<SysProduct> productStream = env.fromElements(

                // 15日 冰箱：10，空调：30，洗衣机：80 =============================
                SysProduct.builder()
                        .id(1501)
                        .name("冰箱")
                        .quantity(10)
                        .date("2024-10-15 08:00:00")
                        .build(),

                SysProduct.builder()
                        .id(1502)
                        .name("空调")
                        .quantity(30)
                        .date("2024-10-15 07:00:00")
                        .build(),

                SysProduct.builder()
                        .id(1503)
                        .name("洗衣机")
                        .quantity(20)
                        .date("2024-10-15 07:00:00")
                        .build(),

                SysProduct.builder()
                        .id(1504)
                        .name("洗衣机")
                        .quantity(30)
                        .date("2024-10-15 08:00:00")
                        .build(),

                // 16日 冰箱：20，空调：90 =============================
                SysProduct.builder()
                        .id(1601)
                        .name("冰箱")
                        .quantity(20)
                        .date("2024-10-16 07:00:00")
                        .build(),

                SysProduct.builder()
                        .id(1602)
                        .name("空调")
                        .quantity(40)
                        .date("2024-10-16 07:00:00")
                        .build(),
                SysProduct.builder()
                        .id(1603)
                        .name("空调")
                        .quantity(50)
                        .date("2024-10-16 08:00:00")
                        .build(),


                // 15日，乱序一下
                SysProduct.builder()
                        .id(1505)
                        .name("洗衣机")
                        .quantity(30)
                        .date("2024-10-15 18:00:00")
                        .build()


        );

        productStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SysProduct>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SysProduct>() {
                                    @Override
                                    public long extractTimestamp(SysProduct element, long recordTimestamp) {

                                        // LocalDateTime.parse("2024-10-01 08:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
                                        //                                .toEpochSecond(ZoneOffset.ofHours(8)) * 1000

                                        // 转为毫秒时间戳
                                        return LocalDateTime.parse(element.date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.ofHours(0)) * 1000;

//                                        return element.getTs();  // 使用毫秒时间戳
                                    }
                                })
                )
                .keyBy(SysProduct::getName)  // 按产品名称分组
//                .keyBy(value -> {
//
//                    return LocalDateTimeUtil.format(LocalDateTimeUtil.parse(value.date, "yyyy-MM-dd HH:mm:ss").toLocalDate(), "yyyy-MM-dd");
//
//
//                })
                .window(TumblingEventTimeWindows.of(Time.days(1)))  // 使用滚动窗口，按天计算
                .aggregate(new AggregateFunction<SysProduct, ProductAcc, ProductAcc>() {
                    @Override
                    public ProductAcc createAccumulator() {
                        return new ProductAcc();
                    }

                    @Override
                    public ProductAcc add(SysProduct value, ProductAcc accumulator) {
                        accumulator.totalQuantity += value.getQuantity();
                        accumulator.productionCount += 1;
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
                        a.totalQuantity += b.totalQuantity;
                        a.productionCount += b.productionCount;
                        a.maxId = Math.max(a.maxId, b.maxId);
                        return a;
                    }
                })
                .print("统计家电生产情况");

        env.execute();
    }


    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class SysProduct {

        // 数据id
        private Integer id;
        // 产品名称
        public String name;

        // 生产的数量,统计每个产品每日生产总数
        private Integer quantity;

        // 生产时间时间
//    public long ts;
        public String date;
    }

    // 累加器类，用于存储总和与生产次数
    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ProductAcc {
        public Integer maxId = 0;
        public String name;
        public String date;
        public int totalQuantity = 0;
        public int productionCount = 0;
    }

}
