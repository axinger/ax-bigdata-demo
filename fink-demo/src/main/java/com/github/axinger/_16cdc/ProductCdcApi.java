package com.github.axinger._16cdc;

import cn.hutool.core.date.LocalDateTimeUtil;
import com.github.axinger._16cdc.model.ProductAcc;
import com.github.axinger._16cdc.model.SysProduct;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ProductCdcApi {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");

                    return SysProduct.builder()
                            .id(Integer.valueOf(split[0]))
                            .name(split[1])
                            .quantity(Integer.valueOf(split[2]))
                            .date(split[3])
                            .build();
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SysProduct>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner(new SerializableTimestampAssigner<SysProduct>() {

                                    @Override
                                    public long extractTimestamp(SysProduct element, long recordTimestamp) {

                                        return LocalDateTime.parse(element.date, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.ofHours(0)) * 1000;
                                    }
                                })
                )
//                .keyBy(SysProduct::getName)  // 按产品名称分组
                .keyBy(product -> product.getName() + "|" +    LocalDateTimeUtil.format(LocalDateTimeUtil.parse(product.getDate(), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")),"yyyy-MM-dd").toString())  // Combine name and date

                .window(TumblingEventTimeWindows.of(Time.seconds(3)))  // 使用滚动窗口，按天计算
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
                        return a;
                    }
                })
                .print("统计家电生产情况");
        env.execute();
    }

}
