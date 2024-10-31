package com.github.axinger._16cdc;

import com.github.axinger._16cdc.model.ProductAcc;
import com.github.axinger._16cdc.model.SysProduct;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class ProductAggregation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        DataStream<String> inputStream = env.fromElements(
                "1501,冰箱,10,2024-10-15 08:00:00",
                "1502,冰箱,5,2024-10-15 08:00:05",
                "1503,冰箱,2,2024-10-15 08:00:10",
                "1504,冰箱,2,2024-10-15 08:00:15",
                "1601,冰箱,10,2024-10-16 08:00:00",
                "1601,冰箱,10,2024-10-16 09:00:00"
                // 更多数据...
        );


        env.socketTextStream("hadoop102", 7777)
                .map(value -> {
                    String[] split = value.split(",");
                    return SysProduct.builder()
                            .id(Integer.valueOf(split[0]))
                            .name(split[1])
                            .quantity(Integer.valueOf(split[2]))
                            .date(LocalDateTime.parse(split[3], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")))
                            .build();
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<SysProduct>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((record, timestamp) -> record.date.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli()))
                .keyBy(SysProduct::getName)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
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

}
