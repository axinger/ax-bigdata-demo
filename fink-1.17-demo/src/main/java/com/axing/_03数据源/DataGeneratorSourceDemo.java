package com.axing._03数据源;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class DataGeneratorSourceDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(2); //每个并行度自增


        DataGeneratorSource<String> dataGeneratorSource = new DataGeneratorSource<>(value -> "no-" + value,
                Long.MAX_VALUE, //最多个数
                RateLimiterStrategy.perSecond(1), //每秒个数
                Types.STRING
        );

        DataStreamSource<String> source = env.fromSource(dataGeneratorSource, WatermarkStrategy.noWatermarks(), "dataGeneratorSource");

        source.print();
        env.execute();
    }
}

