package com.axing._03数据源;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setGroupId("demoGroup")
                .setTopics("test_1")
                .setValueOnlyDeserializer(new SimpleStringSchema()) //只对value序列化
                .setStartingOffsets(OffsetsInitializer.latest()) // 起始位置,默认earliest
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafkaSource");

        source.print();

        env.execute();
    }
}

/*
 * kafka消费参数
 *  offsets:
 *      earliest: 如果有offset,从offset消费,没有,就从最新消费
 *      latest: 如果有offset,从offset消费,没有,就从最新消费
 *
 * flink
 *  earliest: 一定从 最早消费
 *  latest: 一定从 最新消费
 *
 */
