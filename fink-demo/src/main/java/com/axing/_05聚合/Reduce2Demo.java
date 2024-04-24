package com.axing._05聚合;

import com.axing.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Reduce2Demo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("s1", 1L, 11),
                new WaterSensor("s1", 12L, 10),
                new WaterSensor("s1", 13L, 9),
                new WaterSensor("s2", 2L, 22),
                new WaterSensor("s3", 3L, 33)
        );


        source
                .keyBy(value -> value.id)
                // value1 是上次的计算结果,  value2 是即将计算的数据源
                .reduce(((value1, value2) -> new WaterSensor(value1.id, Math.max(value1.ts, value2.ts), value1.vc + value2.vc)))
                .print();

        env.execute();
    }
}

