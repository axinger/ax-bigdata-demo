package com.axing._04转换;

import com.axing.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class MapDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("s1", 1L, 11),
                new WaterSensor("s1", 1L, 12),
                new WaterSensor("s2", 2L, 22),
                new WaterSensor("s3", 3L, 33)
        );


        // lambda表达式
//        source.map(value -> value.id)
//        .print();


        // 定义类,封装逻辑
//        source
//                .filter(new WaterSensorFilter())
//                .filter(new WaterSensorFilter2("s2"))
//                .map(new WaterSensorMap())
//                .print();

//        source
//                .flatMap(new WaterSensorFlatMap()) // 一进多出
//                .print();


        env.execute();
    }
}

