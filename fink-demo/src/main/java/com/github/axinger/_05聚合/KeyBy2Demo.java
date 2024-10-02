package com.github.axinger._05聚合;

import com.github.axinger.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class KeyBy2Demo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<WaterSensor> source = env.fromElements(
                new WaterSensor("s1", 1L, 11),
                new WaterSensor("s1", 12L, 10),
                new WaterSensor("s2", 2L, 22),
                new WaterSensor("s3", 3L, 33)
        );



        /*  keyBy 不是转换算子,只是对数据分区
         *  keyBy 分区和分组区别
         *  keyBy是对数据分组,保证相同的key的数据,在同一个分区:  s1和s1 在同一个并行度中(前面的序号)
         *  分区:一个子任务可以理解为一个分区
         *
         */

        source
                .keyBy(value -> value.id)
//                .sum(2) // 位置:适用Tuple类型
//                .sum("vc")//属性名
//                .min("vc")
                .minBy("vc")//是否取非比较值
                .print();

        env.execute();
    }
}

