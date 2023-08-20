package com.axing._08合流;

import com.axing.bean.WaterSensor;
import com.axing.func.WaterSensorBeanMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//无界流
public class UnionDemo {

    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> source1 = env.fromElements("a1", "a2", "a3");
        DataStreamSource<String> source2 = env.fromElements("b1", "b2", "b3");

        DataStreamSource<Integer> source3 = env.fromElements(1, 2, 3);

//        DataStream<String> union = source1.union(source2).union(source3.map(String::valueOf));

        DataStream<String> union = source1.union(source2,source3.map(String::valueOf));

        union.print();

        env.execute();

    }
}
