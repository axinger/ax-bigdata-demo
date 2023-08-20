package com.axing._08合流;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

//无界流
public class ConnectDemo {

    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> source1 = env.fromElements("a1", "a2", "a3");
        DataStreamSource<String> source2 = env.fromElements("b1", "b2", "b3");

        DataStreamSource<Integer> source3 = env.fromElements(1, 2, 3);


        // 不同数据流,需要各自转换一下,out类型一致
        source1.connect(source2).map(new CoMapFunction<String, String, Object>() {
            @Override
            public Object map1(String value) throws Exception {
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                return value;
            }
        }).print();

        source1.connect(source3).map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value;
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value+"";
            }
        }).print();


        env.execute();

    }
}
