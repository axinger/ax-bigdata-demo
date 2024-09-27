package com.github.axinger._02demo2;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

//无界流
public class StreamApiDemo {

    public static void main(String[] args) throws Exception {

        //执行环境
        Configuration conf = new Configuration();
        conf.set(RestOptions.BIND_PORT, "8082");
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment(conf);
//        environment.setParallelism(3);//全局并行度

        //读取数据, socket
        DataStreamSource<String> read = environment.socketTextStream("hadoop102", 7777);


        //切分,转换二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = read
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                        out.collect(tuple2);
                    }
                })
//                .setParallelism(3) //设置算子map的并行度
                .returns(Types.TUPLE(Types.STRING, Types.INT));//lambda报错,可以在转换的算子之后调用returns(...)方法来显示指明要返回的数据类型信息。

        //按照第一个元素分组
        KeyedStream<Tuple2<String, Integer>, String> keyBy = map.keyBy((value -> value.f0));

        //按照第二个元素聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);
        //输出
        sum.print()
//                .setParallelism(3) //设置算子并行度
        ;
        //流处理,执行,启动操作
        environment.execute();

    }
}
