package com.github.axinger._01demo1;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

//无界流
@Slf4j
public class SocketTextDemo {

    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
//        environment.setParallelism(3);//全局并行度

        //本地开发,有web页面的
//        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //读取数据, socket
        /*
         * 开启一个端口  nc -lk 7777
         * 监听 nc localhost 7777
         * 输入 hello java
         */
        DataStreamSource<String> read = environment.socketTextStream("hadoop102", 7777);


        //切分,转换二元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = read
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    log.info("接收到7777端口数据={}", value);
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
        sum.print("统计单词次数")
//                .setParallelism(3) //设置算子并行度
        ;


        // process 处理函数， 与 sum 同一个级别
        sum.keyBy(value -> value.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Object>() {
                    ReducingState<Map<String, Integer>> state;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        state = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("lastVcStatus",
                                (ReduceFunction<Map<String, Integer>>) (value1, value2) -> {
                                    Map<String, Integer> map2 = new HashMap<>();
                                    map2.putAll(value1);
                                    map2.putAll(value2);
                                    return map2;
                                },
                                Types.MAP(Types.STRING, Types.INT)
                        ));
                    }


                    @Override
                    public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Object>.Context ctx, Collector<Object> out) throws Exception {
                        Map<String, Integer> map1 = new HashMap<>();
                        map1.put(value.f0, value.f1);
                        state.add(map1);
                        System.out.println("state.get() = " + state.get());
                    }
                });


        //流处理,执行,启动操作
        environment.execute("7777端口，单词统计服务");


    }
}
