package com.github.axinger._06分区;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//无界流
public class PartitionDemo2 {

    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(2);


        //读取数据, socket
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 7777);


        //自定义分区
        // 随机分区
        source.partitionCustom(
                        new CustomPartitioner(),
                        value -> value
                )
                .print();


        env.execute();

    }
}
