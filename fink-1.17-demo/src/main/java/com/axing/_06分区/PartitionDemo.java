package com.axing._06分区;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

//无界流
public class PartitionDemo {

    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(2);


        //读取数据, socket
        DataStreamSource<String> source = env.socketTextStream("hadoop102", 7777);


        // 随机分区
        source
                .shuffle() //this.random.nextInt(this.numberOfChannels)
                .print();

        //轮询分区:上游不平均,可以使下游数据平均
        source
                .rebalance()
                .print();


        //缩放轮询:组队,指定几个下游, 比较高效
        source
                .rescale()
                .print();

        //广播: 发送给下游所有子任务
        source
                .broadcast()
                .print();


        // 全部发往第一个子任务,强行并行度等于1
        source
                .global()
                .print();


        // 相同key一个分区
        source
                .keyBy(value -> value)
                .print();


        // 一对一
        source
                .forward()
                .print();

        //自定义分区

        env.execute();

    }
}
