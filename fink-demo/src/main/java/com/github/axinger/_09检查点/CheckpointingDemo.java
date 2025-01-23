package com.github.axinger._09检查点;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;


public class CheckpointingDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
//                .getExecutionEnvironment()
                .createLocalEnvironmentWithWebUI(configuration);


        environment.setParallelism(1);


        //存档到hdfs,需要导入hadoop依赖,,指定hdfs用户名
        System.setProperty("HADOOP_USER_NAME", "admin");


        //1.检查点: 时间毫秒,默认精准异常
        environment.enableCheckpointing(5000);

        //2.指定检测点存储位置
        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/checkpoint");

        // 超时时间:默认10分钟

        //检查点最大数量,最好1,默认值1
        checkpointConfig.setMaxConcurrentCheckpoints(1);


        //取消作业时候,数据保留外部系统
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.NO_EXTERNALIZED_CHECKPOINTS);

        // 1.17特性
        // 非对齐检查点,开启要求:必须设置精准一次,并发必须设置为1
        checkpointConfig.enableUnalignedCheckpoints();

        // 开启非对齐检查点才生效,
        //如果大于0,一开始对齐检查点(barrier)对齐, 对齐时间超过参数,自动切换到非对齐检测点(barrier非对齐)
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(1));


        DataStreamSource<String> dataStreamSource = environment.socketTextStream("hadoop102", 7777);

        dataStreamSource
                .flatMap((String value, Collector<Tuple2<String, Integer>> out) -> {
                    String[] words = value.split(" ");
                    for (String word : words) {
                        Tuple2<String, Integer> tuple2 = Tuple2.of(word, 1);
                        out.collect(tuple2);
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((value -> value.f0))
                .sum(1)
                .print();

        environment.execute();

    }
}
