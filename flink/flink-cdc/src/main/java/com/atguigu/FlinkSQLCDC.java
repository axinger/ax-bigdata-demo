package com.atguigu;

import cn.hutool.core.io.file.FileReader;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLCDC {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //1.1 开启CK
        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        //2.Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点
        //续传,需要从 Checkpoint 或者 Savepoint 启动程序
        //2.1 开启 Checkpoint,每隔 5 秒钟做一次 CK
        env.enableCheckpointing(5000L);
        //2.2 指定 CK 的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次 CK 数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
        //2.5 设置状态后端
//        env.setStateBackend(new FsStateBackend("hdfs://localhost:9000/cdc-test/ck"));


        // 数据源表
        String sourceDDL = new FileReader("source.txt").readString();
//        System.out.println("sourceDDL = " + sourceDDL);

        // 输出目标表
        String sinkDDL = new FileReader("target.txt").readString();
//        System.out.println("sinkDDL = " + sinkDDL);


        // 简单的聚合处理
        String transformDmlSQL = new FileReader("insert.txt").readString();
//        System.out.println("transformDmlSQL = " + transformDmlSQL);

        tableEnv.executeSql(sourceDDL).print();
        tableEnv.executeSql(sinkDDL).print();
        tableEnv.executeSql(transformDmlSQL).print();

        //4.启动
        env.execute("FlinkSQLCDC");

    }

}
