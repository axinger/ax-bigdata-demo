package com.github.axinger._12窗口;

import com.github.axinger.bean.WaterSensor;
import com.github.axinger.func.WaterSensorBeanMap;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Windows1Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        environment.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> ds = environment.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorBeanMap());


        KeyedStream<WaterSensor, String> ks = ds.keyBy(WaterSensor::getId);


        //  TODO 1.指定窗口分配器
        // 1.1  没有key by的窗口,所有数据会进入一个子任务, 并行度为只能1,所以需要key by
        //ds.windowAll()

        //有key by窗口
        //基于时间类型窗口
        WindowedStream<WaterSensor, String, TimeWindow> stream = ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));// 滚动窗口, 窗口长度10s

//        ks.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)))   // 滑动窗口, 窗口长度10s,滑动步长2s
//        ks.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))   // 会话窗口, 超时间隔5s,没有数据来,就认为是一个窗口任务

        //基于计数
//        ks.countWindow(5)  //滚动窗口, 窗口长度5个元素

//        ks.countWindow(5,2) // 滑动窗口,窗口长度5个元素, 滑动步长2个元素
//        ks.window(GlobalWindows.create()); //全局窗口,计数窗口底层,自定义,很少用

        // TODO 2 指定窗口函数
        //增量聚合,来一个,计算一个,窗口触发计算结果
//        stream
//                .reduce()
//        .aggregate()
        //全窗口函数:数据来了,不计算,存起来,窗口触发时候,计算并输出结果

        environment.execute();
    }
}
