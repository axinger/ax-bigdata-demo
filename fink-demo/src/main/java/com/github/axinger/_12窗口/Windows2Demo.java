package com.github.axinger._12窗口;

import com.github.axinger.bean.WaterSensor;
import com.github.axinger.func.WaterSensorBeanMap;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Windows2Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        environment.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> ds = environment.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorBeanMap());


        KeyedStream<WaterSensor, String> ks = ds.keyBy(WaterSensor::getId);


        //  TODO 1.指定窗口分配器
        // 1.1  没有key by的窗口,所有数据会进入一个子任务, 并行度为只能1,所以需要key by
        //ds.windowAll()

        //有key by窗口
        //基于时间类型窗口
        WindowedStream<WaterSensor, String, TimeWindow> stream = ks.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));// 滚动窗口, 窗口长度10


        // TODO 2 指定窗口函数
        //增量聚合,来一个,计算一个,窗口触发计算结果
        SingleOutputStreamOperator<WaterSensor> operator = stream
                .reduce((ReduceFunction<WaterSensor>) (value1, value2) ->
                        new WaterSensor(value1.getId(), value1.ts, value1.vc + value2.vc));


        operator.print();


        environment.execute();

    }
}
