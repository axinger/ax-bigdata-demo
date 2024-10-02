package com.github.axinger._12窗口;

import com.github.axinger.bean.WaterSensor;
import com.github.axinger.func.WaterSensorBeanMap;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Windows3Demo {
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
        // aggregate 3个泛型,可以不一样
        // 类型1: 输入类型
        // 类型2: 累加器类型
        // 类型3: 输出类型

        SingleOutputStreamOperator<String> operator = stream
                .aggregate(new AggregateFunction<WaterSensor, Integer, String>() {


                    // 创建累加器
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    // 聚合逻辑
                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        return value.getVc() + accumulator;
                    }

                    // 获取最终结果
                    @Override
                    public String getResult(Integer accumulator) {
                        return "结果"+ accumulator;
                    }

                    // 只有会话窗口才会用到
                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                });


        operator.print();

        environment.execute();
    }
}
