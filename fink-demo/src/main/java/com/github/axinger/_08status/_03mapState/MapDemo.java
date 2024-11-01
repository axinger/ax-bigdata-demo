package com.github.axinger._08status._03mapState;

import cn.hutool.core.stream.StreamUtil;
import cn.hutool.core.util.StrUtil;
import com.github.axinger.bean.WaterSensor;
import com.github.axinger.func.WaterSensorBeanMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;
import java.util.stream.Collectors;

public class MapDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> operator = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorBeanMap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((e, ts) -> e.getTs() * 1000)
                );

        // 连续的两个水位值,差值超过10
        operator.keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    MapState<Integer, Integer> lastVcStatus;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        MapStateDescriptor<Integer, Integer> stateDescriptor = new MapStateDescriptor<>("lastVcStatus", Types.INT, Types.INT);

                        // 设置状态保留时间，比如统计当日的
                        stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        // 初始化状态
                        lastVcStatus = getRuntimeContext().getMapState(stateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        Integer i = Optional.ofNullable(lastVcStatus.get(value.getVc())).orElse(0);
                        lastVcStatus.put(value.getVc(), ++i);
                        String collect = StreamUtil.of(lastVcStatus.entries())
                                .map(val -> StrUtil.format("{}-{}", val.getKey(), val.getValue()))
                                .collect(Collectors.joining(","));
                        out.collect("传感器id为" + value.getId() + " " + collect);
                    }
                })
                .print("map计数:");

        env.execute();
    }
}
