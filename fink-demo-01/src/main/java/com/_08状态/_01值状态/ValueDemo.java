package com._08状态._01值状态;

import cn.hutool.core.util.StrUtil;
import com.github.axinger.bean.WaterSensor;
import com.github.axinger.func.WaterSensorBeanMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Optional;

public class ValueDemo {

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

                    ValueState<Integer> lastVcStatus;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        lastVcStatus = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVcStatus", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        Integer currentVc = value.getVc();

                        //避免第一次就输入超过10
                        if (Optional.ofNullable(lastVcStatus.value()).isPresent()) {
                            // 取出上一条数据
                            int lastVc = Optional.ofNullable(lastVcStatus.value()).orElse(0);
                            if (Math.abs(currentVc - lastVc) > 10) {
                                out.collect(StrUtil.format("当前水位={},上一个水位{},差值超过10", currentVc, lastVc));
                            }
                        }
                        if (Optional.ofNullable(currentVc).isPresent()) {
                            lastVcStatus.update(currentVc);
                        }
                    }
                })
                .print("相差超过10:");

        env.execute();
    }
}
