package com._08状态._02listState;

import cn.hutool.core.stream.StreamUtil;
import com.axing.bean.WaterSensor;
import com.axing.func.WaterSensorBeanMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class ListDemo {

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

                    ListState<Integer> lastVcStatus;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        lastVcStatus = getRuntimeContext().getListState(new ListStateDescriptor<>("lastVcStatus", Types.INT));
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        // 来一条,存入list
                        lastVcStatus.add(value.getVc());
                        // list取值,排序,保留最大的3个
//                        List<Integer> collect1 = StreamSupport.stream(lastVcStatus.get().spliterator(), false)
                        List<Integer> collect1 = StreamUtil.of(lastVcStatus.get(), false)
                                .sorted(Comparator.reverseOrder()) //逆序
                                .limit(3)
                                .collect(Collectors.toList());
                        out.collect(collect1.toString());
                        // 更新
                        lastVcStatus.update(collect1);

                    }
                })
                .print("最大的3个:");

        env.execute();
    }
}
