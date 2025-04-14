package com.github.axinger.a16cdc;

import cn.hutool.core.date.LocalDateTimeUtil;
import com.github.axinger.a16cdc.model.SysProduct;
import com.github.axinger.a16cdc.model.SysProductMap;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ReduceDemo01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //  1,0,11
        // 2,5,12
        // 3,20,13
        env.socketTextStream("hadoop102", 12001)
                .map(new SysProductMap())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SysProduct>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((e, ts) -> LocalDateTimeUtil.toEpochMilli(e.getDate()))
                )

                // 连续的两个水位值,差值超过10
                .keyBy(val->val.getName())
                .process(new KeyedProcessFunction<String, SysProduct, String>() {


                    ReducingState<Integer> lastVcStatus;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        // 初始化状态
                        lastVcStatus = getRuntimeContext().getReducingState(new ReducingStateDescriptor<>("lastVcStatus",
                                Integer::sum,
                                Types.INT));
                    }

                    @Override
                    public void processElement(SysProduct value, KeyedProcessFunction<String, SysProduct, String>.Context ctx, Collector<String> out) throws Exception {
                        lastVcStatus.add(value.getQuantity());
                        // 在后面,out
                        out.collect(value.getName() + "求和:" + lastVcStatus.get());
                    }
                })
                .print("求和:");

        env.execute();
    }
}
