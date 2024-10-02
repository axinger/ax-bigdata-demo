package com.github.axinger._08状态._03mapState;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo2 {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        DataStreamSource<String> ds = env.socketTextStream("hadoop102", 7777);

        ds.map(new RichMapFunction<String, Object>() {
            // 一个值，还有 MapStateDescriptor
            ValueState<String> lastVcStatus;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("current", String.class);

                // 设置状态保留时间，比如统计当日的
                valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                // 初始化状态
                lastVcStatus = getRuntimeContext().getState(valueStateDescriptor);
            }

            @Override
            public Object map(String value) throws Exception {
                String value1 = lastVcStatus.value();

                lastVcStatus.update(value1 + "," + value);
                return null;
            }
        });
    }
}
