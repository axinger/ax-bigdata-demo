package com.github.axinger._08合流;

import com.alibaba.fastjson2.JSONObject;
import com.github.axinger.base.KafkaApiBaseApp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test04_Api_kafka_read implements KafkaApiBaseApp {
    public static void main(String[] args) {

        new Test04_Api_kafka_read().start("dog-order", "demoGroup");

    }

    @Override
    public void handler(DataStreamSource<String> kafkaDS) {
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSONObject::parseObject);

        //分组
        KeyedStream<JSONObject, String> idKeyBy = jsonDS.keyBy(value -> value.getString("id"));

        //去重方式一，状态+定时器，缺点：不管是否重复，都会等5秒，时效性差

        idKeyBy.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> lastValueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                lastValueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject lastValueObj = lastValueState.value();

                if (lastValueObj == null) {
                    lastValueState.update(value);
                    //当前处理时间
                    long currentTime = ctx.timerService().currentProcessingTime();

                    //设置定时任务
                    ctx.timerService().registerProcessingTimeTimer(currentTime + 5000);

                } else {
                    // 说明重复了， 用当前数据的聚合时间，和状态中的数据聚合时间进行比较，将时间打的放到状态中
                    String lastTs = lastValueObj.getString("聚合时间戳");
                    String curTs = value.getString("聚合时间戳");

                    if (lastTs.equals(curTs)) {
                        lastValueState.update(value);
                    }

                }
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                super.onTimer(timestamp, ctx, out);


                out.collect(lastValueState.value());
                lastValueState.clear();
            }
        });

        //去重方式二，状态+抵消，有点：时效性好；缺点：出现重复，传递3条数据
        idKeyBy.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            ValueState<JSONObject> lastObjState;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueStateDescriptor<JSONObject> stateDescriptor = new ValueStateDescriptor<>("lastValueState", JSONObject.class);
                stateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                lastObjState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

                JSONObject lastJsonObj = lastObjState.value();

                if (lastJsonObj != null) {
                    // 重复了，将已经发送的数据，取反，再传达下游
                    String str1 = lastJsonObj.getString("金额");

                    lastJsonObj.put("金额", "-" + str1);//取反
                    out.collect(lastJsonObj);
                }

                lastObjState.update(jsonObj);

                out.collect(jsonObj);
            }

        }).assignTimestampsAndWatermarks(
                // 先不考虑乱序，单调递增
                WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            // 时间戳
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts") * 1000; // json里面是秒，flink需要毫秒
                            }
                        })
        );


    }
}
