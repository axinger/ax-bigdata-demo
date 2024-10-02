package com.github.axinger._08合流;

import com.alibaba.fastjson2.JSONObject;
import com.github.axinger.base.KafkaApiBaseApp;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Test04_Api_kafka_read implements KafkaApiBaseApp {
    public static void main(String[] args) {

        new Test04_Api_kafka_read().start("dog-order","demoGroup");

    }

    @Override
    public void handler(DataStreamSource<String> kafkaDS) {
        SingleOutputStreamOperator<JSONObject> jsonDS = kafkaDS.map(JSONObject::parseObject);

        //分组
        KeyedStream<JSONObject, String> idKeyBy  = jsonDS.keyBy(value -> value.getString("id"));

        //去重方式一，状态+定时器

        idKeyBy.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {
            private ValueState<JSONObject> lastValueState;
            @Override
            public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
            }

            @Override
            public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {

            }
        });
    }
}
