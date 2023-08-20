package com.axing.func;

import com.axing.bean.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.util.Collector;

public class WaterSensorFlatMap implements FlatMapFunction<WaterSensor,String> {


    @Override
    public void flatMap(WaterSensor value, Collector<String> out) throws Exception {
        if ("s1".equals(value.id)){
            out.collect(String.valueOf(value.vc));
        }else if ("s2".equals(value.id)){
            out.collect(String.valueOf(value.ts));
            out.collect(String.valueOf(value.vc));
        }
    }
}
