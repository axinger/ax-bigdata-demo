package com.axing.func;

import com.axing.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorMap implements MapFunction<WaterSensor,String> {


    @Override
    public String map(WaterSensor value) {
        return value.id;
    }
}
