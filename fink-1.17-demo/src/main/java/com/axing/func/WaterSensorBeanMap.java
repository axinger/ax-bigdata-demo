package com.axing.func;

import com.axing.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;

public class WaterSensorBeanMap implements MapFunction<String, WaterSensor> {


    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        return new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
    }
}
