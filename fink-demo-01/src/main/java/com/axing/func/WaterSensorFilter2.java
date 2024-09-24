package com.axing.func;

import com.axing.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class WaterSensorFilter2 implements FilterFunction<WaterSensor> {
    private final String id;

    public WaterSensorFilter2(String id) {
        this.id = id;
    }

    @Override
    public boolean filter(WaterSensor value) {
        return this.id.equals(value.id);
    }
}
