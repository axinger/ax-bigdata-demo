package com.github.axinger.func;

import com.github.axinger.bean.WaterSensor;
import org.apache.flink.api.common.functions.FilterFunction;

public class WaterSensorFilter implements FilterFunction<WaterSensor> {
    @Override
    public boolean filter(WaterSensor value) {
        return value.vc > 1;
    }
}
