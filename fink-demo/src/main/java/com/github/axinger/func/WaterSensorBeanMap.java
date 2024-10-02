package com.github.axinger.func;

import com.github.axinger.bean.WaterSensor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

@Slf4j
public class WaterSensorBeanMap implements MapFunction<String, WaterSensor> {


    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        WaterSensor waterSensor = new WaterSensor(split[0], Long.valueOf(split[1]), Integer.valueOf(split[2]));
        log.info("解析数据WaterSensor={}", waterSensor);
        System.out.println("waterSensor = " + waterSensor);
        return waterSensor;
    }
}
