package com.github.axinger.func;

import com.github.axinger.bean.WaterSensor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

@Slf4j
public class WaterSensorBeanMap implements MapFunction<String, WaterSensor> {

    //  1,1,11
    // 2,2,12
    // 3,3,13

    @Override
    public WaterSensor map(String value) throws Exception {
        String[] split = value.split(",");
        WaterSensor waterSensor =   WaterSensor.builder()
                .id(split[0])
                .ts(Long.valueOf(split[1]))
                .vc(Integer.valueOf(split[2]))
                .build();
        log.info("解析数据WaterSensor={}", waterSensor);
        return waterSensor;
    }
}
