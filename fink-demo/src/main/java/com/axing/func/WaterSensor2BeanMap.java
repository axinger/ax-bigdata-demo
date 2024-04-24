package com.axing.func;

import com.axing.bean.WaterSensor2;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;

@Slf4j
//@Log4j
public class WaterSensor2BeanMap implements MapFunction<String, WaterSensor2> {


    @Override
    public WaterSensor2 map(String value) throws Exception {
        String[] split = value.split(",");
        WaterSensor2 waterSensor2 = new WaterSensor2(split[0], Long.valueOf(split[2]), split[1]);

        log.info("解析数据WaterSensor2={}",waterSensor2);
        return waterSensor2;

    }
}
