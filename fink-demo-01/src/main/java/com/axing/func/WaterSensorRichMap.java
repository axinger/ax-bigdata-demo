package com.axing.func;

import com.axing.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

// 多了生命周期函数
// 每种的算子都提供了RichXX
public class WaterSensorRichMap extends RichMapFunction<WaterSensor, String> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = getRuntimeContext();
        int index = context.getIndexOfThisSubtask();
        String name = context.getTaskName();
        System.out.println("index = " + index + "name = " + name);

    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public String map(WaterSensor value) throws Exception {
        return value.getTs() + "";
    }
}
