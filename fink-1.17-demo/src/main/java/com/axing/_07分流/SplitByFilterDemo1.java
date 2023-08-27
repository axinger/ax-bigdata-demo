package com.axing._07分流;

import com.axing.bean.WaterSensor;
import com.axing.func.WaterSensorBeanMap;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//无界流
public class SplitByFilterDemo1 {

    public static void main(String[] args) throws Exception {

        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();
        env.setParallelism(1);


        OutputTag<WaterSensor> tag1 = new OutputTag<>("s1", Types.POJO(WaterSensor.class));
        OutputTag<WaterSensor> tag2 = new OutputTag<>("s2", Types.POJO(WaterSensor.class));


        //读取数据, socket

        SingleOutputStreamOperator<WaterSensor> operator = env.socketTextStream("hadoop102", 7777)
                .map(new WaterSensorBeanMap())
                .process(new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = value.getId();
                        if ("s1".equals(id)) {
                            //放入侧输出流s1中
                            ctx.output(tag1, value);
                        } else if ("s2".equals(id)) {
                            //放入侧输出流s2中
                            ctx.output(tag2, value);
                        } else {
                            //主流
                            out.collect(value);
                        }
                    }
                });


        // 主流数据
        operator.print("主流数据");

        //测流
        operator.getSideOutput(tag1).print("测流1");
        operator.getSideOutput(tag2).print("测流2");

        env.execute();

    }
}
