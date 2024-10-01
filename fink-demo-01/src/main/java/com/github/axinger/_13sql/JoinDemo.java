package com.github.axinger._13sql;

import com.github.axinger.bean.WaterSensor;
import com.github.axinger.bean.WaterSensor2;
import com.github.axinger.func.WaterSensor2BeanMap;
import com.github.axinger.func.WaterSensorBeanMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;


@Slf4j
public class JoinDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(1);

        // table环境
        StreamTableEnvironment env = StreamTableEnvironment.create(environment);

        // 默认状态没有过期时间
        //设置状态 过期时间
        env.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        SingleOutputStreamOperator<WaterSensor> ds1 = environment
                .socketTextStream("hadoop102", 8888)
                .map(new WaterSensorBeanMap());


        SingleOutputStreamOperator<WaterSensor2> ds2 = environment
                .socketTextStream("hadoop102", 9999)
                .map(new WaterSensor2BeanMap());


//        tableEnvironment.executeSql("") //执行sql，如建表

        env.createTemporaryView("t1", ds1); //临时表
        env.createTemporaryView("t2", ds2);


        // sql join
        env.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 join t2 on t1.id=t2.id")
                .execute()
                .print();

        // left join
        env.sqlQuery("select t1.id,t1.vc,t2.id,t2.name from t1 left join t2 on t1.id=t2.id")
                .execute()
                .print();
    }
}
