package com.axing.demo.flink.job;

import com.alibaba.fastjson2.JSON;
import com.axing.demo.flink.config.CheckPointConfig;
import com.axing.demo.flink.config.MyJsonSchema;
import com.axing.demo.flink.model.ResponseModel;
import com.axing.demo.flink.sink.OrderSink;
import com.axing.demo.flink.sink.UserSink;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author xing
 */
@Component
@Slf4j
public class StreamingJob implements ApplicationRunner {

    @Autowired
    private CheckPointConfig checkPointConfig;

    ExecutorService executorService = Executors.newFixedThreadPool(1);

    @Override
    public void run(ApplicationArguments args) {

        executorService.submit(() -> {
                flinkCdc();
        });
    }


    public void flinkCdc(){
        try {
            MySqlSource<String> cdcMysqlSource = MySqlSource.<String>builder()
                    .hostname("127.0.0.1")
                    .port(3306)
                    .scanNewlyAddedTableEnabled(true)
                    .username("root")
                    .password("123456")
                    .serverTimeZone("Asia/Shanghai")
                    .databaseList("flink_a")
                    .tableList("flink_a.my_order", "flink_a.user")
                    .deserializer(new MyJsonSchema())
                    .build();

            Configuration configuration = new Configuration();
            // read checkpoint record
            // 第一次读取需要注释此行，后续增加表时，开启此行，flink-ck后 ‘27b27e36750ff997a7bd3b9933c5f3c9/chk-12404’换成存储路径下对应文件夹即可，实现旧表增量读取，新表全量读取
//            configuration.setString("execution.savepoint.path", checkPointConfig.fullPath());
            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
            env.setRestartStrategy(RestartStrategies.noRestart());
//            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
            // enable checkpoint
//            env.enableCheckpointing(3000);
            // set local storage path
//            env.getCheckpointConfig().setCheckpointStorage(checkPointConfig.fileStorage());

            // 多表进行分片处理
            OutputTag<String> orderTag = new OutputTag<>("flink_a.my_order", Types.STRING);
            OutputTag<String> userTag = new OutputTag<>("flink_a.user", Types.STRING);

            DataStreamSource<String> mySQL_source = env.fromSource(cdcMysqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

            SingleOutputStreamOperator<String> process = mySQL_source
                    .map((MapFunction<String, ResponseModel>) value -> {
                        ResponseModel responseModel = JSON.parseObject(value, ResponseModel.class);
                        return responseModel;
                    }).process(new ProcessFunction<ResponseModel, String>() {
                        @Override
                        public void processElement(ResponseModel value, Context context, Collector<String> collector) {
                            String jsonString = JSON.toJSONString(value);
                            if ("my_order".equals(value.getTable())) {
                                context.output(orderTag, jsonString);
                            } else if ("user".equals(value.getTable())) {
                                context.output(userTag, jsonString);
                            }
                        }
                    });

            DataStream<String> orderStream = process.getSideOutput(orderTag);
            DataStream<String> userStream = process.getSideOutput(userTag);
            orderStream.print();
            userStream.print();
            //自定义sink
            orderStream.addSink(new OrderSink());
            userStream.addSink(new UserSink());
            env.execute("flinkCdc");
        } catch (Exception e) {
            log.error("启动失败 = {}, = {}",e.getMessage(),e.getCause().getMessage());

            System.exit(0);
        }
    }
}